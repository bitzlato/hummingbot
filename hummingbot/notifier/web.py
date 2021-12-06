#!/usr/bin/env python
from os.path import join, realpath
import sys; sys.path.insert(0, realpath(join(__file__, "../../../")))
import os
import time
import datetime
import threading
from collections import deque

from typing import (
    Any,
    List,
    Callable,
    Optional,
)
import logging
import asyncio

from flask import Flask, render_template, request
from turbo_flask import Turbo
from telegram.bot import Bot
from telegram.update import Update

import hummingbot
from hummingbot.logger import HummingbotLogger
from hummingbot.connector.in_flight_order_base import TradeType
from hummingbot.notifier.notifier_base import NotifierBase
from hummingbot.model.trade_fill import TradeFill
from hummingbot.client.config.global_config_map import global_config_map
from hummingbot.core.utils.async_call_scheduler import AsyncCallScheduler
from hummingbot.core.utils.async_utils import safe_ensure_future

BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(os.path.dirname(__file__))))
template_dir = os.path.join(os.path.abspath(os.path.dirname(__file__)), "templates")
_flask_app: Flask = Flask(__name__, template_folder=template_dir)
_turbo = Turbo(_flask_app)


async def start_app(_app: Flask, port):
    threading.Thread(target=_app.run, kwargs=dict(host="0.0.0.0", port=port)).start()


DISABLED_COMMANDS = {
    "connect",             # disabled because telegram can't display secondary prompt
    "create",              # disabled because telegram can't display secondary prompt
    # "import",              # disabled because telegram can't display secondary prompt
    "export",              # disabled for security
}


def authorized_only(handler: Callable[[Any, Bot, Update], None]) -> Callable[..., Any]:
    """ Decorator to check if the message comes from the correct chat_id """
    def wrapper(self, *args, **kwargs):
        try:
            return handler(self, *args, **kwargs)
        except Exception as e:
            WebNotifier.logger().exception(f"Exception occurred within Web module: {e}")

    return wrapper


class WebNotifier(NotifierBase):
    tn_logger: Optional[HummingbotLogger] = None
    _instance: "WebNotifier" = None

    @classmethod
    def logger(cls) -> HummingbotLogger:
        if cls.tn_logger is None:
            cls.tn_logger = logging.getLogger(__name__)
        return cls.tn_logger

    @classmethod
    def get_instance(cls, **kwargs) -> "WebNotifier":
        if cls._instance is None:
            cls._instance = WebNotifier(**kwargs)
        return cls._instance

    def __init__(self, port: int, hb: "hummingbot.client.hummingbot_application.HummingbotApplication") -> None:
        global _flask_app
        super().__init__()

        self._port = port or global_config_map.get("web_ui_port").value
        self._app = _flask_app
        self._hb = hb
        self._ev_loop = asyncio.get_event_loop()
        self._async_call_scheduler = AsyncCallScheduler.shared_instance()
        self._msg_queue: asyncio.Queue = asyncio.Queue()
        self._send_msg_task: Optional[asyncio.Task] = None
        self._web_ui_application_task: Optional[asyncio.Task] = None
        self.last_messages = []

    def start(self):
        if not self._started:
            self._started = True
            self._web_ui_application_task = safe_ensure_future(start_app(self._app, self._port), loop=self._ev_loop)
            self._send_msg_task = safe_ensure_future(self.send_msg_from_queue(), loop=self._ev_loop)
            self.logger().info("Web is listening...")

    def stop(self) -> None:
        if self._send_msg_task:
            self._send_msg_task.cancel()
        if self._web_ui_application_task:
            self._web_ui_application_task.cancel()

    async def async_handler(self, command: str) -> None:
        async_scheduler: AsyncCallScheduler = AsyncCallScheduler.shared_instance()
        try:
            output = f"\n[WEB Input] {command}"

            self._hb.app.log(output)

            # if the command does starts with any disabled commands
            if any([command.lower().startswith(dc) for dc in DISABLED_COMMANDS]):
                self.add_msg_to_queue(f"Command {command} is disabled from web")
            else:
                await async_scheduler.call_async(self._hb._handle_command, command)
        except Exception as e:
            self.add_msg_to_queue(str(e))

    def handler(self, command: str) -> None:
        safe_ensure_future(self.async_handler(command=command), loop=self._ev_loop)

    @staticmethod
    def _divide_chunks(arr: List[Any], n: int = 5):
        """ Break a list into chunks of size N """
        for i in range(0, len(arr), n):
            yield arr[i:i + n]

    def add_msg_to_queue(self, msg: str):
        lines: List[str] = msg.split("\n")
        msg_chunks: List[List[str]] = self._divide_chunks(lines, 30)
        for chunk in msg_chunks:
            self._msg_queue.put_nowait("\n".join(chunk))

    async def send_msg_from_queue(self):
        while True:
            try:
                new_msg: str = await self._msg_queue.get()
                if isinstance(new_msg, str) and len(new_msg) > 0:
                    self.last_messages.append(new_msg)
                    if len(self.last_messages) > 10:
                        self.last_messages = self.last_messages[-10:]
            except Exception as e:
                self.logger().error(str(e))
            await asyncio.sleep(1)


@_flask_app.route('/', methods=['post', 'get'])
def index():
    if request.method == 'POST':
        command = request.form.get('command')
        web_notifier = WebNotifier.get_instance()
        if command.lower() == 'import':
            command += f" {web_notifier._hb.strategy_file_name}"
        web_notifier.handler(command=command)
    return render_template('index.html')


@_flask_app.route('/logs')
def logs():
    return render_template('logs.html')


@_flask_app.route('/orders')
def orders():
    return render_template('orders.html')


@_flask_app.route('/trades')
def trades():
    return render_template('trades.html')


@_flask_app.route('/balances')
def balances():
    return render_template('balances.html')


@_flask_app.route('/config', methods=['post', 'get'])
def config():
    web_notifier = WebNotifier.get_instance()
    config_name = web_notifier._hb.strategy_file_name

    with open(os.path.join(BASE_DIR, "conf", config_name)) as f:
        conf_yml = f.read()

    if request.method == 'POST':
        updated_config = request.form.get('config')

        if conf_yml != updated_config:
            with open(os.path.join(BASE_DIR, "conf", config_name), "w") as f:
                f.write(updated_config)
            conf_yml = updated_config

    return render_template('config.html', conf_yml=conf_yml, config_name=config_name)


@_flask_app.context_processor
def inject_logs():
    web_notifier = WebNotifier.get_instance()
    log_dir = os.path.join(BASE_DIR, global_config_map.get('log_file_path').value)
    log_file_name = f"logs_{web_notifier._hb.strategy_file_name.replace('.yml', '.log')}"

    with open(os.path.join(log_dir, log_file_name)) as f:
        lines = deque(f, maxlen=100)
    res = []
    for line in reversed(lines):
        split_line = line.split('-')
        if len(split_line) < 7:
            continue
        res.append(
            {
                "date": "-".join(split_line[:3]),
                "source": split_line[4],
                "level": split_line[5],
                "log": "-".join(split_line[6::])
            }
        )

    return {"lines": res}


@_flask_app.context_processor
def inject_stat():
    web_notifier = WebNotifier.get_instance()

    return {
        "duration": web_notifier._hb.app.timer.log_lines[0] if len(web_notifier._hb.app.timer.log_lines) > 0 else "",
        "trades": web_notifier._hb.app.trade_monitor.log_lines[0] if len(web_notifier._hb.app.trade_monitor.log_lines) > 0 else "",
        "system_info": web_notifier._hb.app.process_usage.log_lines[0] if len(web_notifier._hb.app.process_usage.log_lines) > 0 else ""
    }


@_flask_app.context_processor
def inject_orders():
    web_notifier = WebNotifier.get_instance()
    orders = {}
    for market in web_notifier._hb.markets.values():
        o = market.in_flight_orders
        sell = list(filter(lambda x: x.trade_type == TradeType.SELL, o.values()))
        buy = list(filter(lambda x: x.trade_type == TradeType.BUY, o.values()))
        if market.name not in orders:
            orders[market.name] = {"sell_orders": [], "buy_orders": []}
        orders[market.name]["sell_orders"] = sell
        orders[market.name]["buy_orders"] = buy
    return {
        "orders": orders
        # "sell_orders": list(filter(lambda x: x.trade_type == TradeType.SELL, orders.values())),
        # "buy_orders": list(filter(lambda x: x.trade_type == TradeType.BUY, orders.values())),
    }


@_flask_app.context_processor
def inject_balances():
    web_notifier = WebNotifier.get_instance()
    balances = {}
    for market in web_notifier._hb.markets.values():
        balances.update({market: market._account_balances})
    return {
        "balances": balances,
    }


@_flask_app.context_processor
def inject_trades():
    web_notifier = WebNotifier.get_instance()
    session = web_notifier._hb.trade_fill_db._session_cls()
    try:
        trades = session.query(TradeFill).filter(TradeFill.config_file_path == web_notifier._hb.strategy_file_name).all()
    except Exception:
        trades = []
    finally:
        session.close()

    return {"trades_hist": trades}


@_flask_app.context_processor
def inject_messages():
    web_notifier = WebNotifier.get_instance()

    return {"messages": web_notifier.last_messages}


@_flask_app.before_first_request
def before_first_request():
    threading.Thread(target=update_load).start()


@_flask_app.template_filter('ctime')
def timectime(s):
    return datetime.datetime.fromtimestamp(int(s) / 1000)


def update_load():
    with _flask_app.app_context():
        while True:
            time.sleep(1)
            _turbo.push(_turbo.replace(render_template('loadavg.html'), 'load'))
            _turbo.push(_turbo.replace(render_template('loadlogs.html'), 'logsTable'))
            _turbo.push(_turbo.replace(render_template('messages.html'), 'messageBox'))
            _turbo.push(_turbo.replace(render_template('loadorders.html'), 'ordersTable'))
            _turbo.push(_turbo.replace(render_template('loadtrades.html'), 'tradesTable'))
            _turbo.push(_turbo.replace(render_template('loadbalances.html'), 'balancesBox'))
