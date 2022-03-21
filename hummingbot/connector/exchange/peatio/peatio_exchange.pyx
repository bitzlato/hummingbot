from asyncio import CancelledError

import aiohttp
import asyncio
from decimal import Decimal

import uuid

from aiohttp import ClientTimeout, ServerTimeoutError
from libc.stdint cimport int64_t
import logging
import time
import pandas as pd
from typing import (
    Any,
    AsyncIterable,
    Dict,
    List,
    Optional
)
import ujson

from hummingbot.core.clock cimport Clock

from hummingbot.connector.exchange.peatio.order_request import OrderRequest
from hummingbot.connector.exchange.peatio.peatio_api_order_book_data_source import PeatioAPIOrderBookDataSource
from hummingbot.connector.exchange.peatio.peatio_auth import PeatioAuth
from hummingbot.connector.exchange.peatio.peatio_user_stream_tracker import PeatioUserStreamTracker
from hummingbot.core.data_type.cancellation_result import CancellationResult
from hummingbot.core.data_type.limit_order import LimitOrder
from hummingbot.core.data_type.order_book cimport OrderBook
from hummingbot.core.data_type.transaction_tracker import TransactionTracker
from hummingbot.core.event.events import (
    MarketEvent,
    BuyOrderCompletedEvent,
    SellOrderCompletedEvent,
    OrderFilledEvent,
    OrderCancelledEvent,
    BuyOrderCreatedEvent,
    SellOrderCreatedEvent,
    MarketTransactionFailureEvent,
    MarketOrderFailureEvent,
    OrderType,
    TradeType,
    TradeFee
)
from hummingbot.core.network_iterator import NetworkStatus
from hummingbot.core.utils.async_call_scheduler import AsyncCallScheduler
from hummingbot.core.utils.async_utils import (
    safe_ensure_future,
    safe_gather,
)
from hummingbot.logger import HummingbotLogger
from hummingbot.connector.exchange.peatio.peatio_api_user_stream_data_source import (
    PEATIO_SUBSCRIBE_TOPICS,
    # PEATIO_ACCOUNT_UPDATE_TOPIC,
    PEATIO_ORDER_UPDATE_TOPIC
)
from hummingbot.connector.exchange.peatio.peatio_in_flight_order import PeatioInFlightOrder
from hummingbot.connector.exchange.peatio.peatio_order_book_tracker import PeatioOrderBookTracker
from hummingbot.connector.exchange.peatio.peatio_urls import PEATIO_ROOT_API
from hummingbot.connector.exchange.peatio.peatio_utils import (
    convert_to_exchange_trading_pair,
    convert_from_exchange_trading_pair,
    get_new_client_order_id,
    PeatioAPIError
)
from hummingbot.connector.trading_rule cimport TradingRule
from hummingbot.connector.exchange_base import ExchangeBase
from hummingbot.core.utils.tracking_nonce import get_tracking_nonce
from hummingbot.core.utils.estimate_fee import estimate_fee

hm_logger = None
s_decimal_0 = Decimal(0)
s_decimal_NaN = Decimal("NaN")
s_decimal_inf = Decimal('inf')


cdef class PeatioExchangeTransactionTracker(TransactionTracker):
    cdef:
        PeatioExchange _owner

    def __init__(self, owner: PeatioExchange):
        super().__init__()
        self._owner = owner

    cdef c_did_timeout_tx(self, str tx_id):
        TransactionTracker.c_did_timeout_tx(self, tx_id)
        self._owner.c_did_timeout_tx(tx_id)

cdef class PeatioExchange(ExchangeBase):
    MARKET_BUY_ORDER_COMPLETED_EVENT_TAG = MarketEvent.BuyOrderCompleted.value
    MARKET_SELL_ORDER_COMPLETED_EVENT_TAG = MarketEvent.SellOrderCompleted.value
    MARKET_ORDER_CANCELLED_EVENT_TAG = MarketEvent.OrderCancelled.value
    MARKET_TRANSACTION_FAILURE_EVENT_TAG = MarketEvent.TransactionFailure.value
    MARKET_ORDER_FAILURE_EVENT_TAG = MarketEvent.OrderFailure.value
    MARKET_ORDER_FILLED_EVENT_TAG = MarketEvent.OrderFilled.value
    MARKET_BUY_ORDER_CREATED_EVENT_TAG = MarketEvent.BuyOrderCreated.value
    MARKET_SELL_ORDER_CREATED_EVENT_TAG = MarketEvent.SellOrderCreated.value
    API_CALL_TIMEOUT = 10.0
    UPDATE_ORDERS_INTERVAL = 10.0
    SHORT_POLL_INTERVAL = 5.0
    LONG_POLL_INTERVAL = 25.0

    @classmethod
    def logger(cls) -> HummingbotLogger:
        global hm_logger
        if hm_logger is None:
            hm_logger = logging.getLogger(__name__)
        return hm_logger

    def __init__(self,
                 peatio_access_key: str,
                 peatio_secret_key: str,
                 trading_pairs: Optional[List[str]] = None,
                 trading_required: bool = True):

        super().__init__()
        self._account_id = ""
        self._async_scheduler = AsyncCallScheduler(call_interval=0.5)
        self._ev_loop = asyncio.get_event_loop()
        self._peatio_auth = PeatioAuth(access_key=peatio_access_key, secret_key=peatio_secret_key)
        self._in_flight_orders = {}
        self._last_poll_timestamp = 0
        self._last_timestamp = 0
        self._order_book_tracker = PeatioOrderBookTracker(trading_pairs=trading_pairs)
        self._poll_notifier = asyncio.Event()
        self._shared_client = None
        self._status_polling_task = None
        self._trading_required = trading_required
        self._trading_rules = {}
        self._trading_rules_polling_task = None
        self._order_sync_polling_task = None
        self._tx_tracker = PeatioExchangeTransactionTracker(self)

        self._user_stream_event_listener_task = None
        self._user_stream_tracker = PeatioUserStreamTracker(peatio_auth=self._peatio_auth, trading_pairs=trading_pairs)

    @property
    def name(self) -> str:
        return "peatio"

    @property
    def order_book_tracker(self) -> PeatioOrderBookTracker:
        return self._order_book_tracker

    @property
    def order_books(self) -> Dict[str, OrderBook]:
        return self._order_book_tracker.order_books

    @property
    def trading_rules(self) -> Dict[str, TradingRule]:
        return self._trading_rules

    @property
    def in_flight_orders(self) -> Dict[str, PeatioInFlightOrder]:
        return self._in_flight_orders

    @property
    def limit_orders(self) -> List[LimitOrder]:
        return [
            in_flight_order.to_limit_order()
            for in_flight_order in self._in_flight_orders.values()
        ]

    @property
    def tracking_states(self) -> Dict[str, Any]:
        return {
            key: value.to_json()
            for key, value in self._in_flight_orders.items()
        }

    def restore_tracking_states(self, saved_states: Dict[str, Any]):
        self._in_flight_orders.update({
            key: PeatioInFlightOrder.from_json(value)
            for key, value in saved_states.items()
        })

    @property
    def shared_client(self) -> str:
        return self._shared_client

    @shared_client.setter
    def shared_client(self, client: aiohttp.ClientSession):
        self._shared_client = client

    async def get_active_exchange_markets(self) -> pd.DataFrame:
        return await PeatioAPIOrderBookDataSource.get_active_exchange_markets()

    cdef c_start(self, Clock clock, double timestamp):
        self._tx_tracker.c_start(clock, timestamp)
        ExchangeBase.c_start(self, clock, timestamp)

    cdef c_stop(self, Clock clock):
        ExchangeBase.c_stop(self, clock)
        self._async_scheduler.stop()

    async def start_network(self):
        self._stop_network()
        self._order_book_tracker.start()
        self._trading_rules_polling_task = safe_ensure_future(self._trading_rules_polling_loop())
        # self._order_sync_polling_task = safe_ensure_future(self._order_sync_polling_loop())
        self._user_stream_event_listener_task = safe_ensure_future(self._user_stream_event_listener())
        if self._trading_required:
            self._user_stream_tracker_task = safe_ensure_future(self._user_stream_tracker.start())
            self._status_polling_task = safe_ensure_future(self._status_polling_loop())

    def _stop_network(self):
        self._order_book_tracker.stop()
        if self._status_polling_task is not None:
            self._status_polling_task.cancel()
            self._status_polling_task = None
        if self._trading_rules_polling_task is not None:
            self._trading_rules_polling_task.cancel()
            self._trading_rules_polling_task = None
        if self._user_stream_event_listener_task is not None:
            self._user_stream_event_listener_task.cancel()
            self._user_stream_event_listener_task = None
        if self._user_stream_tracker_task is not None:
            self._user_stream_tracker_task.cancel()
            self._user_stream_tracker_task = None
        if self._order_sync_polling_task is not None:
            self._order_sync_polling_task.cancel()
            self._order_sync_polling_task = None

    async def stop_network(self):
        self._stop_network()

    async def check_network(self) -> NetworkStatus:
        try:
            await self._api_request(method="get", path_url="/public/timestamp", is_auth_required=True)
        except asyncio.CancelledError:
            raise
        except Exception as e:
            return NetworkStatus.NOT_CONNECTED
        return NetworkStatus.CONNECTED

    cdef c_tick(self, double timestamp):
        cdef:
            double now = time.time()
            double poll_interval = (self.SHORT_POLL_INTERVAL
                                    if now - self._user_stream_tracker.last_recv_time > 60.0
                                    else self.LONG_POLL_INTERVAL)
            int64_t last_tick = <int64_t> (self._last_timestamp / poll_interval)
            int64_t current_tick = <int64_t> (timestamp / poll_interval)
        ExchangeBase.c_tick(self, timestamp)
        self._tx_tracker.c_tick(timestamp)
        if current_tick > last_tick:
            if not self._poll_notifier.is_set():
                self._poll_notifier.set()
        self._last_timestamp = timestamp

    async def _http_client(self) -> aiohttp.ClientSession:
        if self._shared_client is None:
            self._shared_client = aiohttp.ClientSession()
        return self._shared_client

    async def _api_request(self, method, path_url, params: Optional[Dict[str, Any]] = None, data=None,
                           is_auth_required: bool = True):

        content_type = "application/json"
        accept = "application/json"

        headers = {
            "Content-Type": content_type,
            "Accept": accept,
        }

        url = PEATIO_ROOT_API + path_url

        client = await self._http_client()
        if is_auth_required:
            headers = self._peatio_auth.add_auth_data(headers=headers)
        timeout = ClientTimeout(connect=0.15, total=self.API_CALL_TIMEOUT)
        try:
            if not data:
                response = await client.request(
                    method=method.upper(),
                    url=url,
                    headers=headers,
                    params=params,
                    timeout=timeout
                )
            else:
                response = await client.request(
                    method=method.upper(),
                    url=url,
                    headers=headers,
                    params=params,
                    data=ujson.dumps(data),
                    timeout=timeout
                )
        except Exception as e:
            self._shared_client = None
            raise e

        if response.status not in [200, 201]:
            if response.status in [422, 404]:
                raise PeatioAPIError(await response.json())
            raw_text = await response.text()
            raise IOError(f"Error fetching data from {url}. HTTP status is {response.status} with response {raw_text}. Nonce={headers.get('X-Auth-Nonce')}")

        try:
            data = await response.json()
        except Exception as e:
            raise IOError(f"Error parsing data from {url}. status={response.status} with error {str(e)}")

        return data

    async def _update_balances(self):
        cdef:
            dict data
            list balances
            dict new_available_balances = {}
            dict new_balances = {}
            str asset_name
            object balance

        balances = await self._api_request("get", path_url=f"/account/balances", is_auth_required=True)
        if len(balances) > 0:
            for balance_entry in balances:
                asset_name = balance_entry["currency"].replace("-", "").upper()
                balance = Decimal(balance_entry["balance"])
                locked_balance = Decimal(balance_entry["locked"])
                if balance == s_decimal_0:
                    continue
                if asset_name not in new_available_balances:
                    new_available_balances[asset_name] = s_decimal_0
                if asset_name not in new_balances:
                    new_balances[asset_name] = s_decimal_0
                new_balances[asset_name] += balance
                if balance > s_decimal_0:
                    new_available_balances[asset_name] = balance - locked_balance

            self._account_available_balances.clear()
            self._account_available_balances = new_available_balances
            self._account_balances.clear()
            self._account_balances = new_balances

    cdef object c_get_fee(self,
                          str base_currency,
                          str quote_currency,
                          object order_type,
                          object order_side,
                          object amount,
                          object price):

        is_maker = order_type is OrderType.LIMIT_MAKER
        return estimate_fee(self.name, is_maker)

    async def _update_trading_rules(self):
        cdef:
            # The poll interval for trade rules is 60 seconds.
            int64_t last_tick = <int64_t> (self._last_timestamp / 60.0)
            int64_t current_tick = <int64_t> (self._current_timestamp / 60.0)
        if current_tick > last_tick or len(self._trading_rules) < 1:
            exchange_markets = await self._api_request("get", path_url="/public/markets")
            trading_rules_list = self._format_trading_rules(exchange_markets)
            self._trading_rules.clear()
            for trading_rule in trading_rules_list:
                self._trading_rules[convert_from_exchange_trading_pair(trading_rule.trading_pair)] = trading_rule

    def _format_trading_rules(self, raw_trading_pair_info: List[Dict[str, Any]]) -> List[TradingRule]:
        """
        raw_trading_pair_info example:
        [
          {
            "symbol": "string",
            "name": "string",
            "type": "string",
            "base_unit": "string",
            "quote_unit": "string",
            "min_price": 0,
            "max_price": 0,
            "min_amount": 0,
            "amount_precision": 0,
            "price_precision": 0,
            "state": "string"
          }
        ]
        """
        cdef:
            list trading_rules = []

        for info in filter(lambda x: x['state'] == 'enabled', raw_trading_pair_info):
            try:
                trading_rules.append(
                    TradingRule(
                        trading_pair=info["symbol"],
                        min_order_size=Decimal(info["min_amount"]),
                        max_order_size=s_decimal_inf,  # TODO Не ограничен?
                        min_price_increment=Decimal(f"1e-{info['price_precision']}"),
                        min_base_amount_increment=Decimal(f"1e-{info['amount_precision']}"),
                        min_quote_amount_increment=Decimal(f"1e-{info['amount_precision']}"),
                        # TODO Равен amount_precision?
                        min_notional_size=Decimal(info["min_amount"]) * Decimal(info["min_price"])
                    )
                )
            except Exception:
                self.logger().error(f"Error parsing the trading pair rule {info}. Skipping.", exc_info=True)
        return trading_rules

    async def get_order_status(self, exchange_order_id: str) -> Dict[str, Any]:
        """
        Example:
        {
          "id": 0,
          "uuid": "string",
          "side": "string",
          "ord_type": "string",
          "price": 0,
          "avg_price": 0,
          "state": "string",
          "market": "string",
          "market_type": "string",
          "created_at": "string",
          "updated_at": "string",
          "origin_volume": 0,
          "remaining_volume": 0,
          "executed_volume": 0,
          "maker_fee": 0,
          "taker_fee": 0,
          "trades_count": 0,
          "trades": [
            {
              "id": "string",
              "price": 0,
              "amount": 0,
              "total": 0,
              "fee_currency": 0,
              "fee": 0,
              "fee_amount": 0,
              "market": "string",
              "market_type": "string",
              "created_at": "string",
              "taker_type": "string",
              "side": "string",
              "order_id": 0
            }
          ]
        }
        """
        path_url = f"/market/orders/{exchange_order_id}"
        return await self._api_request("get", path_url=path_url, is_auth_required=True)

    async def update_tracked_order(self, order_obj: dict, tracked_order: PeatioInFlightOrder, exch_order_id):
        order_state = order_obj["state"]
        tracked_order.exchange_order_id = exch_order_id

        # possible order states are "wait", "done", "cancel", "rejected", "reject"
        self.logger().info(f"update_tracked_order --- 0; order_state: {order_state}, order_id: {tracked_order.client_order_id}, exchange_order_id: {exch_order_id}")
        if order_state not in ["wait", "done", "cancel", "rejected", "reject"]:
            self.logger().warning(f"Unrecognized order update response - {order_obj}")

        # Calculate the newly executed amount for this update.
        tracked_order.last_state = order_state
        new_confirmed_amount = Decimal(order_obj["remaining_volume"])
        execute_amount_diff = Decimal(order_obj["executed_volume"]) - tracked_order.executed_amount_base
        self.logger().debug(f"update_tracked_order --- 1; execute_amount_diff: {execute_amount_diff} new_confirmed_amount: {new_confirmed_amount}")

        if execute_amount_diff > s_decimal_0:
            tracked_order.fee_paid = Decimal(order_obj.get("maker_fee", 0))
            self.logger().debug(f"update_tracked_order --- 2; fee_paid: {tracked_order.fee_paid}")

            for trade in order_obj.get('trades', []):
                if trade["id"] in tracked_order.trade_ids:
                    continue
                self.logger().debug(f"update_tracked_order --- 3; executed_amount_base: {tracked_order.executed_amount_base}, executed_amount_quote: {tracked_order.executed_amount_quote}")
                tracked_order.executed_amount_base += Decimal(trade.get("amount", 0))
                tracked_order.executed_amount_quote += Decimal(trade.get("total", 0))
                tracked_order.amount = new_confirmed_amount

                price = Decimal(trade.get("price", 0))
                self.logger().debug(f"update_tracked_order --- 4; price: {price}")
                order_filled_event = OrderFilledEvent(
                    timestamp=self._current_timestamp,
                    order_id=tracked_order.client_order_id,
                    trading_pair=tracked_order.trading_pair,
                    trade_type=tracked_order.trade_type,
                    order_type=tracked_order.order_type,
                    price=price,
                    amount=Decimal(trade["amount"]),
                    trade_fee=self.c_get_fee(
                        tracked_order.base_asset,
                        tracked_order.quote_asset,
                        tracked_order.order_type,
                        tracked_order.trade_type,
                        price,
                        execute_amount_diff,
                    ),
                    exchange_trade_id=str(trade["id"])
                )
                self.logger().debug(f"update_tracked_order --- 5; price: {price}")
                self.logger().info(f"Filled {execute_amount_diff} out of {tracked_order.amount} of the "
                                   f"order {tracked_order.client_order_id}.")
                self.c_trigger_event(self.MARKET_ORDER_FILLED_EVENT_TAG, order_filled_event)
                tracked_order.trade_ids.add(trade["id"])

        if tracked_order.is_open:
            self.logger().debug(f"update_tracked_order --- 6; is_open: {tracked_order.is_open}")
            return tracked_order

        if tracked_order.is_done:
            self.logger().info(f"update_tracked_order --- 7; is_done: {tracked_order.is_done}")
            self.c_stop_tracking_order(tracked_order.client_order_id)
            if tracked_order.trade_type is TradeType.BUY:
                self.logger().warning(f"update_tracked_order --- 8;")
                self.logger().info(f"The market buy order {tracked_order.client_order_id} has completed "
                                   f"according to order status API.")
                self.c_trigger_event(
                    self.MARKET_BUY_ORDER_COMPLETED_EVENT_TAG,
                    BuyOrderCompletedEvent(
                        timestamp=self._current_timestamp,
                        order_id=tracked_order.client_order_id,
                        base_asset=tracked_order.base_asset,
                        quote_asset=tracked_order.quote_asset,
                        fee_asset=tracked_order.fee_asset or tracked_order.base_asset,
                        base_asset_amount=tracked_order.executed_amount_base,
                        quote_asset_amount=tracked_order.executed_amount_quote,
                        fee_amount=tracked_order.fee_paid,
                        order_type=tracked_order.order_type,
                        exchange_order_id=exch_order_id
                    )
                )
            else:
                self.logger().info(f"The market sell order {tracked_order.client_order_id} has completed "
                                   f"according to order status API.")
                self.logger().info(f"update_tracked_order --- 9;")
                self.c_trigger_event(
                    self.MARKET_SELL_ORDER_COMPLETED_EVENT_TAG,
                    SellOrderCompletedEvent(
                        timestamp=self._current_timestamp,
                        order_id=tracked_order.client_order_id,
                        base_asset=tracked_order.base_asset,
                        quote_asset=tracked_order.quote_asset,
                        fee_asset=tracked_order.fee_asset or tracked_order.quote_asset,
                        base_asset_amount=tracked_order.executed_amount_base,
                        quote_asset_amount=tracked_order.executed_amount_quote,
                        fee_amount=tracked_order.fee_paid,
                        order_type=tracked_order.order_type,
                        exchange_order_id=exch_order_id
                    )
                )

        if tracked_order.is_cancelled:
            self.logger().info(f"The order {tracked_order.client_order_id} has been cancelled "
                               f"according to order delta websocket API.")
            self.logger().debug(f"update_tracked_order --- 10;")
            self.c_trigger_event(self.MARKET_ORDER_CANCELLED_EVENT_TAG,
                                 OrderCancelledEvent(
                                     timestamp=self._current_timestamp,
                                     order_id=tracked_order.client_order_id,
                                     exchange_order_id=exch_order_id
                                 ))
            self.c_stop_tracking_order(tracked_order.client_order_id)

        if tracked_order.is_failure:
            self.logger().info(f"The order {tracked_order.client_order_id} has been rejected "
                               f"according to order delta websocket API.")
            self.logger().debug(f"update_tracked_order --- 11;")
            self.c_trigger_event(self.MARKET_ORDER_FAILURE_EVENT_TAG,
                                 MarketOrderFailureEvent(
                                     timestamp=self._current_timestamp,
                                     order_id=tracked_order.client_order_id,
                                     order_type=tracked_order.order_type
                                 ))
            self.c_stop_tracking_order(tracked_order.client_order_id)

        return tracked_order

    async def _update_order_status(self):
        cdef:
            # The poll interval for order status is 10 seconds.
            int64_t last_tick = <int64_t> (self._last_poll_timestamp / self.UPDATE_ORDERS_INTERVAL)
            int64_t current_tick = <int64_t> (self._current_timestamp / self.UPDATE_ORDERS_INTERVAL)

        if current_tick > last_tick and len(self._in_flight_orders) > 0:
            tracked_orders = list(self._in_flight_orders.values())
            for tracked_order in tracked_orders:
                try:
                    order_update = await self.get_order_status(tracked_order.client_order_id)
                except PeatioAPIError as e:
                    errors = e.error_payload.get("errors")
                    if "record.not_found" in errors:
                        self.c_stop_tracking_order(tracked_order.client_order_id)
                        self.c_trigger_event(self.MARKET_ORDER_FAILURE_EVENT_TAG,
                                             MarketOrderFailureEvent(
                                                 self._current_timestamp,
                                                 tracked_order.client_order_id,
                                                 tracked_order.order_type
                                             ))
                    else:
                        self.logger().error(f"Fail to retrieve order update for {tracked_order.client_order_id} - {errors}")
                    continue

                if order_update is None:
                    self.logger().network(
                        f"Error fetching status update for the order {tracked_order.client_order_id}: "
                        f"{order_update}.",
                        app_warning_msg=f"Could not fetch updates for the order {tracked_order.client_order_id}. "
                                        f"The order has either been filled or canceled."
                    )
                    continue
                exchange_order_id = str(order_update['id'])
                await self.update_tracked_order(
                    order_obj=order_update,
                    tracked_order=tracked_order,
                    exch_order_id=exchange_order_id
                )

    async def _status_polling_loop(self):
        while True:
            try:
                self._poll_notifier = asyncio.Event()
                await self._poll_notifier.wait()

                await safe_gather(
                    self._update_balances(),
                    self._update_order_status(),
                )
                self._last_poll_timestamp = self._current_timestamp
            except asyncio.CancelledError:
                raise
            except Exception:
                self.logger().network("Unexpected error while fetching account updates.",
                                      exc_info=True,
                                      app_warning_msg="Could not fetch account updates from Peatio. "
                                                      "Check API key and network connection.")
                await asyncio.sleep(0.5)

    async def _order_sync_polling_loop(self):
        while True:
            try:
                # for pair in set(map(self.in_flight_orders.values())):
                await safe_gather(
                    *list(
                        map(lambda x: self.sync_orders(trading_pair=x, depth=0),
                            set(map(lambda x: x.trading_pair, self.in_flight_orders.values())))
                    )
                )
            except Exception:
                self.logger().network("Unexpected error while sync orders.",
                                      exc_info=True,
                                      app_warning_msg="Could not fetch orders from Peatio. "
                                                      "Check API key and network connection.")
            await asyncio.sleep(10)

    async def _trading_rules_polling_loop(self):
        while True:
            try:
                await self._update_trading_rules()
                await asyncio.sleep(60 * 5)
            except asyncio.CancelledError:
                raise
            except Exception:
                self.logger().network("Unexpected error while fetching trading rules.",
                                      exc_info=True,
                                      app_warning_msg="Could not fetch new trading rules from Peatio. "
                                                      "Check network connection.")
                await asyncio.sleep(0.5)

    async def _iter_user_stream_queue(self) -> AsyncIterable[Dict[str, Any]]:
        while True:
            try:
                yield await self._user_stream_tracker.user_stream.get()
            except asyncio.CancelledError:
                raise
            except Exception as e:
                self.logger().error(f"Unknown error. Retrying after 1 second. {e}", exc_info=True)
                await asyncio.sleep(1.0)

    def get_in_flight_orders_by_exchange_id(self, exchange_order_id: str) -> PeatioInFlightOrder:
        values = [v for v in self._in_flight_orders.values() if v.exchange_order_id == exchange_order_id]
        if len(values) > 0:
            return values[0]

    async def _user_stream_event_listener(self):
        async for stream_message in self._iter_user_stream_queue():
            try:
                for channel in stream_message.keys():
                    if channel not in PEATIO_SUBSCRIBE_TOPICS:
                        continue
                    data = stream_message[channel]
                    # if len(data) == 0 and stream_message["code"] == 200:
                    #     # This is a subcribtion confirmation.
                    #     self.logger().info(f"Successfully subscribed to {channel}")
                    #     continue

                    # if channel == PEATIO_ACCOUNT_UPDATE_TOPIC:
                    #     asset_name = data["currency"].upper()
                    #     balance = data["balance"]
                    #     available_balance = data["available"]
                    #
                    #     self._account_balances.update({asset_name: Decimal(balance)})
                    #     self._account_available_balances.update({asset_name: Decimal(available_balance)})
                    #     continue

                    if channel == PEATIO_ORDER_UPDATE_TOPIC:
                        exchange_order_id = str(data["id"])

                        tracked_order = tracked_order = self._in_flight_orders.get(data["uuid"])
                        if tracked_order is None:
                            continue

                        await self.update_tracked_order(
                            order_obj=data,
                            tracked_order=tracked_order,
                            exch_order_id=exchange_order_id
                        )
                    else:
                        # Ignore all other user stream message types
                        continue
            except asyncio.CancelledError:
                raise
            except Exception as e:
                self.logger().error(f"Unexpected error in user stream listener loop. {e}", exc_info=True)
                await asyncio.sleep(5.0)

    @property
    def status_dict(self) -> Dict[str, bool]:
        return {
            "order_books_initialized": self._order_book_tracker.ready,
            "account_balance": len(self._account_balances) > 0 if self._trading_required else True,
            "trading_rule_initialized": len(self._trading_rules) > 0
        }

    @property
    def ready(self) -> bool:
        return all(self.status_dict.values())

    def supported_order_types(self):
        return [OrderType.LIMIT]

    async def place_order(self,
                          client_order_id: str,
                          trading_pair: str,
                          amount: Decimal,
                          is_buy: bool,
                          order_type: OrderType,
                          price: Decimal) -> dict:
        path_url = "/market/orders"
        side = "buy" if is_buy else "sell"
        order_type_str = str(order_type.name).lower()

        params = {
            "market": convert_to_exchange_trading_pair(trading_pair),
            "side": side,
            "volume": f"{amount:f}",
            "ord_type": order_type_str,
            "price": f"{price:f}",
            "uuid": client_order_id
        }

        try:
            self.logger().info(f"put new order !!!!!")
            self._user_stream_tracker.taker_order_stream.put_nowait(params)
            # exchange_order = await self._api_request(
            #     "post",
            #     path_url=path_url,
            #     data=params,
            #     is_auth_required=True
            # )
        # except ServerTimeoutError as e:
        #     self.logger().error(
        #         f"failed place order with params {params} with error ServerTimeoutError: {e}",
        #         exc_info=True
        #     )
        #     raise e
        except CancelledError as e:
            self.logger().network(
                f"failed place order with params {params} with error CancelledError: {e}",
                exc_info=True
            )
            raise e
        # except PeatioAPIError as e:
        #     self.logger().network(
        #         f"failed place order with params {params} with error PeatioApiError: {e}",
        #         exc_info=True
        #     )
        #     raise e
        except Exception as e:
            self.logger().error(f"Error place order with params {params}: {e}", exc_info=True)
            await self.sync_orders(trading_pair=trading_pair, depth=5)
            raise e
        return params

    async def sync_orders(self, trading_pair: str = None, depth: int = 3):
        self.logger().info(f"start sync orders for market {trading_pair}")
        try:
            exchange_trading_pair = convert_to_exchange_trading_pair(trading_pair) if trading_pair is not None else None
            path_url = "/market/orders"
            wait_orders = await self._api_request(
                "get",
                path_url=path_url,
                data={"market": exchange_trading_pair, "state": "wait"},
                is_auth_required=True
            )

            pending_orders = await self._api_request(
                "get",
                path_url=path_url,
                data={"market": exchange_trading_pair, "state": "pending"},
                is_auth_required=True
            )

            known_order_ids = set(map(lambda x: x.client_order_id, self.in_flight_orders.values()))
            for order_id in set(map(lambda x: str(x['uuid']), wait_orders + pending_orders)).difference(known_order_ids):
                self.logger().info(f"cancel {order_id} order")
                cancel_path_url = f"/market/orders/{order_id}/cancel"
                response = await self._api_request("post", path_url=cancel_path_url, is_auth_required=True)
        except Exception:
            if depth > 0:
                await self.sync_orders(trading_pair=trading_pair, depth=depth - 1)
            else:
                self.logger().error(f"failed to cancel orders for market = {trading_pair}", exc_info=True)
                await self.cancel_all(
                    timeout_seconds=3,
                    trading_pair=trading_pair,
                )

    async def batch_place_order(self, orders_data: List[OrderRequest]):
        path_url = "/market/orders/batch"

        def map_orders_data_to_results(_results: list):
            _result = {}
            for od in orders_data:
                for i, r in enumerate(_results):
                    if od.amount == Decimal(r['origin_volume']) \
                       and od.price == Decimal(r['price']) \
                       and convert_to_exchange_trading_pair(od.trading_pair) == r['market'] \
                       and ("buy" if od.is_buy else "sell") == r['side'] \
                       and str(od.order_type.name).lower() == r['ord_type']:
                        break
                _results.pop(i)
                _result[od] = r
            return _result

        data = []
        for order_data in orders_data:
            func = self.get_data_for_buy_order if order_data.is_buy is True else self.get_data_for_sell_order

            trading_pair, decimal_amount, is_buy, order_type, decimal_price = await func(
                trading_pair=order_data.trading_pair,
                amount=order_data.amount,
                order_type=order_data.order_type,
                price=order_data.price
            )

            data.append(
                {
                    "market": convert_to_exchange_trading_pair(trading_pair),
                    "side": "buy" if is_buy else "sell",
                    "volume": f"{decimal_amount:f}",
                    "ord_type": str(order_type.name).lower(),
                    "price": f"{decimal_price:f}",
                    "uuid": str(uuid.uuid4())
                }
            )

        try:
            self.logger().info({"orders": data})
            exchange_orders = await self._api_request(
                "post",
                path_url=path_url,
                data={"orders": data},
                is_auth_required=True
            )
            self.logger().info(f"exchange_orders: {exchange_orders}, data: {data}")
            for req, exchange_order in map_orders_data_to_results(exchange_orders).items():
                self.c_start_tracking_order(
                    client_order_id=exchange_order['uuid'],
                    trading_pair=trading_pair,
                    order_type=order_type,
                    trade_type=TradeType.BUY if is_buy else TradeType.SELL,
                    price=Decimal(exchange_order['price']),
                    amount=Decimal(exchange_order['origin_volume']),
                )
                tracked_order = self._in_flight_orders.get(req.client_order_id)
                if tracked_order is not None:
                    self.logger().info(
                        f"Created {order_type} {'buy' if is_buy else 'sell'} order {req.client_order_id} ({exchange_order['id']})"
                        f" for {decimal_amount} {trading_pair} with status {exchange_order['state']}.")
                if is_buy is True:
                    self.c_trigger_event(
                        self.MARKET_BUY_ORDER_CREATED_EVENT_TAG,
                        BuyOrderCreatedEvent(
                            timestamp=self._current_timestamp,
                            type=order_type,
                            trading_pair=trading_pair,
                            amount=Decimal(exchange_order['origin_volume']),
                            price=Decimal(exchange_order['price']),
                            order_id=str(req.client_order_id),
                            exchange_order_id=exchange_order["id"]
                        )
                    )
                else:
                    self.c_trigger_event(
                        self.MARKET_SELL_ORDER_CREATED_EVENT_TAG,
                        SellOrderCreatedEvent(
                            timestamp=self._current_timestamp,
                            type=order_type,
                            trading_pair=trading_pair,
                            amount=Decimal(exchange_order['origin_volume']),
                            price=Decimal(exchange_order['price']),
                            order_id=str(req.client_order_id),
                            exchange_order_id=exchange_order["id"]
                        )
                    )
        except asyncio.CancelledError:
            raise
        except Exception:
            for order_data in orders_data:
                self.c_stop_tracking_order(order_id=order_data.client_order_id)
                self.logger().network(
                    f"Error submitting {'buy' if is_buy else 'sell'} {order_data.order_type.name.lower()} order to Peatio for "
                    f"{order_data.amount} {order_data.trading_pair} "
                    f"{order_data.price}.",
                    exc_info=True,
                    app_warning_msg=f"Failed to submit orders to Peatio. Check API key and network connection."
                )
            self.c_trigger_event(self.MARKET_ORDER_FAILURE_EVENT_TAG,
                                 MarketOrderFailureEvent(self._current_timestamp, order_data.client_order_id, order_type))

    async def get_data_for_buy_order(self, trading_pair: str, amount: Decimal,
                                     order_type: OrderType, price: Optional[Decimal] = s_decimal_0):
        cdef:
            TradingRule trading_rule = self._trading_rules[trading_pair]
            object current_price = self.c_get_price(trading_pair, False)
            object decimal_amount
            object decimal_price

        if order_type is OrderType.LIMIT:
            decimal_price = self.c_quantize_order_price(trading_pair, price)
            decimal_amount = self.c_quantize_order_amount(
                trading_pair=trading_pair,
                amount=amount,
                price=price if current_price.is_nan() else current_price
            )

            if decimal_amount < trading_rule.min_order_size:
                raise ValueError(f"Buy order amount {decimal_amount} is lower than the minimum order size "
                                 f"{trading_rule.min_order_size}.")
        else:
            decimal_amount = amount
            decimal_price = price
        return trading_pair, decimal_amount, True, order_type, decimal_price

    async def execute_buy(self,
                          client_order_id: str,
                          trading_pair: str,
                          amount: Decimal,
                          order_type: OrderType,
                          price: Optional[Decimal] = s_decimal_0):

        trading_pair, decimal_amount, is_buy, order_type, decimal_price = await self.get_data_for_buy_order(
            trading_pair=trading_pair, amount=amount, order_type=order_type, price=price
        )
        try:
            exchange_order = await self.place_order(client_order_id, trading_pair, decimal_amount, is_buy, order_type, decimal_price)
            self.c_start_tracking_order(
                client_order_id=str(client_order_id),
                trading_pair=trading_pair,
                order_type=order_type,
                trade_type=TradeType.BUY,
                price=decimal_price,
                amount=decimal_amount,
            )
            tracked_order = self._in_flight_orders.get(client_order_id)
            if tracked_order is not None:
                self.logger().info(
                    f"Created {order_type} buy order {client_order_id} for {decimal_amount} {trading_pair}.")
            self.c_trigger_event(self.MARKET_BUY_ORDER_CREATED_EVENT_TAG,
                                 BuyOrderCreatedEvent(
                                     timestamp=self._current_timestamp,
                                     type=order_type,
                                     trading_pair=trading_pair,
                                     amount=decimal_amount,
                                     price=decimal_price,
                                     order_id=client_order_id,
                                 ))
        except asyncio.CancelledError:
            raise
        except Exception:
            self.c_stop_tracking_order(client_order_id)
            order_type_str = order_type.name.lower()
            self.logger().network(
                f"Error submitting buy {order_type_str} order to Peatio for "
                f"{decimal_amount} {trading_pair} "
                f"{decimal_price}.",
                exc_info=True,
                app_warning_msg=f"Failed to submit buy order to Peatio. Check API key and network connection."
            )
            self.c_trigger_event(self.MARKET_ORDER_FAILURE_EVENT_TAG,
                                 MarketOrderFailureEvent(self._current_timestamp, client_order_id, order_type))

    cdef str c_buy(self,
                   str trading_pair,
                   object amount,
                   object order_type=OrderType.LIMIT,
                   object price=s_decimal_0,
                   dict kwargs={}):
        cdef:
            str client_order_id = get_new_client_order_id(TradeType.BUY, trading_pair)

        safe_ensure_future(self.execute_buy(client_order_id, trading_pair, amount, order_type, price))
        return client_order_id

    async def get_data_for_sell_order(self, trading_pair: str, amount: Decimal,
                                      order_type: OrderType, price: Optional[Decimal] = s_decimal_0):
        cdef:
            TradingRule trading_rule = self._trading_rules[trading_pair]
            object decimal_amount
            object decimal_price
            object current_price = self.c_get_price(trading_pair, False)

        decimal_price = self.c_quantize_order_price(trading_pair, price)
        decimal_amount = self.c_quantize_order_amount(
            trading_pair=trading_pair,
            amount=amount,
            price=decimal_price if current_price.is_nan() else current_price
        )

        if decimal_amount < trading_rule.min_order_size:
            raise ValueError(f"Sell order amount {decimal_amount}({amount}) is lower than the minimum order size "
                             f"{trading_rule.min_order_size}.")
        return trading_pair, decimal_amount, False, order_type, decimal_price

    async def execute_sell(self,
                           client_order_id: str,
                           trading_pair: str,
                           amount: Decimal,
                           order_type: OrderType,
                           price: Optional[Decimal] = s_decimal_0):

        trading_pair, decimal_amount, is_buy, order_type, decimal_price = await self.get_data_for_sell_order(
            trading_pair=trading_pair, amount=amount, order_type=order_type, price=price
        )
        try:
            exchange_order = await self.place_order(client_order_id, trading_pair, decimal_amount, False, order_type, decimal_price)
            self.c_start_tracking_order(
                client_order_id=client_order_id,
                trading_pair=trading_pair,
                order_type=order_type,
                trade_type=TradeType.SELL,
                price=decimal_price,
                amount=decimal_amount
            )
            tracked_order = self._in_flight_orders.get(client_order_id)
            if tracked_order is not None:
                self.logger().info(f"Created {order_type} sell order {client_order_id} for {decimal_amount} {trading_pair}.")
            self.c_trigger_event(self.MARKET_SELL_ORDER_CREATED_EVENT_TAG,
                                 SellOrderCreatedEvent(
                                     timestamp=self._current_timestamp,
                                     type=order_type,
                                     trading_pair=trading_pair,
                                     amount=decimal_amount,
                                     price=decimal_price,
                                     order_id=client_order_id,
                                     exchange_order_id=None
                                 ))
        except asyncio.CancelledError:
            raise
        except Exception:
            self.c_stop_tracking_order(client_order_id)
            order_type_str = order_type.name.lower()
            self.logger().network(
                f"Error submitting sell {order_type_str} order to Peatio for "
                f"{decimal_amount} {trading_pair} "
                f"{decimal_price}.",
                exc_info=True,
                app_warning_msg=f"Failed to submit sell order to Peatio. Check API key and network connection."
            )
            self.c_trigger_event(self.MARKET_ORDER_FAILURE_EVENT_TAG,
                                 MarketOrderFailureEvent(self._current_timestamp, client_order_id, order_type))

    cdef str c_sell(self,
                    str trading_pair,
                    object amount,
                    object order_type=OrderType.LIMIT, object price=s_decimal_0,
                    dict kwargs={}):
        cdef:
            int64_t tracking_nonce = <int64_t> get_tracking_nonce()
            str client_order_id = get_new_client_order_id(TradeType.SELL, trading_pair)
        safe_ensure_future(self.execute_sell(client_order_id, trading_pair, amount, order_type, price))
        return client_order_id

    async def execute_cancel(self, trading_pair: str, client_order_id: str, retry_count: int = 5):
        cancel_id = uuid.uuid4()
        self.logger().info(f"start cancel order ({client_order_id}) [cancel_id={cancel_id}]")
        try:
            tracked_order = self._in_flight_orders.get(client_order_id)
            if tracked_order is None:
                self.logger().warning(f"Order - {client_order_id} not found (already canceled) [cancel_id={cancel_id}]")
                return

            path_url = f"/market/orders/{tracked_order.client_order_id}/cancel"
            response = await self._api_request("post", path_url=path_url, is_auth_required=True)
            self.logger().info(f"finish cancel order ({client_order_id}) [cancel_id={cancel_id}] with response {response}")
            await self.update_tracked_order(
                order_obj=response,
                tracked_order=tracked_order,
                exch_order_id=tracked_order.exchange_order_id
            )
        except PeatioAPIError as e:
            errors = e.error_payload.get("errors")
            if "record.not_found" in errors:
                self.c_stop_tracking_order(tracked_order.client_order_id)
                self.c_trigger_event(self.MARKET_ORDER_FAILURE_EVENT_TAG,
                                     MarketOrderFailureEvent(
                                         self._current_timestamp,
                                         client_order_id,
                                         tracked_order.order_type
                                     ))

            self.logger().error(
                f"Failed to cancel order {client_order_id} [cancel_id={cancel_id}]: {str(e)}",
                exc_info=True,
                app_warning_msg=f"Failed to cancel the order {client_order_id} on Peatio. "
                                f"Check API key and network connection."
            )

        except Exception as e:
            self.logger().error(
                f"Failed to cancel order {client_order_id} [cancel_id={cancel_id}]: {str(e)}",
                exc_info=True,
                app_warning_msg=f"Failed to cancel the order {client_order_id} on Peatio. "
                                f"Check API key and network connection."
            )

    cdef c_cancel(self, str trading_pair, str order_id):
        safe_ensure_future(self.execute_cancel(trading_pair, order_id))
        return order_id

    async def cancel_all(self, timeout_seconds: float, **kwargs) -> List[CancellationResult]:
        self.logger().info(f"cancel all orders for pair {kwargs.get('trading_pair')}")
        data = {}
        open_orders = filter(lambda x: x.is_open, self._in_flight_orders.values())
        if kwargs.get('trading_pair') is not None:
            open_orders = filter(lambda x: x.trading_pair == kwargs.get('trading_pair'), open_orders)
            data['market'] = convert_to_exchange_trading_pair(kwargs.get('trading_pair'))
        if kwargs.get('trade_type') is not None:
            open_orders = filter(lambda x: x.trade_type == kwargs.get('trade_type'), open_orders)
            data['side'] = "buy" if kwargs.get('trade_type') is TradeType.BUY else "sell"
        open_orders = list(open_orders)

        self.logger().debug(f"cancel orders: {open_orders}")
        path_url = "/market/orders/cancel"
        try:
            cancel_all_results = await self._api_request(
                "post",
                path_url=path_url,
                data=data,
                is_auth_required=True
            )
            self.logger().info(f"cancel_all_results: {cancel_all_results}")
            if len(open_orders) < 1:
                return []
            results = await safe_gather(*list(map(lambda x: self.get_order_status(exchange_order_id=str(x['id'])), cancel_all_results)))
            cancellation_results = list(map(lambda x: CancellationResult(x['id'], x["state"] == "cancel"), results))
            return cancellation_results
        except Exception as e:
            self.logger().network(
                "Failed to cancel all orders",
                exc_info=True,
                app_warning_msg=f"Failed to cancel all orders on Peatio. Check API key and network connection."
            )
        return []

    cdef OrderBook c_get_order_book(self, str trading_pair):
        cdef:
            dict order_books = self._order_book_tracker.order_books

        if trading_pair not in order_books:
            raise ValueError(f"No order book exists for '{trading_pair}'.")
        return order_books.get(trading_pair)

    cdef c_did_timeout_tx(self, str tracking_id):
        self.c_trigger_event(self.MARKET_TRANSACTION_FAILURE_EVENT_TAG,
                             MarketTransactionFailureEvent(self._current_timestamp, tracking_id))

    cdef c_start_tracking_order(self,
                                str client_order_id,
                                str trading_pair,
                                object order_type,
                                object trade_type,
                                object price,
                                object amount,):
        self._in_flight_orders[client_order_id] = PeatioInFlightOrder(
            client_order_id=str(client_order_id),
            trading_pair=str(trading_pair),
            order_type=order_type,
            trade_type=trade_type,
            price=price,
            amount=amount,
            exchange_order_id=None,
            initial_state="pending"
        )

    cdef c_stop_tracking_order(self, str order_id):
        if order_id in self._in_flight_orders:
            del self._in_flight_orders[order_id]

    cdef object c_get_order_price_quantum(self, str trading_pair, object price):
        cdef:
            TradingRule trading_rule = self._trading_rules[trading_pair]
        return trading_rule.min_price_increment

    cdef object c_get_order_size_quantum(self, str trading_pair, object order_size):
        cdef:
            TradingRule trading_rule = self._trading_rules[trading_pair]
        return Decimal(trading_rule.min_base_amount_increment)

    cdef object c_quantize_order_amount(self, str trading_pair, object amount, object price=s_decimal_0):
        cdef:
            TradingRule trading_rule = self._trading_rules[trading_pair]
            object quantized_amount = ExchangeBase.c_quantize_order_amount(self, trading_pair, amount)
            object current_price = self.c_get_price(trading_pair, False)
            object notional_size
        # Check against min_order_size. If not passing check, return 0.
        if quantized_amount < trading_rule.min_order_size:
            self.logger().debug(f"quantized_amount ({quantized_amount}) < min_order_size ({trading_rule.min_order_size}) for {trading_pair.upper()}")
            return s_decimal_0

        # Check against max_order_size. If not passing check, return maximum.
        if quantized_amount > trading_rule.max_order_size:
            return trading_rule.max_order_size

        if price == s_decimal_0:
            if current_price.is_nan():
                return quantized_amount
            notional_size = current_price * quantized_amount
        else:
            notional_size = price * quantized_amount
        # Add 1% as a safety factor in case the prices changed while making the order.
        if notional_size < trading_rule.min_notional_size * Decimal("1.01"):
            return s_decimal_0

        return quantized_amount

    def get_price(self, trading_pair: str, is_buy: bool) -> Decimal:
        return self.c_get_price(trading_pair, is_buy)

    def buy(self, trading_pair: str, amount: Decimal, order_type=OrderType.MARKET,
            price: Decimal = s_decimal_NaN, **kwargs) -> str:
        return self.c_buy(trading_pair, amount, order_type, price, kwargs)

    def sell(self, trading_pair: str, amount: Decimal, order_type=OrderType.MARKET,
             price: Decimal = s_decimal_NaN, **kwargs) -> str:
        return self.c_sell(trading_pair, amount, order_type, price, kwargs)

    def cancel(self, trading_pair: str, client_order_id: str):
        return self.c_cancel(trading_pair, client_order_id)

    def get_fee(self,
                base_currency: str,
                quote_currency: str,
                order_type: OrderType,
                order_side: TradeType,
                amount: Decimal,
                price: Decimal = s_decimal_NaN) -> TradeFee:
        return self.c_get_fee(base_currency, quote_currency, order_type, order_side, amount, price)

    def get_order_book(self, trading_pair: str) -> OrderBook:
        return self.c_get_order_book(trading_pair)

    def quantize_order_amount(self, trading_pair: str, amount: Decimal, price: Decimal = s_decimal_0) -> Decimal:
        """
        Applies trading rule to quantize order amount.
        """
        return self.c_quantize_order_amount(trading_pair, amount, price)
