#!/usr/bin/env python
from decimal import Decimal

import aiohttp
import asyncio
import time

import logging

from typing import (
    Optional,
    AsyncIterable, Any, Dict, List,
)

from hummingbot.connector.exchange.peatio.peatio_auth import PeatioAuth
from hummingbot.connector.exchange.peatio.peatio_urls import PEATIO_WS_URL
from hummingbot.core.data_type.user_stream_tracker_data_source import UserStreamTrackerDataSource
from hummingbot.core.utils.async_utils import safe_ensure_future
from hummingbot.logger import HummingbotLogger


# PEATIO_ACCOUNT_UPDATE_TOPIC = "accounts.update#2"
PEATIO_ORDER_UPDATE_TOPIC = "order"

PEATIO_SUBSCRIBE_TOPICS = {
    PEATIO_ORDER_UPDATE_TOPIC,
}


class PeatioAPIUserStreamDataSource(UserStreamTrackerDataSource):
    _hausds_logger: Optional[HummingbotLogger] = None

    @classmethod
    def logger(cls) -> HummingbotLogger:
        if cls._hausds_logger is None:
            cls._hausds_logger = logging.getLogger(__name__)

        return cls._hausds_logger

    def __init__(self, peatio_auth: PeatioAuth):
        self._current_listen_key = None
        self._current_endpoint = None
        self._listen_for_user_steam_task = None
        self._last_recv_time: float = 0
        self._auth: PeatioAuth = peatio_auth
        self._client_session: aiohttp.ClientSession = None
        self._websocket_connection: aiohttp.ClientWebSocketResponse = None
        super().__init__()

    @property
    def last_recv_time(self) -> float:
        return self._last_recv_time

    async def _subscribe_topic(self, topic: str):
        subscribe_request: Dict[str, Any] = {
            "event": "subscribe",
            "streams": [topic],
        }
        await self._websocket_connection.send_json(subscribe_request)
        self._last_recv_time = time.time()

    async def get_ws_connection(self) -> aiohttp.client._WSRequestContextManager:
        if self._client_session is None:
            self._client_session = aiohttp.ClientSession()

        stream_url: str = f"{PEATIO_WS_URL}" + "?cancel_on_close=1"
        return self._client_session.ws_connect(stream_url, headers=self._auth.add_auth_data())

    async def _socket_user_stream(self) -> AsyncIterable[str]:
        """
        Main iterator that manages the websocket connection.
        """
        while True:
            try:
                raw_msg = await asyncio.wait_for(self._websocket_connection.receive(), timeout=30)
                self._last_recv_time = time.time()

                if raw_msg.type != aiohttp.WSMsgType.TEXT:
                    # since all ws messages from Peatio are TEXT, any other type should cause ws to reconnect
                    return

                yield raw_msg.json()
            except asyncio.TimeoutError as e:
                self.logger().error("Userstream websocket timeout, going to reconnect...", exc_info=e)
                return

    async def listen_for_user_stream(self, ev_loop: asyncio.BaseEventLoop, output: asyncio.Queue):
        while True:
            try:
                # Initialize Websocket Connection
                async with (await self.get_ws_connection()) as ws:
                    self._websocket_connection = ws

                    # Subscribe to Topic(s)
                    await self._subscribe_topic(PEATIO_ORDER_UPDATE_TOPIC)

                    # Listen to WebSocket Connection
                    async for message in self._socket_user_stream():
                        output.put_nowait(message)

            except asyncio.CancelledError:
                raise
            except IOError as e:
                self.logger().error(e, exc_info=True)
            except Exception as e:
                self.logger().error(f"Unexpected error occurred! {e}", exc_info=True)
            finally:
                if self._websocket_connection is not None:
                    await self._websocket_connection.close()
                    self._websocket_connection = None
                if self._client_session is not None:
                    await self._client_session.close()
                    self._client_session = None


class PeatioAPIUserStreamDataSourceNew(UserStreamTrackerDataSource):
    PING_TIMEOUT = 30
    MSG_TIMEOUT = 30
    SUBSCRIBE_TOPICS = []

    _hausds_logger: Optional[HummingbotLogger] = None

    @classmethod
    def logger(cls) -> HummingbotLogger:
        if cls._hausds_logger is None:
            cls._hausds_logger = logging.getLogger(__name__)

        return cls._hausds_logger

    def __init__(self, peatio_auth: PeatioAuth):
        self._current_listen_key = None
        self._current_endpoint = None
        self._listen_for_user_steam_task = None
        self._last_recv_time: float = 0
        self._auth: PeatioAuth = peatio_auth
        self._client_session: aiohttp.ClientSession = None
        self._websocket_connection: aiohttp.ClientWebSocketResponse = None
        super().__init__()

    @property
    def last_recv_time(self) -> float:
        return self._last_recv_time

    async def get_ws_connection(self) -> aiohttp.client._WSRequestContextManager:
        if self._client_session is None:
            self._client_session = aiohttp.ClientSession()
        return self._client_session.ws_connect(PEATIO_WS_URL + "?cancel_on_close=1", headers=self._auth.add_auth_data())

    async def subscribe_to_topics(self, ws_connection: aiohttp.ClientWebSocketResponse, topics: List[str]):
        subscribe_request = {
            "event": "subscribe",
            "streams": [
                topics
            ]
        }
        await ws_connection.send_json(subscribe_request)

    async def _place_order(self, ws_connection: aiohttp.ClientWebSocketResponse,
                           _uuid: str, market: str, side: str, volume: Decimal, ord_type: str, price: Decimal) -> str:

        request = {
            "event": "order",
            "data": {
                "market": market,
                "side": side,
                "volume": str(volume),
                "ord_type": ord_type,
                "price": str(price),
                "uuid": _uuid
            }
        }

        await ws_connection.send_json(request)

        return _uuid

    async def unsubscribe_to_topics(self, ws_connection: aiohttp.ClientWebSocketResponse, topics: List[str]):
        subscribe_request = {
            "event": "unsubscribe",
            "streams": [
                topics
            ]
        }

        await ws_connection.send_json(subscribe_request)

    async def _socket_user_stream(self, ws_connection: aiohttp.ClientWebSocketResponse) -> AsyncIterable[str]:
        """
        Main iterator that manages the websocket connection.
        """
        try:
            while True:
                try:
                    msg = await asyncio.wait_for(ws_connection.receive(), timeout=self.MSG_TIMEOUT)
                    self._last_recv_time = time.time()
                    if msg.type != aiohttp.WSMsgType.TEXT:
                        # since all ws messages from Peatio are TEXT, any other type should cause ws to reconnect
                        return
                    yield msg.json()
                except asyncio.TimeoutError:
                    pong_waiter = await ws_connection.ping()
                    await asyncio.wait_for(pong_waiter, timeout=self.PING_TIMEOUT)
                    self._last_recv_time = time.time()

        except asyncio.TimeoutError:
            self.logger().error("Userstream websocket timeout, going to reconnect...")
        finally:
            await ws_connection.close()

    async def listen_for_user_stream(self, ev_loop: asyncio.BaseEventLoop, output: asyncio.Queue, **kwargs):
        await self.start(ev_loop=ev_loop, output=output, user_input=kwargs.get('user_input'))

    async def listener_order_info_task(self, ws: aiohttp.ClientWebSocketResponse, output: asyncio.Queue):
        await self.subscribe_to_topics(ws, self.SUBSCRIBE_TOPICS)

        # Listen to WebSocket Connection
        async for message in self._socket_user_stream(ws):
            output.put_nowait(message)

    async def _iter_order_stream_queue(self, queue: asyncio.Queue) -> AsyncIterable[Dict[str, Any]]:
        while True:
            try:
                yield await queue.get()
            except asyncio.CancelledError:
                raise
            except Exception as e:
                self.logger().error(f"Unknown error. Retrying after 1 second. {e}", exc_info=True)
                await asyncio.sleep(1.0)

    async def sender_order_task(self, ws: aiohttp.ClientWebSocketResponse, _input: asyncio.Queue):
        async for order_message in self._iter_order_stream_queue(queue=_input):
            try:
                # _uuid: str, market: str, side: str, volume: Decimal, ord_type: str, price: Decimal

                _uuid = order_message["uuid"]
                market = order_message["market"]
                side = order_message["side"]
                volume = order_message["volume"]
                ord_type = order_message["ord_type"]
                price = order_message["price"]

                assert isinstance(_uuid, str), "_uuid must be str"
                assert isinstance(market, str), "market must be str"
                assert isinstance(side, str), "side must be str"
                assert isinstance(volume, Decimal), "volume must be Decimal"
                assert isinstance(ord_type, str), "ord_type must be str"
                assert isinstance(price, Decimal), "price must be Decimal"

                await self._place_order(ws_connection=ws, _uuid=_uuid, market=market,
                                        side=side, volume=volume, ord_type=ord_type, price=price)
            except KeyError:
                self.logger().warning("order data is failed.", exc_info=True)
                continue
            except AssertionError:
                self.logger().warning("order data is failed.", exc_info=True)
                continue
            except asyncio.CancelledError:
                raise
            except Exception as e:
                self.logger().error(f"Unexpected error in user stream listener loop. {e}", exc_info=True)
                await asyncio.sleep(5.0)

    async def start(self, ev_loop: asyncio.BaseEventLoop, user_input: asyncio.Queue, output: asyncio.Queue):
        while True:
            try:
                # Initialize Websocket Connection
                self.logger().info("create new ws connection")
                async with (await self.get_ws_connection()) as ws:
                    self._websocket_connection = ws

                    listener_task = safe_ensure_future(self.listener_order_info_task(ws=ws, output=output))
                    sender_task = safe_ensure_future(self.sender_order_task(ws=ws, _input=user_input))

                    while not ws.closed:
                        time.sleep(0.01)

                    listener_task.cancel()
                    sender_task.cancel()
            #
            except asyncio.CancelledError:
                raise
            except Exception:
                self.logger().error("Unexpected error occurred!", exc_info=True)
            finally:
                if self._websocket_connection is not None:
                    await self._websocket_connection.close()
                    self._websocket_connection = None
                if self._client_session is not None:
                    await self._client_session.close()
                    self._client_session = None
