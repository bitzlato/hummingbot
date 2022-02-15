import re
import uuid
from typing import (
    Optional,
    Tuple, Dict, Any)
from hummingbot.client.config.config_var import ConfigVar
from hummingbot.client.config.config_methods import using_exchange

from hummingbot.core.event.events import (
    TradeType
)


RE_4_LETTERS_QUOTE = re.compile(r"^(\w+)(usdt|husd)$")
RE_3_LETTERS_QUOTE = re.compile(r"^(\w+)(btc|eth|trx)$")
RE_2_LETTERS_QUOTE = re.compile(r"^(\w+)(ht)$")
TOKEN_TYPES = ["erc-20", "bec-20", "hrc-20", "earc-20", "plgn"]

CENTRALIZED = True

EXAMPLE_PAIR = "ETH-USDT"

DEFAULT_FEES = [0.2, 0.2]

BROKER_ID = "AAc484720a"


class PeatioAPIError(IOError):
    def __init__(self, error_payload: Dict[str, Any]):
        super().__init__(str(error_payload))
        self.error_payload = error_payload


# def split_trading_pair(trading_pair: str) -> Optional[Tuple[str, str]]:
#     try:
#         m = RE_4_LETTERS_QUOTE.match(trading_pair)
#         if m is None:
#             m = RE_3_LETTERS_QUOTE.match(trading_pair)
#             if m is None:
#                 m = RE_2_LETTERS_QUOTE.match(trading_pair)
#         return m.group(1), m.group(2)
#     # Exceptions are now logged as warnings in trading pair fetcher
#     except Exception:
#         return None


def split_trading_pair(trading_pair: str) -> Optional[Tuple[str, str, str, str]]:
    try:
        base, quote = trading_pair.split(sep="_", maxsplit=1)
        base_type = next(iter([i.replace("-", "") for i in TOKEN_TYPES if i.replace("-", "") in base]), "")
        base = base.replace(base_type, "", 1)
        quote_type = next(iter([i.replace("-", "") for i in TOKEN_TYPES if i.replace("-", "") in quote]), "")
        quote = quote.replace(quote_type, "", 1)

        return base, base_type, quote, quote_type
    # Exceptions are now logged as warnings in trading pair fetcher
    except Exception:
        return None


def convert_from_exchange_trading_pair(exchange_trading_pair: str) -> Optional[str]:
    if split_trading_pair(exchange_trading_pair) is None:
        return None
    # Peatio uses lowercase (btcusdt)
    base_asset, base_type, quote_asset, quote_type = split_trading_pair(exchange_trading_pair)
    return f"{base_asset.upper()}{base_type.upper()}-{quote_asset.upper()}{quote_type.upper()}"


def convert_to_exchange_trading_pair(trading_pair: str) -> str:
    # Peatio uses lowercase (btcusdt)
    return trading_pair.replace("-", "_").lower()


def get_new_client_order_id(trade_type: TradeType, trading_pair: str) -> str:
    # side = ""
    # if trade_type is TradeType.BUY:
    #     side = "buy"
    # if trade_type is TradeType.SELL:
    #     side = "sell"
    # tracking_nonce = get_tracking_nonce()
    # return f"{BROKER_ID}-{side}-{trading_pair}-{tracking_nonce}"
    return str(uuid.uuid4())


KEYS = {
    "peatio_access_key":
        ConfigVar(key="peatio_access_key",
                  prompt="Enter your Peatio ACCESS key >>> ",
                  required_if=using_exchange("peatio"),
                  is_secure=True,
                  is_connect_key=True),
    "peatio_secret_key":
        ConfigVar(key="peatio_secret_key",
                  prompt="Enter your Peatio secret key >>> ",
                  required_if=using_exchange("peatio"),
                  is_secure=True,
                  is_connect_key=True),
}
