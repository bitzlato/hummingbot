from decimal import Decimal
from typing import (
    Any,
    Dict, Set
)

from hummingbot.core.event.events import (
    OrderType,
    TradeType
)
from hummingbot.connector.in_flight_order_base import InFlightOrderBase


cdef class PeatioInFlightOrder(InFlightOrderBase):
    def __init__(self,
                 client_order_id: str,
                 trading_pair: str,
                 order_type: OrderType,
                 trade_type: TradeType,
                 price: Decimal,
                 amount: Decimal,
                 exchange_order_id: str = None,
                 initial_state: str = "pending"):
        self.trade_ids = set()
        super().__init__(
            client_order_id,
            exchange_order_id,
            trading_pair,
            order_type,
            trade_type,
            price,
            amount,
            initial_state  # wait, done, cancel
        )

    @property
    def is_done(self) -> bool:
        return self.last_state in {"done", }

    @property
    def is_cancelled(self) -> bool:
        return self.last_state in {"cancel", }

    @property
    def is_failure(self) -> bool:
        return self.last_state in {"rejected", "reject", }

    @property
    def is_open(self) -> bool:
        return self.last_state in {"wait", "pending", }

    @classmethod
    def from_json(cls, data: Dict[str, Any]) -> InFlightOrderBase:
        cdef:
            str client_order_id = data.get("client_order_id") or data.get("order_id")
            PeatioInFlightOrder retval = PeatioInFlightOrder(
                client_order_id=client_order_id,
                exchange_order_id=data["exchange_order_id"],
                trading_pair=data["trading_pair"],
                order_type=getattr(OrderType, data["order_type"]),
                trade_type=getattr(TradeType, data["trade_type"]),
                price=Decimal(data["price"]),
                amount=Decimal(data["amount"]),
                initial_state=data["last_state"]
            )
        retval.executed_amount_base = Decimal(data["executed_amount_base"])
        retval.executed_amount_quote = Decimal(data["executed_amount_quote"])
        retval.fee_asset = data["fee_asset"]
        retval.fee_paid = Decimal(data["fee_paid"])
        retval.last_state = data["last_state"]
        return retval
