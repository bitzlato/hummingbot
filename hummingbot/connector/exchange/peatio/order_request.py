import dataclasses
from decimal import Decimal

from hummingbot.core.event.events import OrderType


@dataclasses.dataclass
class OrderRequest:
    client_order_id: str
    trading_pair: str
    amount: Decimal
    is_buy: bool
    order_type: OrderType
    price: Decimal

    def __hash__(self):
        return self.client_order_id.__hash__()
