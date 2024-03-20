from pydantic import BaseModel
from typing import Optional
from datetime import datetime


class BridgesNativeEvmEth(BaseModel):

    date: Optional[datetime] = None
    from_address: Optional[str] = None
    to_address: Optional[str] = None
    tc_from: Optional[str] = None
    tc_to: Optional[str] = None
    bridge: Optional[str] = None
    tx_type: Optional[str] = None
    value: Optional[float] = None
    value_usd: Optional[float] = None
    gas_used: Optional[float] = None
    fee_usd: Optional[float] = None
    tx_count: Optional[int] = None
