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


class BridgesTokensEvmEth(BaseModel):
    date: Optional[datetime] = None
    from_address: Optional[str] = None
    to_address: Optional[str] = None
    evt_from_address: Optional[str] = None
    evt_to_address: Optional[str] = None
    symbol: Optional[str] = None
    bridge: Optional[str] = None
    tx_type: Optional[str] = None
    usd_token_value: Optional[float] = None
    gas_used: Optional[float] = None
    fee_usd: Optional[float] = None
    tx_count: Optional[int] = None


class StargateBridgesDailyAgg(BaseModel):

    date: Optional[datetime] = None
    source_chain_name: Optional[str] = None
    destination_chain_name: Optional[str] = None
    user_address: Optional[str] = None
    transfer_type: Optional[str] = None
    currency_symbol: Optional[str] = None
    amount_usd: Optional[float] = None
    tx_fee_usd: Optional[float] = None
    tx_count: Optional[int] = None


class AcrossAggregatorDaily(BaseModel):
    date: Optional[datetime] = None
    user: Optional[str] = None
    src_chain: Optional[str] = None
    dst_chain: Optional[str] = None
    token_symbol: Optional[str] = None
    tx_count: Optional[int] = None
    avg_token_price: Optional[float] = None
    value_usd: Optional[float] = None
    relay_fee_in_usd: Optional[float] = None
    lp_fee_in_usd: Optional[float] = None
