from pydantic import BaseModel
from typing import Optional
from datetime import datetime


class Token(BaseModel):
    symbol: Optional[str] = None
    name: Optional[str] = None
    address: Optional[str] = None
    decimals: Optional[int] = None


class Route(BaseModel):
    chain_id: Optional[int] = None
    amount: Optional[int] = None
    token: Optional[Token] = None


class SymbiosisBridgeExplorerTransaction(BaseModel):
    id: Optional[int] = None
    from_client_id: Optional[str] = None
    from_chain_id: Optional[int] = None
    from_tx_hash: Optional[str] = None
    join_chain_id: Optional[int] = None
    join_tx_hash: Optional[str] = None
    to_chain_id: Optional[int] = None
    to_tx_hash: Optional[str] = None
    event_type: Optional[int] = None
    type: Optional[int] = None
    hash: Optional[str] = None
    state: Optional[int] = None
    created_at: Optional[str] = None
    mined_at: Optional[str] = None
    success_at: Optional[str] = None
    from_address: Optional[str] = None
    from_sender: Optional[str] = None
    duration: Optional[int] = None
    to_address: Optional[str] = None
    to_sender: Optional[str] = None
    amounts: Optional[str] = None  # JSON string of list
    tokens: Optional[str] = None  # JSON string of list
    token_symbol: Optional[str] = None
    token_name: Optional[str] = None
    token_address: Optional[str] = None
    token_decimals: Optional[int] = None
    from_route: Optional[str] = None  # JSON string of list
    to_route: Optional[str] = None  # JSON string of list
    transit_token: Optional[str] = None
    from_amount_usd: Optional[float] = None
    to_amount_usd: Optional[float] = None
    to_tx_id: Optional[int] = None
    retry_active: Optional[bool] = None
