from datetime import datetime
from typing import Optional

from pydantic import BaseModel, Field


class AllBridgeExplorerTransfer(BaseModel):
    id: Optional[str] = None
    status: Optional[str] = None
    timestamp: Optional[int] = None
    from_chain_symbol: Optional[str] = None
    to_chain_symbol: Optional[str] = None
    from_amount: Optional[str] = None
    to_amount: Optional[str] = None
    stable_fee: Optional[str] = None
    from_token_address: Optional[str] = None
    to_token_address: Optional[str] = None
    from_address: Optional[str] = None
    to_address: Optional[str] = None
    messaging_type: Optional[str] = None
    partner_id: Optional[int] = None
    from_gas: Optional[str] = None
    to_gas: Optional[str] = None
    relayer_fee_in_native: Optional[str] = None
    relayer_fee_in_tokens: Optional[str] = None
    send_transaction_hash: Optional[str] = None
    receive_transaction_hash: Optional[str] = None
    api_url: Optional[str] = None


class PoolInfo(BaseModel):
    pool_info_a_value: Optional[str] = Field(None, alias="poolInfoaValue")
    pool_info_d_value: Optional[str] = Field(None, alias="poolInfodValue")
    pool_info_token_balance: Optional[str] = Field(None, alias="poolInfotokenBalance")
    pool_info_v_usd_balance: Optional[str] = Field(None, alias="poolInfovUsdBalance")
    pool_info_total_lp_amount: Optional[str] = Field(
        None, alias="poolInfototalLpAmount"
    )
    pool_info_acc_reward_per_share_p: Optional[str] = Field(
        None, alias="poolInfoaccRewardPerShareP"
    )
    pool_info_p: Optional[int] = Field(None, alias="poolInfop")


class AllBridgeExplorerTokenInfo(BaseModel):
    # remove alias

    blockchain: str
    name: str
    pool_address: Optional[str] = None
    token_address: Optional[str] = None
    decimals: Optional[int] = None
    symbol: Optional[str] = None
    fee_share: Optional[str] = None
    apr: Optional[str] = None
    apr7d: Optional[str] = None
    apr30d: Optional[str] = None
    lp_rate: Optional[str] = None
    cctp_address: Optional[str] = None
    cctp_fee_share: Optional[str] = None
    pool_info_a_value: Optional[str] = None
    pool_info_d_value: Optional[str] = None
    pool_info_token_balance: Optional[str] = None
    pool_info_v_usd_balance: Optional[str] = None
    pool_info_total_lp_amount: Optional[str] = None
    pool_info_acc_reward_per_share_p: Optional[str] = None
    pool_info_p: Optional[int] = None
    api_url: Optional[str] = None
    updated_at: Optional[datetime] = None
