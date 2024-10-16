from datetime import datetime
from typing import Optional

from pydantic import BaseModel


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


class HourlyTokenPricingBlockchainEth(BaseModel):
    symbol: Optional[str] = None
    date: Optional[str] = None
    average_price: Optional[float] = None
    max_price: Optional[float] = None


class CannonicalBridgesFlowsTokensHourly(BaseModel):
    """
    sample data:
        {
            "date": "2024-01-19 12:00:00.000 UTC",
            "fs_bridge": null,
            "ts_bridge": "Arbitrum Bridge",
            "symbol": "wstETH",
            "usd_token_value": "206569.16322411445",
            "gas_used": "3.42112e-13",
            "fee_usd": "20.096766404637268",
            "tx_count": "2"
        }
    """

    date: Optional[str] = None
    fs_bridge: Optional[str] = None
    ts_bridge: Optional[str] = None
    symbol: Optional[str] = None
    usd_token_value: Optional[float] = None
    gas_used: Optional[float] = None
    fee_usd: Optional[float] = None
    tx_count: Optional[int] = None


class CannonicalBridgesFlowsNativeHourly(BaseModel):
    """
    sample data:
        {
            "date": "2024-01-19 12:00:00.000 UTC",
            "fs_bridge": null,
            "ts_bridge": "Arbitrum Bridge",
            "symbol": "wstETH",
            "usd_token_value": "206569.16322411445",
            "gas_used": "3.42112e-13",
            "fee_usd": "20.096766404637268",
            "tx_count": "2"
        }
    """

    date: Optional[str] = None
    fs_bridge: Optional[str] = None
    ts_bridge: Optional[str] = None
    symbol: Optional[str] = None
    usd_token_value: Optional[float] = None
    gas_used: Optional[float] = None
    fee_usd: Optional[float] = None
    tx_count: Optional[int] = None


class ArbWethDepositTransaction(BaseModel):
    hash: Optional[str] = None
    user_address: Optional[str] = None
    date: Optional[str] = None
    origin_chain: Optional[str] = None
    destination_chain: Optional[str] = None
    volume_eth: Optional[float] = None


class BridgesAggregateFlowsMonthly(BaseModel):
    """
    cols in the data:
    date
    currency_symbol
    source_chain_name
    destination_chain_name
    tx_count_10k_txs
    volume_10k_txs
    avg_volume_10k_txs
    avg_volume
    total_txs
    total_volume
    """

    bridge: Optional[str] = None
    date: Optional[str] = None
    currency_symbol: Optional[str] = None
    source_chain_name: Optional[str] = None
    destination_chain_name: Optional[str] = None
    tx_count_10k_txs: Optional[int] = None
    volume_10k_txs: Optional[float] = None
    avg_volume_10k_txs: Optional[float] = None
    avg_volume: Optional[float] = None
    total_txs: Optional[int] = None
    total_volume: Optional[float] = None


class BridgesAggregateFlowsDaily(BridgesAggregateFlowsMonthly):
    date: Optional[str] = None
