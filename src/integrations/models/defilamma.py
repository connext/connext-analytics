from datetime import datetime
from typing import Dict, List, Optional
from pydantic import BaseModel, HttpUrl


class DefilammaChains(BaseModel):
    gecko_id: str
    tvl: float
    tokenSymbol: str
    cmcId: str
    name: str
    chainId: int


class DefilammaBridges(BaseModel):
    id: int
    name: str
    displayName: Optional[str]
    icon: Optional[str]
    volumePrevDay: Optional[float]
    volumePrev2Day: Optional[float]
    lastHourlyVolume: Optional[float]
    currentDayVolume: Optional[float]
    lastDailyVolume: Optional[float]
    dayBeforeLastVolume: Optional[float]
    weeklyVolume: Optional[float]
    monthlyVolume: Optional[float]
    chains: Optional[List[str]]
    destinationChain: Optional[str]
    upload_timestamp: Optional[datetime]


class DefilammaBridgesHistoryWallets(BaseModel):
    date: Optional[int]
    bridge_id: Optional[int]
    key: Optional[str]
    usdValue: Optional[float]
    txs: Optional[int]
    chain_id: Optional[str]
    wallet_address: Optional[str]
    upload_timestamp: Optional[datetime]


class DefilammaBridgesHistoryTokens(BaseModel):
    date: Optional[int]
    bridge_id: Optional[int]
    key: Optional[str]
    usdValue: Optional[float]
    amount: Optional[str]
    symbol: Optional[str]
    decimals: Optional[int]
    chain_id: Optional[str]
    token_address: Optional[str]
    upload_timestamp: Optional[datetime]


# DEFILAMMA
class DefilammaStables(BaseModel):
    id: int
    name: str
    symbol: str
    gecko_id: str
    pegType: str
    pegMechanism: str
    circulating: Optional[float]
    circulatingPrevDay: Optional[float]
    circulatingPrevWeek: Optional[float]
    circulatingPrevMonth: Optional[float]
    price: Optional[float]
    delisted: Optional[str]
    chains: Optional[List[str]]
    upload_timestamp: Optional[datetime]


class DefilammaProtocols(BaseModel):
    id: str
    name: Optional[str]
    address: Optional[str]
    symbol: Optional[str]
    url: Optional[HttpUrl]
    description: Optional[str]
    chain: Optional[str]
    logo: Optional[HttpUrl]
    audits: Optional[str]
    audit_note: Optional[str]
    gecko_id: Optional[str]
    cmcId: Optional[str]
    category: Optional[str]
    chains: Optional[List[str]]
    module: Optional[str]
    twitter: Optional[str]
    forkedFrom: Optional[List[str]]
    oracles: Optional[List[str]]
    listedAt: Optional[int]
    slug: Optional[str]
    tvl: Optional[float]
    chainTvls: Optional[Dict]
    change_1h: Optional[float]
    change_1d: Optional[float]
    change_7d: Optional[float]
    tokenBreakdowns: Optional[Dict]
    mcap: Optional[float]
    referralUrl: Optional[HttpUrl]
    treasury: Optional[str]
    audit_links: Optional[List[HttpUrl]]
    openSource: Optional[str]
    governanceID: Optional[List[str]]
    github: Optional[List[str]]
    stablecoins: Optional[List[str]]
    parentProtocol: Optional[str]
    wrongLiquidity: Optional[str]
    staking: Optional[str]
    pool2: Optional[str]
    assetToken: Optional[str]
    language: Optional[str]
    oraclesByChain: Optional[List[str]]
    deadUrl: Optional[bool]
    rugged: Optional[bool]
    upload_timestamp: Optional[datetime]
