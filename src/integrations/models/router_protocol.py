from pydantic import BaseModel
from typing import List, Optional


class TransactionData(BaseModel):
    src_timestamp: Optional[int]
    dest_timestamp: Optional[int]
    src_chain_id: Optional[str]
    dest_chain_id: Optional[str]
    src_tx_hash: Optional[str]
    dest_tx_hash: Optional[str]
    status: Optional[str]
    src_address: Optional[str]
    dest_address: Optional[str]
    src_amount: Optional[str]
    dest_amount: Optional[str]
    dest_stable_amount: Optional[str]
    src_symbol: Optional[str]
    dest_symbol: Optional[str]
    dest_stable_symbol: Optional[str]
    has_message: Optional[bool]
    native_token_amount: Optional[str]


class TransactionsResponse(BaseModel):
    limit: Optional[int]
    page: Optional[int]
    total: Optional[int]
    data: List[TransactionData]


class Data(BaseModel):
    findNitroTransactionsByFilter: TransactionsResponse


class GraphQLResponseRouterProtocol(BaseModel):
    data: Data


def model_testing():
    response_data = {
        "data": {
            "findNitroTransactionsByFilter": {
                "limit": 30,
                "page": 1,
                "total": 773808,
                "data": [
                    {
                        "src_timestamp": 1702024047,
                        "dest_timestamp": 1702024172,
                        "src_chain_id": "137",
                        "dest_chain_id": "42161",
                        "src_tx_hash": "0xbf3bf2c87c6b54a4e3bc079245fc58918dc7075014b4480a438f298b346b8914",
                        "dest_tx_hash": "0x1b4be4d0d744f33a6209a57b021a68f7b328f9eb8565284d9ec93d2e9f43a2db",
                        "status": "completed",
                        "src_address": "0xC168E40227E4ebD8C1caE80F7a55a4F0e6D66C97",
                        "dest_address": "0x13538f1450Ca2E1882Df650F87Eb996fF4Ffec34",
                        "src_amount": "2.22",
                        "dest_amount": "2.21778",
                        "dest_stable_amount": "2.21778",
                        "src_symbol": "DFYN",
                        "dest_symbol": "DFYN",
                        "dest_stable_symbol": "DFYN",
                        "has_message": False,
                        "native_token_amount": "",
                    }
                ],
            }
        }
    }

    try:
        response = GraphQLResponseRouterProtocol(**response_data)
        print(response)
    except Exception as e:
        print(e)
