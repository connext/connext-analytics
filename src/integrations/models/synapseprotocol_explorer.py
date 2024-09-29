from typing import Optional, Union

from pydantic import BaseModel, validator


class SingleSideInfo(BaseModel):
    chainID: Optional[int]
    destinationChainID: Optional[int]
    address: Optional[str]
    hash: Optional[str]  # txnHash
    value: Optional[str]
    formattedValue: Optional[Union[str, float]]  # Adjusted to accept float as well
    tokenAddress: Optional[str]
    tokenSymbol: Optional[str]
    time: Optional[int]
    eventType: Optional[Union[str, int]]  # Adjusted to accept int as well

    # Convert float and int to string for formattedValue and eventType
    @validator("formattedValue", "eventType", pre=True, always=True)
    def convert_to_string(cls, v):
        return str(v)


class TransactionInfo(BaseModel):
    fromInfo: SingleSideInfo
    toInfo: SingleSideInfo
    kappa: Optional[str]
    pending: Optional[bool]
    swapSuccess: Optional[bool]

    def to_flat_dict(self):
        # Manually prefix the keys of fromInfo and toInfo
        flat_dict = {f"from_{k}": v for k, v in self.fromInfo.model_dump().items()}
        flat_dict.update({f"to_{k}": v for k, v in self.toInfo.model_dump().items()})
        # Add the other fields
        flat_dict["kappa"] = self.kappa
        flat_dict["pending"] = self.pending
        flat_dict["swapSuccess"] = self.swapSuccess

        return flat_dict


class FlattenedTransactionInfo(BaseModel):
    from_chain_id: Optional[int]
    from_destination_chain_id: Optional[int]
    from_address: Optional[str]
    from_hash: Optional[str]
    from_value: Optional[str]
    from_formatted_value: Optional[str]
    from_token_address: Optional[str]
    from_token_symbol: Optional[str]
    from_time: Optional[int]
    from_event_type: Optional[str]
    to_chain_id: Optional[int]
    to_destination_chain_id: Optional[int]
    to_address: Optional[str]
    to_hash: Optional[str]
    to_value: Optional[str]
    to_formatted_value: Optional[str]
    to_token_address: Optional[str]
    to_token_symbol: Optional[str]
    to_time: Optional[int]
    to_event_type: Optional[str]
    kappa: Optional[str]
    pending: Optional[bool]
    swap_success: Optional[bool]
