# Amount Calculations for Bridges

This document details the methods used to calculate token amounts transferred across various bridge scenarios. The general rule is to have a `from_amount` and adjust it based on different factors, such as relay fees and gas amounts.


### 1. Anchor Formula to `to_amount` on the Destination Chain

**Summary**:  
We calculate the final token amount on the destination chain (`to_amount`) and make adjustments to account for relay fees. The gas fees are handled separately in another token.

**Formula**:  
`to_amount = from_amount - relay_fee_amount`

**Changes**:

- `from_amount` is adjusted by adding the `relay_fee_amount` to the original value.
- Gas fees are excluded as they are paid separately.

---

### Across v3 + v2
  **Calclation and modification:**

    - **Final Amounts Anchor Formulas:**
    
      - to_amount = from_amount - relay_amount
      - gas_amount is kept seperate, as it is not part of amounts sent and paid by user seperately!

    - **V3:**
      - from_amount -> In across v3 raw data represent the sent amount, without any calculations from our side.
      - to_amount -> in across v3 raw data represent the received amount, without any calculations from our side.
      - gas_amount -> Not available in raw data.
      - relay_fee_amount -> Calculated as `from_amount - to_amount`. Relay fee token is same as from token.

    - **V2:**
      - from_amount -> In across v2 raw data represent the sent amount, without any calculations from our side.
      - to_amount -> Caculated as, `from_amount * (1 -relay_fee_amount - lp_fee_amount)`
      - gas_amount -> Not available in raw data.

---

### All Bridge Transactions

  **Calclation and modification:**

    - **Final Amounts Anchor Formulas:**
    
      - to_amount = As per the user in the destination side
      - from_amount = As per the bridge on the source side
      - relay_amount = As per the bridge
        - if we do from_amount - to_amount, we get a larger amount, which could be potential slippage!
      - gas_amount is kept seperate, as it is not part of amounts sent and paid by user 
      seperately!
    
    - No Modifications Done on Amounts

---

### DeBridge

**Summary**:  
In DeBridge, the raw data shows `from_amount` as equal to `to_amount`. We calculate the amount by adding `relay_fee_amount` to the `from_amount`. An additional fee exists but is not included in the equation.

**Formula**:  
Same as Across v2.

**Expected Formula**:  
Same as Across v2.

**Changes**:  
Same as v2, with the caveat of the extra fee being excluded from the calculation.

---

### Hop

**Summary**:  
For Hop, the `from_amount` is initially the same as `to_amount`, and we adjust by adding the `relay_fee_amount`. Gas fees are paid separately.

**Formula**:  
Same as Across v2.

**Expected Formula**:  
Same as Across v2.

**Changes**:  
Same as v2.

---

### Synapse

**Summary**:  
The `from_amount` equals the `to_amount` in the raw data, and adjustments are made by adding the `relay_fee_amount`. Gas is paid separately in another token.

**Formula**:  
Same as Across v2.

**Expected Formula**:  
Same as Across v2.

**Changes**:  
Same as v2.

**Sample Data**:

```json
{
  "to_hash": "0x317d31aa5b8faf2a55a603775c2ac7a2b8f4aef08fff7c75019ff49615d57ea7",
  "from_token_symbol": "nUSD",
  "to_token_symbol": "nUSD",
  "from_amount": "15.9466",
  "to_amount": "14.9779",
  "relayer_amount": null
}
```

---

### Symbiosis

**Summary**:  
The raw data for Symbiosis provides a `from_amount` that already includes the `relay_fee_amount`. Therefore, no further adjustment is needed here.

**Formula**:  
No changes required.


**Changes**:  
No additional adjustments required as the raw data already includes the necessary fees.

**Sample Data**:

```json
{
  "to_hash": "0xfb9b1a27e2e54f5fa6fe4ccb539e312edba6f5ef4abb0c90ee1f2a1cab1ea41a",
  "from_token_symbol": "USDC",
  "to_token_symbol": "USDC",
  "from_amount": "24999.80485",
  "to_amount": "24945.978945",
  "relay_amount": null
}
```
