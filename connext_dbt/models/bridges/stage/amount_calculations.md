{% docs amount_calculations %}

# Amount Calculations for Bridges

This document details the methods used to calculate token amounts transferred across various bridge scenarios. The general rule is to have a `from_amount` and adjust it based on different factors, such as relay fees and gas amounts.

- Most bridge, relay amount has slippage embeddedin to them as there is no way to know it based on the raw data available.
---

### Across v3 + v2

  **Final Amounts Anchor Formulas:**
    
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

**Final Amounts Anchor Formulas:**

  - to_amount = As per the user in the destination side
  - from_amount = As per the bridge on the source side
  - relay_amount = As per the bridge
    - if we do from_amount - to_amount, we get a larger amount, which could be potential slippage!
  - gas_amount is kept seperate, as it is not part of amounts sent and paid by user seperately!
  - Additionaly, there is an interim_amount, which is used to store the amount before the final calculation, this is used to store the amount
    that is actual and post swap amount used by the bridge to relay the transfer.
    
    - eg: BERT -> ETH -> ETH
    - https://app.debridge.finance/order?orderId=0x5e45dd0b95e7eb004061b4673e9e33004f60969b83173748ec17a09a5eacdb87

---

### Hop

**Summary**:  
For Hop, the `from_amount` is initially the same as `to_amount`, and we adjust by adding the `relay_fee_amount`. Gas fees are paid separately.

**Final Amounts Anchor Formulas:**

  - to_amount = As per the user in the destination side
  - from_amount = As per the bridge on the source side
  - relay_amount = from_amount - to_amount.
    - As per the bridge- its bonder fee on the destination side. Gas is not included in this.          
  - gas_amount is kept seperate, as it is not part of amounts sent and paid by user seperately!


---

### Synapse

**Final Amounts Anchor Formulas:**
  
  eg: https://explorer.synapseprotocol.com/tx/8f1143064979a1b86b93f1360aa470bfa1c96f5888ca88fd753988a3cf234f2f?chainIdFrom=1&chainIdTo=137

  - to_amount = As per the user in the destination side
  - from_amount = As per the bridge on the source side
  - relay_amount = Not avialable in raw data.  calculated as the difference between the from and to amounts
    - relay_amount = from_amount - to_amount.
    - There are instances where the relay_amount is negative, which means positive slippage.
    - Slippage is emmbeded into the relay amount. Current no way to know relay from the data
  - gas_amount: Not Aviable. Looking at the txs, it is excluded from the amount that user sends

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

**Final Amounts Anchor Formulas:**

  - to_amount = As per the user in the destination side
  - from_amount = As per the bridge on the source side
  - relay_amount = Not avialable in raw data.  calculated as the difference between the from and to amounts
    - relay_amount = from_amount - to_amount.
    - There are instances where the relay_amount is negative, which means positive slippage.
      - eg: https://explorer.symbiosis.finance/transactions/1329/0xf984bd1213ba85dbb7b2521d3b96ca16dfdbe97d04e9b04f39b1a218e7e6cebf
  - gas_amount: Not Aviable. Looking at the txs, it is excluded from the amount that user sends

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

{% enddocs %}
