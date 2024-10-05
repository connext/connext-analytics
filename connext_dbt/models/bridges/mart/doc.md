{% docs all_combined_bridges_txs %}
### All Combined Bridges Transactions

This model consolidates bridge transactions from multiple sources, providing a unified view of cross-chain activities. It includes details such as transaction IDs, user addresses, token information, amounts transferred, and associated fees.

#### Columns

- **from_amount**: The original amount transferred from the source chain.
- **from_amount_usd**: The USD equivalent of the original transferred amount.
- **to_amount**: The amount received on the destination chain.
- **to_amount_usd**: The USD equivalent of the received amount on the destination chain.
- **relay_amount**: The amount relayed during the bridge transaction.
- **relay_amount_usd**: The USD value of the relayed amount.
- **gas_amount**: The gas fee incurred for the transaction.
- **gas_amount_usd**: The USD equivalent of the gas fee.
{% enddocs %}
