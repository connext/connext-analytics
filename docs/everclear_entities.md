### Summary of Each Component

#### **Architecture**

Everclear employs a Spoke and Hub model for handling intents and settlements across supported domains. Here's a breakdown of the process:

- **Spoke and Hub Contracts**: The core contracts that trigger and manage functions for intents and settlements.
- **SpokeGateway and HubGateway**: Gateways that facilitate the transport of messages between domains.
- **Hyperlane**: The transport layer used for communication between the Spoke and Hub.

**Entities Involved**:
- **Rebalancers**: Use the system to balance funds across domains.
- **Arbitrageurs**: Monitor the system to purchase discounted intents.

**Flow**:
1. Rebalancer creates an intent.
2. Arbitrageur picks up the intent.
3. Funds are moved from their wallet to the Spoke contract.
4. Intents are queued and sent to the Clearing chain.
5. On the Hub, intents are matched or converted into invoices/deposits.
6. Matched intents become settlements in the settlement queue.
7. Settlements are processed and sent back to the Spoke domains.

### **Flow of Funds**

1. **Intent Creation**: Rebalancers create intents, pulling funds from their wallets.
2. **Spoke Contract**: Holds the funds and increases the unclaimed balance.
3. **Intent Queue**: Intents are transported to the Clearing chain.
4. **Hub Domain**: Processes intents into invoices or deposits.
5. **Settlement Queue**: Processes invoices/deposits into settlements.
6. **Settlement Strategies**: Netting and xERC20 strategies used for settlement.
7. **Spoke Domains**: Final settlements are sent back, and users receive tokens or balance updates.

### **Components**

#### **Supported Domains**

- **Spoke**: Holds funds and formats messages for dispatch via the SpokeGateway.
- **SpokeGateway**: Dispatches and formats settlement messages.

#### **Clearing Layer (Hub)**

- **Hub**: Handles inbound intents and dispatches settlements.
- **HubGateway**: Dispatches messages and formats inbound payloads.

#### **Agents**

- **Lighthouse**: Manages queues and executes cron jobs based on configured thresholds.
- **Cartographer**: Creates a cross-chain view of the network state using indexing layers (subgraphs).

#### **Transport Layer (Hyperlane)**

- **Hyperlane**: Manages the transport of messages across the network.

### Entity Diagram


```mermaid
flowchart TD
    A[Rebalancer/Arbitrageur] -> B[Intent Creation]
    B -> C[Spoke Contract]
    C -> D[Intent Queue]
    D -> E[Clearing Chain (Hub)]
    E -> F{Intent Processing}
    F ->|Matched| G[Deposit Queue]
    F ->|Unmatched| H[Invoice Queue]
    G -> I[Settlement Queue]
    H -> I[Settlement Queue]
    I -> J[Settlement Strategies]
    J ->|Netting| K[Spoke Domains]
    J ->|xERC20| K[Spoke Domains]
    K -> L[User Receives Tokens/Balance]
    C -> M[SpokeGateway]
    M -> N[Hyperlane]
    N -> O[HubGateway]
    O -> E
    E -> P[HubGateway]
    P -> Q[Hyperlane]
    Q -> R[SpokeGateway]
    R -> S[Spoke Contract]
    
    subgraph Agents
        T[Lighthouse] -- Manages -> D
        U[Cartographer] -- Indexes -> Q
    end
```

### Explanation of Each Step in the Diagram

- **Rebalancer** (A): Users who create intents.
    - **Intent Creation**: The process where Rebalancers create intents.
- **Arbitrageur** (B): Users who pick up intents.
- **Spoke Contract** (C): Holds the funds and manages intents.
- **Intent Queue** (D): Collects intents to be sent to the Hub for processing.
- **Clearing Chain (Hub)** (E): Central hub for processing intents.
- **Intent Processing** (F): Determines whether intents are matched or unmatched.
  - **Matched Intents** go to the Deposit Queue (G).
  - **Unmatched Intents** go to the Invoice Queue (H).
- **Settlement Queue** (I): Collects settlements from both Deposit and Invoice Queues.
- **Settlement Strategies** (J): Different strategies for settling intents.
  - **Netting Strategy** (K): Settles netted intents.
  - **xERC20 Strategy** (K): Settles xERC20 tokens.
- **User Receives Tokens/Balance** (L): Final step where users receive their settlements.
- **SpokeGateway** (M): Facilitates message dispatch from the Spoke to the transport layer.
- **Hyperlane** (N): The transport layer for messages between the Hub and Spoke.
- **HubGateway** (O): Manages message formatting and dispatch to the Hub.
- **HubGateway** (P): Processes outbound messages to Hyperlane.
- **SpokeGateway** (R): Receives messages from Hyperlane and processes them at the Spoke.
- **Spoke Contract** (S): Final destination where intents are settled.

#### **Agents**
- **Lighthouse** (T): Manages the queues and ensures timely processing.
- **Cartographer** (U): Provides a cross-chain view of the network state using indexing layers.


### Flow of funds

```mermaid
flowchart TD
    A[Rebalancer/Arbitrageur] --> B[Intent Creation]
    B --> C[Spoke Contract]
    C --> D[Intent Queue]
    D --> E[Clearing Chain (Hub)]
    E --> F{Intent Processing}
    F -->|Matched| G[Deposit Queue]
    F -->|Unmatched| H[Invoice Queue]
    G --> I[Settlement Queue]
    H --> I[Settlement Queue]
    I --> J[Settlement Strategies]
    J -->|Netting| K[Spoke Domains]
    J -->|xERC20| K[Spoke Domains]
    K --> L[User Receives Tokens/Balance]
    C --> M[SpokeGateway]
    M --> N[Hyperlane]
    N --> O[HubGateway]
    O --> E
    E --> P[HubGateway]
    P --> Q[Hyperlane]
    Q --> R[SpokeGateway]
    R --> S[Spoke Contract]
    
    subgraph Agents
        T[Lighthouse] -- Manages --> D
        U[Cartographer] -- Indexes --> Q
    end
```

### Summary of Each Entity and Process

#### Entities

- **Rebalancers**: Users who create intents to balance funds across domains.
- **Arbitrageurs**: Users who pick up intents to purchase discounted intents.
- **Spoke Contract**: Holds the funds and manages intents.
- **SpokeGateway**: Facilitates message dispatch from the Spoke to the transport layer.
- **Hyperlane**: The transport layer for messages between the Hub and Spoke.
- **HubGateway**: Manages message formatting and dispatch to the Hub.
- **Clearing Chain (Hub)**: Central hub for processing intents.
- **Settlement Queue**: Collects settlements from both Deposit and Invoice Queues.
- **Settlement Strategies**: Different strategies for settling intents (Netting and xERC20).
- **Lighthouse**: Manages the queues and ensures timely processing.
- **Cartographer**: Provides a cross-chain view of the network state using indexing layers.

#### Processes

1. **Intent Creation**: Rebalancers or Arbitrageurs create intents, pulling funds from their wallets.
2. **Spoke Contract**: Holds the funds and increases the unclaimed balance.
3. **Intent Queue**: Intents are transported to the Clearing chain.
4. **Hub Domain**: Processes intents into invoices or deposits.
5. **Settlement Queue**: Processes invoices/deposits into settlements.
6. **Settlement Strategies**: Netting and xERC20 strategies used for settlement.
7. **Spoke Domains**: Final settlements are sent back, and users receive tokens or balance updates.


### Table and Schema

### Metrics and calculations

```sql

    -- Top 100 rows from each table
    
    -- assets:
        -- Takeaway: 
        -- 1. there are 2 assets with addresses
    SELECT 'assets' AS table_name, * FROM public.assets LIMIT 100

    -- Balances: Nothing in balance
    SELECT 'balances' AS table_name, * FROM public.balances LIMIT 100

    -- checkpoints: There are check_names: origin/hub_invoice___chain_ids and other cols is check_point: not sure what that is
    SELECT 'checkpoints' AS table_name, * FROM public.checkpoints LIMIT 100

    -- no data on depositors
    SELECT 'depositors' AS table_name, * FROM public.depositors LIMIT 100

    -- no data on destination_intents
    SELECT 'destination_intents' AS table_name, * FROM public.destination_intents LIMIT 100

    -- data by id -> domain | message_id | etc 
    SELECT 'hub_intents' AS table_name, * FROM public.hub_intents LIMIT 100

    SELECT 'messages' AS table_name, * FROM public.messages LIMIT 100

    -- origin_intents, queues, tokens
    SELECT 'origin_intents' AS table_name, * FROM public.origin_intents LIMIT 100

    SELECT 'queues' AS table_name, * FROM public.queues LIMIT 100

    SELECT 'tokens' AS table_name, * FROM public.tokens LIMIT 100

```


- **Settlement_Rate_3h**
  - Category: SLA for Market Makers
  - Description: Percentage of transactions settled within 3 hours
  - Target: Purchase 100% of invoices in 3+ hours
  - Property: by chains; by assets
  - Tables to use:
    - `public.messages` OR  Combination of `public.origin_intents` and `public.hub_intents`
        - type: `SETTLEMENT`
        - there is a timestamp for each id. there is queue numbers: 1- 15 etc (`cols: Fisrt | last`)
  - Calculation
    - for a given timeframe, take a ratio of tx where type: `SETTLEMENT` / count(tx)
  - Question
    - Is the state change of type gets records in the same table or in log table
  - SQL:
    ```sql
    SELECT 'messages' AS table_name, * FROM public.messages LIMIT 100
    ```

- **Invoices_1h_Retention_Rate**
  - Category: SLA for Market Makers
  - Description: Percentage of invoices that remain in the system for ~1h
  - Target: >60% of invoices remain ~1h
  - Property: by chains; by assets
  - Tables to use
    - `public.hub_invoices`
    - `public.intents`
  - Calculation
    - for a given invoice calculate the time from invoice to settlement, take a ratio of tx with invoice to settlement time < 1h / count(tx)
  - Question
    - Is the state change of type gets records in the same table or in log table

- **Epoch_Discounts**
  - Category: SLA for Market Makers
  - Description: Number of epoch discounts applied to the invoice before settlement
  - Target: for >60% of the invoices remained ~1h, receiving a discount of <2 epochs
  - Property: by chains; by assets
  - Tables to use
    - `public.hub_invoices` and `public.settlements_intents` and `public.messages`: also to check on the state change of types.
- Calculation
  - for a given invoice calculate the number of epochs applied before settlement(entry_epoch - exit_epoch)

- **Trading_Volume**
  - Category: SLA for Market Makers
  - Description: Daily trading volume for MMs
  - Target: Daily trading activity above 6x committed sum
  - Property: by chains; by assets; by MMs
  - Tables to use
    - `public.intents`
  - Calculation
    - for a given invoice calculate the number of epochs applied before settlement(entry_epoch - exit_epoch)

- **Discount_value**
  - Category: SLA for Market Makers
  - Description: The average discount applied to invoices
  - Target: KR3: Efficiency gain for users on par with Across (TBD)
  - Property: by chains; by assets; by MMs

- **APY**
  - Category: SLA for Market Makers
  - Description: Annual Percentage Yield
  - Target: Average APY
  - Property: by chains; by assets; by MMs

- **KR1_Clearing_Volume**
  - Category: OKRs
  - Description: Clearing volume (settlement + netted)
  - Target: KR1: 1B clearing volume (run-rate for Day-30)
  - Property: by chains; by assets

- **KR2_Netting_Rate**
  - Category: OKRs
  - Description: Percentage of transactions netted within 24 hours
  - Target: KR2: 60% netted within 24h
  - Property: by chains; by assets

- **KR3_Total_rebalancing_fee**
  - Category: OKRs
  - Description: Total fee = Protocol fee + Discount
  - Target: KR3: Efficiency gain on par with Across (TBD)
  - Property: by chains; by assets

- **Settlement_Rate_6h**
  - Category: OKRs
  - Description: Percentage of transactions settled within 6 hours
  - Target: KR4: 97% settled within 6 hours
  - Property: by chains; by assets

- **Settlement_Rate_24h**
  - Category: OKRs
  - Description: Percentage of transactions settled within 24 hours
  - Target: KR4: 100% settled within 24h
  - Property: by chains; by assets

- **Total_Protocol_Revenue**
  - Category: Product Metrics
  - Description: Total revenue generated by the protocol
  - Target: Total protocol revenue
  - Property: by chains; by assets

- **Settlement_Time**
  - Category: Product Metrics
  - Description: Average time taken to settle the intent
  - Target: Average settlement time (weekly)
  - Property: by chains; by assets

- **Wallet_retention_rate**
  - Category: Product Metrics
  - Description: Measures the frequency and consistency of user activity associated with specific wallet addresses over time, indicating user retention and engagement levels
  - Target: Retention
  - Property: by chains; by assets

- **Average_intent_size**
  - Category: Product Metrics
  - Description: Average check
  - Property: by chains; by assets

- **Amount_of_intents**
  - Category: Product Metrics
  - Description: Number of intents
  - Property: by chains; by assets



### Types per table column.
Adding a new log for each alter trigger in postgres

- **public.intent_status**
  - `NONE`
  - `ADDED`
  - `DEPOSIT_PROCESSED`
  - `FILLED`
  - `ADDED_AND_FILLED`
  - `INVOICED`
  - `SETTLED`
  - `SETTLED_AND_MANUALLY_EXECUTED`
  - `UNSUPPORTED`
  - `UNSUPPORTED_RETURNED`
  - `DISPATCHED`
  - `DISPATCHED_UNSUPPORTED`

- **public.message_type**
  - `INTENT`
  - `FILL`
  - `SETTLEMENT`
  - `MAILBOX_UPDATE`
  - `SECURITY_MODULE_UPDATE`
  - `GATEWAY_UPDATE`
  - `LIGHTHOUSE_UPDATE`

- **public.queue_type**
  - `INTENT`
  - `FILL`
  - `SETTLEMENT`
  - `DEPOSIT`