{% docs __overview__ %}

# Everclear Data Warehouse

**DOCS UPDATE DATE: 8th October 2024 | DOC VERSION: 0.0.1 | In Progress(Doc might not be up to date with the latest code/models)**

Welcome to the Connext Analytics DBT Documentation. This landing page provides an overview of the project, its models, and useful resources.

## Project Overview

The Connext DBT project is designed to transform and model data related to blockchain bridges, liquidity metrics, and user interactions. The project leverages **dbt** to ensure data integrity, scalability, and maintainability.

## Models

### Metrics Models- TBD

Metrics models compute key performance indicators and aggregate data for analysis.

### Staging Models

Staging models handle the initial extraction and cleansing of raw data from various sources.

#### Bridges

- **Bridges Staging Models**
  - [`stg_all_bridge_txs`](models/bridges/stage/stg_all_bridge_txs.sql): Extracts and deduplicates bridge transaction data.
  - [`stg_symbiosis_txs`](models/bridges/stage/stg_symbiosis_txs.sql): Processes Symbiosis bridge transactions.
  - [`stg_connext_txs`](models/bridges/stage/stg_connext_txs.sql): Processes Connext bridge transactions.
  - [`stg_hop_txs`](models/bridges/stage/stg_hop_txs.sql): Processes Hop bridge transactions.
  - [`stg_debridge_txs`](models/bridges/stage/stg_debridge_txs.sql): Processes Debridge bridge transactions.
  - [`stg_synapse_txs`](models/bridges/stage/stg_synapse_txs.sql): Processes Synapse bridge transactions.
  - [`stg_all_bridge_txs`](models/bridges/stage/stg_all_bridge_txs.sql): Processes all bridge transactions

#### Bridges Mart Models

Mart models are optimized for reporting and analytics, combining data from multiple sources.

- [`all_combined_bridges_txs`](models/bridges/mart/all_combined_bridges_txs.sql): Merges transactions from multiple bridge sources.
- [`all_combined_bridges_txs_modelling_format`](models/bridges/mart/all_combined_bridges_txs_modelling_format.sql): Formats combined transaction data for analysis.

## Resources

- **Project Repository:** [GitHub](https://github.com/connext/connext-analytics)
- **DBT Documentation:** [dbt Docs](https://github.com/connext/connext-analytics/tree/main/connext_dbt)

### commands to run

- to get stats for all bridges txs:

```bash
pipenv run dbt run-operation print_profile_docs --args '{"relation_name": "all_combined_bridges_txs", "schema": "bridges"}'
```

{% enddocs %}
