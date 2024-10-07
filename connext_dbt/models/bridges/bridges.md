# Bridges Models Documentation

## Overview

The Bridges section of the Connext DBT project focuses on transforming and modeling data related to various blockchain bridges. This includes staging raw transaction data, cleaning and formatting it, and creating mart models optimized for reporting and analytics.

## Models

### `all_combined_bridges_txs`

- **ID Column:**
  - `id`

- **Tests:**
  - `not_null`
  - `unique`

- **Overview:**
  Combines data from multiple bridge transaction sources (`cln_hop_txs`, `cln_debridge_txs`, `cln_symbiosis_txs`, `cln_synapse_txs`, `cln_all_bridge_txs`) to create a unified view of all bridge transactions. This model ensures that all records from different bridges are included using the `UNION ALL` operator.

- **Transformations:**
  Aggregates and unifies transaction data from various source tables by performing a `UNION ALL`. This ensures a comprehensive dataset that includes all bridge transactions across different protocols.

### `all_combined_bridges_txs_modelling_format`

- **ID Column:**
  - `id`

- **Tests:**
  - None specified.

- **Overview:**
  Formats the combined bridge transaction data for analysis. It includes fields such as timestamps, amounts, fees, user addresses, and chain information, ensuring that the data is ready for downstream analytics.

- **Transformations:**
  Maps and renames columns to standardize field names across different bridge sources. Converts data types and structures timestamps to ensure consistency, facilitating easier analysis and reporting.

### `cln_debridge_txs`

- **ID Column:**
  - `id`

- **Tests:**
  - `not_null`
  - `unique`

- **Overview:**
  Cleans and transforms data from the `stg_debridge_txs` table by renaming columns and converting data types. This preparation ensures data consistency and readiness for further analysis.

- **Transformations:**
  Renames columns to maintain uniform naming conventions and casts data types to appropriate formats. Filters and cleans records to remove any inconsistencies or irrelevant data points.

### `cln_hop_txs`

- **ID Column:**
  - `id`

- **Tests:**
  - `not_null`
  - `unique`

- **Overview:**
  Cleans and transforms data from the `stg_hop_txs` table. It renames columns and converts data types to ensure the data is correctly formatted for analysis.

- **Transformations:**
  Standardizes column names and data types. Applies necessary formatting to prepare the transaction data for seamless integration with other bridge data sources.

### `cln_symbiosis_txs`

- **ID Column:**
  - `id`

- **Tests:**
  - `not_null`
  - `unique`

- **Overview:**
  Extracts and cleans transfer records from the `source_symbiosis_bridge_explorer_transactions` table for the staging layer. It converts timestamps and filters records to ensure data relevancy and accuracy.

- **Transformations:**
  Converts raw timestamps to standardized formats and filters transactions based on specific criteria to ensure only relevant and accurate data is retained for analysis.

### `cln_synapse_txs`

- **ID Column:**
  - `id`

- **Tests:**
  - `not_null`
  - `unique`

- **Overview:**
  Cleans and transforms data from the `stg_synapse_txs` table by renaming columns and converting data types. Prepares the data for further analysis by ensuring all necessary fields are correctly formatted.

- **Transformations:**
  Standardizes column naming and data types. Ensures consistency across Synapse transactions, facilitating their integration with other bridge transaction data.

## Mart Models Lineage

### `all_combined_bridges_txs`

- **Source Models:**
  - `cln_hop_txs`
  - `cln_debridge_txs`
  - `cln_symbiosis_txs`
  - `cln_synapse_txs`
  - `cln_all_bridge_txs`

- **Description:**
  Aggregates transaction data from the above source models to provide a comprehensive view of all bridge transactions within the data warehouse.

### `all_combined_bridges_txs_modelling_format`

- **Source Model:**
  - `all_combined_bridges_txs`

- **Description:**
  Formats and structures the aggregated bridge transactions for analytical purposes, adding necessary fields and ensuring data integrity for reporting.

## Summary

The Bridges models within the Connext DBT project are crucial for consolidating and preparing bridge transaction data from various sources. By staging, cleaning, and aggregating this data, the project ensures that stakeholders have access to reliable and comprehensive analytics on blockchain bridge activities.
