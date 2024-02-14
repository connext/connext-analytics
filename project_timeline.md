# Daily Work Updates

## 2024/02/13

**Completed Task:**

1. Added chains to Routes.
   - Added Base, zkSync, Avalanche and zkEVM to the list of chains
   - Unable to add, Metis, Mantle- These are not part of LIFI Chains. Check the dataset: `mainnet-bigq.stage.source__lifi__all_chains`
   - Added a new endpoint: `source_lifi__bridges_exchanges` to the Pipeline: This can used to quicky add chains in near future aswell as Tokens.
     - In Future we should just skip on the python code to do the path generation and replace it will SQL. Clean + fast.
   - These new chains resulted in 1104 paths were passed as inputs to the LIFI API.
      - Result: Loaded 883 rows to Cloud Storage -> Big Query -> Table:lifiRoutes.
   - **Above Steps overall impact:**
      - Total working pathways increased from 850 to 1013(+5 on a 2nd test run I did- just to make sure, everything is working). table: `mainnet-bigq.raw.stg__inputs_connext_routes_working_pathways`
2. Hop protocol Bug fixed.
   - Issue
      - Out of blue, pileline was failing on 12th Feb. investigation revealled, In table: `mainnet-bigq.raw.source_socket__routes` there is an integer col: `estimatedrelativetimeuntilbond`. API started giving out text for the column, 
      - eg DATA: `in 24 minutes, in 29 minutes, in 13 minutes, 0, 0`
      - Resulting in BQ upload failure.
   - Fix applied:
      - If there is text found in new data, replace it with ZERO.
3. Routes Pull from LIFI and Socket verified.
   - Verified data pulls for new Routes update from Task:1. Everything working fine.
4. Added new metric dataset and set up any new models as views.

**Pending Tasks:**

1. Debug SQL from Carto <> Big Query: Routers bad
