# Connext Data pipline


## notes

- `2024-02-12 19:59:17 UTC` job run on LIFI has all the data that ran for 2 hours.

## Bridge Analytics

Notes:

    - Data Aggreagtion are daily and tx level

- Chains
    - Native Bridges
        - ETH
            - Source: Dune
            - data_agg : daily
        - ARB
            - Source: Dune
        - OP
        - BNB
        - Gnosis
        - Linea
            - source_1: Dune(No Withdrawal Data available)
            - explorer has data limit on only last 10k tx to pull
            - The total tx data is around 500k
        - Mode
            - source_1: Dune
            - source_2: https://explorer.mode.network/api-docs
        - Matic
        - Base
        - Metis
            - Source: https://explorer.metis.io/documentation/api-swagger
    - 3rd Party Bridge
        - Across
        - Stargate
        - Synopsis
        - Hop
        - misc


# data needed -> Fee paid | gas fee | from_token | to_token