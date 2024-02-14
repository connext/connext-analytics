# Missing Data on Lifi

## Resource

- Debug Query in started BQ section

### Analysis- Steps

1.  Pipeline Failure
    - Data is getting puled everyday
    - data pulled til 12th feb 24
2.  Data Pull Failure
    - There is no data pull failure in logs
3.  Data missing in Source- API
    - `Not checked`
4.  Data agg. on daily basis:

        ````json
        [{

    "upload_date": "2024-02-12 17:13:22.000000 UTC",
    "all_routes": "2477",
    "amorok_routes": "334"
    }, {
    "upload_date": "2024-02-12 02:03:31.000000 UTC",
    "all_routes": "2517",
    "amorok_routes": "383"
    }, {
    "upload_date": "2024-02-11 02:04:34.000000 UTC",
    "all_routes": "2219",
    "amorok_routes": "1"
    }, {
    "upload_date": "2024-02-10 02:04:27.000000 UTC",
    "all_routes": "2136",
    "amorok_routes": "6"
    }, {
    "upload_date": "2024-02-09 15:33:04.000000 UTC",
    "all_routes": "2276",
    "amorok_routes": "393"
    }, {
    "upload_date": "2024-02-09 05:37:00.000000 UTC",
    "all_routes": "2181",
    "amorok_routes": "193"
    }, {
    "upload_date": "2024-02-08 05:38:03.000000 UTC",
    "all_routes": "2368",
    "amorok_routes": "364"
    }, {
    "upload_date": "2024-02-08 03:25:33.000000 UTC",
    "all_routes": "2280",
    "amorok_routes": "341"
    }, {
    "upload_date": "2024-02-07 05:31:16.000000 UTC",
    "all_routes": "2356",
    "amorok_routes": "288"
    }, {
    "upload_date": "2024-02-06 05:37:41.000000 UTC",
    "all_routes": "2340",
    "amorok_routes": "247"
    }, {
    "upload_date": "2024-02-05 20:53:50.000000 UTC",
    "all_routes": "1880",
    "amorok_routes": "308"
    }, {
    "upload_date": "2024-02-05 20:50:22.000000 UTC",
    "all_routes": "1876",
    "amorok_routes": "297"
    }, {
    "upload_date": "2024-02-04 06:03:00.000000 UTC",
    "all_routes": "2496",
    "amorok_routes": "458"
    }, {
    "upload_date": "2024-02-04 05:33:39.000000 UTC",
    "all_routes": "2469",
    "amorok_routes": "403"
    }, {
    "upload_date": "2024-02-03 06:01:51.000000 UTC",
    "all_routes": "2790",
    "amorok_routes": "471"
    }, {
    "upload_date": "2024-02-03 05:32:14.000000 UTC",
    "all_routes": "2523",
    "amorok_routes": "389"
    }, {
    "upload_date": "2024-02-02 23:32:19.000000 UTC",
    "all_routes": "2731",
    "amorok_routes": "405"
    }, {
    "upload_date": "2024-02-02 22:06:10.000000 UTC",
    "all_routes": "2127",
    "amorok_routes": "417"
    }, {
    "upload_date": "2024-02-02 21:54:33.000000 UTC",
    "all_routes": "1931",
    "amorok_routes": "351"
    }, {
    "upload_date": "2024-02-02 21:34:09.000000 UTC",
    "all_routes": "2299",
    "amorok_routes": "333"
    }, {
    "upload_date": "2024-02-02 20:02:47.000000 UTC",
    "all_routes": "2485",
    "amorok_routes": "448"
    }, {
    "upload_date": "2024-02-02 19:34:04.000000 UTC",
    "all_routes": "2527",
    "amorok_routes": "425"
    }, {
    "upload_date": "2024-02-02 18:02:59.000000 UTC",
    "all_routes": "2505",
    "amorok_routes": "442"
    }, {
    "upload_date": "2024-02-02 17:32:28.000000 UTC",
    "all_routes": "2565",
    "amorok_routes": "394"
    }, {
    "upload_date": "2024-02-02 16:04:59.000000 UTC",
    "all_routes": "2487",
    "amorok_routes": "458"
    }, {
    "upload_date": "2024-02-02 15:34:09.000000 UTC",
    "all_routes": "2455",
    "amorok_routes": "357"
    }, {
    "upload_date": "2024-02-02 14:02:28.000000 UTC",
    "all_routes": "2500",
    "amorok_routes": "458"
    }, {
    "upload_date": "2024-02-02 13:33:30.000000 UTC",
    "all_routes": "2474",
    "amorok_routes": "400"
    }, {
    "upload_date": "2024-02-02 12:04:48.000000 UTC",
    "all_routes": "2556",
    "amorok_routes": "459"
    }, {
    "upload_date": "2024-02-02 11:35:28.000000 UTC",
    "all_routes": "2389",
    "amorok_routes": "230"
    }, {
    "upload_date": "2024-02-02 10:23:49.000000 UTC",
    "all_routes": "2468",
    "amorok_routes": "406"
    }, {
    "upload_date": "2024-02-02 09:52:30.000000 UTC",
    "all_routes": "2548",
    "amorok_routes": "460"
    }, {
    "upload_date": "2024-02-01 22:06:01.000000 UTC",
    "all_routes": "1788",
    "amorok_routes": "222"
    }, {
    "upload_date": "2024-02-01 21:31:37.000000 UTC",
    "all_routes": "1965",
    "amorok_routes": "308"
    }]```

    After-Match Query
        - [SQL link for above data](https://console.cloud.google.com/bigquery?ws=!1m7!1m6!12m5!1m3!1smainnet-bigq!2sus-central1!3sf5bd6530-db17-4efc-b885-a47281dcf868!2e1)
        - [SQL for pathways that we are intentionally skipping]
          - name: `mainnet-bigq.raw.stg__inputs_connext_routes_skipped_pathways`
          - As we can see from the above table, All of the tokens we dont support(no Token Symbol available for them) are the ones we have skipped.

5. Compare the routes that are missing in amorak for 10th and 11th

### Conclusion

There was a bug with pathways. The join between table: all_possible_routes <> working_routes was set on INNER but should have been LEFT/None.
Bug has been fixed. THe data push is working.