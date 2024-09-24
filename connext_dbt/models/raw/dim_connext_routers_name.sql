SELECT
    router,
    name
FROM UNNEST([
    STRUCT(
        '0x76cf58ce587bc928fcc5ad895555fd040e06c61a' AS router, 'BTRST' AS name
    ),
    STRUCT(
        '0x5d527765252003AceE6545416F6a9C8D15ae8402' AS router, '01node' AS name
    ),
    STRUCT(
        '0x49a9E7ec76Bc8fDF658d09557305170d9F01D2fA' AS router,
        'BlockTech 3' AS name
    ),
    STRUCT(
        '0x6273c0965A1dB4F8A6277d490B4fD48715a42b96' AS router,
        'Xocalatl' AS name
    ),
    STRUCT(
        '0x9584Eb0356a380b25D7ED2C14c54De58a25f2581' AS router,
        'Mike Nai' AS name
    ),
    STRUCT(
        '0xC4Ae07F276768A3b74AE8c47bc108a2aF0e40eBa' AS router, 'P2P 2' AS name
    ),
    STRUCT(
        '0xEca085906cb531bdf1F87eFA85c5bE46aA5C9d2c' AS router,
        'BlockTech 2' AS name
    ),
    STRUCT(
        '0x22831e4f21ce65b33ef45df0e212b5bebf130e5a' AS router,
        'BlockTech 1' AS name
    ),
    STRUCT(
        '0xbe7bc00382a50a711d037eaecad799bb8805dfa8' AS router,
        'Minerva' AS name
    ),
    STRUCT(
        '0x63Cda9C42db542bb91a7175E38673cFb00D402b0' AS router,
        'Consensys Mesh' AS name
    ),
    STRUCT(
        '0xF26c772C0fF3a6036bDdAbDAbA22cf65ECa9F97c' AS router,
        'Connext' AS name
    ),
    STRUCT(
        '0x97b9dcB1AA34fE5F12b728D9166ae353d1e7f5C4' AS router, 'P2P 1' AS name
    ),
    STRUCT(
        '0x8cb19ce8eedf740389d428879a876a3b030b9170' AS router, 'BWare' AS name
    ),
    STRUCT(
        '0x0e62f9fa1f9b3e49759dc94494f5bc37a83d1fad' AS router,
        'Bazilik' AS name
    ),
    STRUCT(
        '0x58507fed0cb11723dfb6848c92c59cf0bbeb9927' AS router,
        'Hashquark' AS name
    ),
    STRUCT(
        '0x7ce49752fFA7055622f444df3c69598748cb2E5f' AS router,
        'Vault Staking' AS name
    ),
    STRUCT(
        '0x33b2ad85f7dba818e719fb52095dc768e0ed93ec' AS router,
        'Ethereal' AS name
    ),
    STRUCT(
        '0x048a5EcC705C280b2248aefF88fd581AbbEB8587' AS router, 'Gnosis' AS name
    ),
    STRUCT(
        '0x975574980a5Da77f5C90bC92431835D91B73669e' AS router, '01node' AS name
    ),
    STRUCT(
        '0x6892d4D1f73A65B03063B7d78174dC6350Fcc406' AS router, 'Unagii' AS name
    ),
    STRUCT(
        '0x32d63da9f776891843c90787cec54ada23abd4c2' AS router, 'Ingag' AS name
    ),
    STRUCT(
        '0xFaAB88015477493cFAa5DFAA533099C590876F21' AS router,
        'Paradox' AS name
    ),
    STRUCT(
        '0x6fd84ba95525c4ccd218f2f16f646a08b4b0a598' AS router, 'Dokia' AS name
    )
])
