---
sources:
  - la_base_eth_flow_compare_slow_fast_paths: la_base_eth_flow_compare_slow_fast_paths.sql
  - la_base_eth_flow: la_base_eth_flow.sql
  - la_base_volume_compare: la_base_volume_compare.sql
---

# Analysis of BASE Liquidity

Analysis to establish best practices for liquidity provision.

**Setup**
- Date filter
- Destination Chain filter
- Status
    - Router Liqudity Analysis
        - only fastPaths
        - slow paths is the oppourtunity
        - utilization is the key
            - higher the utlization -> higher the APY
    - Pool Liquidity Analysis
- 

## Takeaways

- Total change in volume in USD for a given Pool
- % Growth in volume in USD for a given Pool over period of week
- Compare to other chains
- 

### Doubts
- What if there is no pool liquidity <> Will the router liquidity be utilized?

### Questions

- 



















- Transaction count and Volume in base, as well as compared to other chains
    - For last 90 days, Cross tab comparison of volume and transfers on base

    - % wise comparison of Base Volume for slow and fast paths
        <AreaChart 
            data={la_base_eth_flow_compare_slow_fast_paths}  
            x=date 
            y=volume
            type=stacked100
            series=status
        />

- Flow of amount in and out of base for ETH in last 90 days        
    <BarChart 
        data={la_base_eth_flow} 
        x=status
        y=volume
    />

- Last 90 days volume compare for WETH across destination chains
    
    <AreaChart 
    data={la_base_volume_compare}  
    x=date
    y=volume
    type=stacked100
    series=destination_domain_name
    />

- Base