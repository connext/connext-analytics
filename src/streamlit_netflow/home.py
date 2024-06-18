import streamlit as st
from setup import page_settings


def main() -> None:
    st.title("Bridges Netflow")
    st.markdown(
        """
        ## Metrics
        
        #### Page 1: Compare netting windows against combinations chains, assets, asset groups, bridges

        **Netting of asset, occurs within a particular chain, asset_group and a pre-selected netting window**

        - Daily Avg. amount netted based on the window
        - Overall Bridges Netflow Metrics
            - Daily Avg. Netting by Asset
            - Daily Avg. Netting by Asset Group
            - Daily Avg. Netting by Bridge
            - Daily Avg. Netting by Chain

        ---
        #### page 2: Compare netting window against different netting windows
        - same filters as page 1
        - compare avg. % Vol netted in 1H, 3H, 6H, 12H,1D windows
            - Naturally the relationship is directly proportional to the timewindow
            - But intresting thing is to find pleateaus where in increase in netting window leads no significant 
            increase in % vol netted

        ---

        **Netting Window:**
        - timegroup: is 1H, 3H, 6H, 12H,1D groups- based on the netting window calculate the metrics
        based on the above selected netting window group the data and calculate the metrics
        

        **Filters**
        - Chains
        - Asset
        - Bridges
        
        **Preset**
        - Netting Window
        - Bool Selections:
            - Asset Group
            - Stablecoins      
    """
    )
    return None


if __name__ == "__main__":
    main()
