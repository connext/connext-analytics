import pandas as pd
import streamlit as st
import plotly.express as px
from setup import (
    get_db_url,
    create_engine,
    get_latest_value_by_date,
    get_raw_data_from_postgres_by_sql,
)


def create_weekly_cohort_plot(df):
    # Ensure the date columns are in datetime format
    df["cohort_week"] = pd.to_datetime(df["cohort_week"])
    df["week"] = pd.to_datetime(df["week"])

    # Pivot the data to create a matrix
    cohort_pivot = df.pivot_table(
        index="cohort_week", columns="weeks_since_cohort", values="retention_rate"
    )

    # Plot the heatmap using Plotly
    fig = px.imshow(
        cohort_pivot,
        labels=dict(x="Weeks Since Cohort", y="Cohort Week", color="Retention Rate"),
        x=cohort_pivot.columns,
        y=cohort_pivot.index,
        color_continuous_scale="Blues",
    )

    fig.update_layout(
        title="Weekly Cohort Retention Rate",
        xaxis_title="Weeks Since Cohort",
        yaxis_title="Cohort Week",
    )

    st.plotly_chart(fig)


@st.cache_data(ttl=3600)
def get_metric_8_netting_rate(mode: str) -> pd.DataFrame:
    sql_query = """
        SELECT
            DATE_TRUNC('day', to_timestamp(i.origin_timestamp)) AS day,
            COUNT(i.id) AS netted_count,
            COUNT(CASE
                WHEN i.settlement_timestamp - i.origin_timestamp <= 3600 THEN i.id
            END) AS count_of_intents_within_1h,
            -- Calculating the percentage of invoices netted within 24 hour
            ROUND(COUNT(CASE
                WHEN i.settlement_timestamp - i.origin_timestamp <= 3600 THEN i.id
            END) * 100.0 / COUNT(i.id), 3) AS netting_rate_1h_percentage,
            -- Calculating the percentage of invoices netted within 24 hour
            ROUND(COUNT(CASE
                WHEN i.settlement_timestamp - i.origin_timestamp <= 86400 THEN i.id
            END) * 100.0 / COUNT(i.id), 3) AS netting_rate_24h_percentage
        FROM public.intents i
        WHERE i.settlement_status = 'SETTLED' 
        AND i.origin_ttl = 0
        GROUP BY 1;
    """
    # Database connection settings
    if mode == "prod":
        db_url = get_db_url(mode="prod")
    else:
        db_url = get_db_url(mode="test")

    # Create a database connection
    engine = create_engine(db_url)

    # Execute the query and return the result as a DataFrame
    with engine.connect() as connection:
        df = pd.read_sql_query(sql_query, connection)

    return df


def main() -> None:
    """Steps:
    1. get all raw data for each metric on daily basis- 13 metrics,
    2.  create a formated data based on the metrics
    3. apply filters on the data
    4. plot the data into line chart and big number
    """
    st.title("Everclear Testnet")

    # ----------------------------------------------------------------------------
    st.markdown("## 1. Settlement Rate 24h")

    metric_1_Settlement_Rate_24h = get_raw_data_from_postgres_by_sql(
        "Metric_1_Settlement_Rate_24h", mode="test"
    )
    latest_metric_1_Settlement_Rate_24h = get_latest_value_by_date(
        metric_1_Settlement_Rate_24h, date_col="day"
    )
    col1, col2 = st.columns(2)
    with col1:
        st.dataframe(latest_metric_1_Settlement_Rate_24h, hide_index=True)
    with col2:
        # plot line chart
        st.line_chart(metric_1_Settlement_Rate_24h, y="prct_of_settled_count", x="day")

    # ----------------------------------------------------------------------------

    st.markdown("## 2. Invoices 1h Retention Rate")
    metric_2_Invoices_1h_Retention_Rate = get_raw_data_from_postgres_by_sql(
        "Metric_2_Invoices_1h_Retention_Rate", mode="test"
    )
    latest_metric_2_Invoices_1h_Retention_Rate = get_latest_value_by_date(
        metric_2_Invoices_1h_Retention_Rate, date_col="day"
    )

    col1, col2 = st.columns(2)
    with col1:
        st.dataframe(latest_metric_2_Invoices_1h_Retention_Rate, hide_index=True)
    with col2:
        st.line_chart(
            metric_2_Invoices_1h_Retention_Rate,
            y="retention_rate",
            x="day",
        )

    # ----------------------------------------------------------------------------
    st.markdown(
        """
        ## 3. Epoch Discount
        The Epoch Discount metric involves calculating the number of discounts applied to invoices before settlement.
        1. Difference in Amounts: Calculated as origin_intent_amount - settled_amount.
        2. Difference in Epochs: Calculated as settlement_timestamp - origin_timestamp.
    """
    )
    metric_3_Epoch_Discount = get_raw_data_from_postgres_by_sql(
        "Metric_3_Epoch_Discount", mode="test"
    )

    latest_metric_3_Epoch_Discount = get_latest_value_by_date(
        metric_3_Epoch_Discount, date_col="day"
    )

    col1, col2 = st.columns(2)
    with col1:
        st.dataframe(latest_metric_3_Epoch_Discount, hide_index=True)
    with col2:
        st.line_chart(
            metric_3_Epoch_Discount,
            y="discount_epoch",
            x="day",
        )

    # ----------------------------------------------------------------------------
    st.markdown("## 4. Volume by Market Maker")
    metric_4_volume_by_market_maker = get_raw_data_from_postgres_by_sql(
        "Metric_4_Volume_by_Market_Maker", mode="test"
    )

    latest_metric_4_volume_by_market_maker = get_latest_value_by_date(
        metric_4_volume_by_market_maker, date_col="day"
    )

    col1, col2 = st.columns(2)
    with col1:
        st.dataframe(latest_metric_4_volume_by_market_maker, hide_index=True)
    with col2:
        st.line_chart(
            metric_4_volume_by_market_maker,
            y="volume_by_market_maker",
            x="day",
        )

    # ----------------------------------------------------------------------------
    st.markdown("## 5. AVG Discount Value Invoice")
    Metric_5_AVG_Discount_Value_Invoice = get_raw_data_from_postgres_by_sql(
        "Metric_5_AVG_Discount_Value_Invoice", mode="test"
    )
    latest_metric_5_AVG_Discount_Value_Invoice = get_latest_value_by_date(
        Metric_5_AVG_Discount_Value_Invoice, date_col="day"
    )
    col1, col2 = st.columns(2)
    with col1:
        st.dataframe(latest_metric_5_AVG_Discount_Value_Invoice, hide_index=True)
    with col2:
        st.line_chart(
            Metric_5_AVG_Discount_Value_Invoice,
            y="discount_value",
            x="day",
        )

    # ----------------------------------------------------------------------------
    st.markdown("## 6. APY for MM")
    metric_6_APY_for_MM = get_raw_data_from_postgres_by_sql(
        "Metric_6_APY_for_MM", mode="test"
    )

    latest_metric_6_APY_for_MM = get_latest_value_by_date(
        metric_6_APY_for_MM, date_col="day"
    )

    col1, col2 = st.columns(2)
    with col1:
        st.dataframe(latest_metric_6_APY_for_MM, hide_index=True)
    with col2:
        st.line_chart(metric_6_APY_for_MM, y="apy", x="day")

    # ----------------------------------------------------------------------------
    st.markdown("## 7. Clearing Volume")
    metric_7_Clearing_Volume = get_raw_data_from_postgres_by_sql(
        "Metric_7_Clearing_Volume", mode="test"
    )

    # get latest value for both intent_type
    latest_metric_7_Clearing_Volume = metric_7_Clearing_Volume.groupby(
        "intent_type"
    ).tail(1)

    col1, col2 = st.columns(2)
    with col1:
        st.dataframe(latest_metric_7_Clearing_Volume, hide_index=True)
    with col2:
        st.line_chart(
            metric_7_Clearing_Volume,
            y="volume",
            x="day",
            color="intent_type",
        )

    # ----------------------------------------------------------------------------
    st.markdown("## 8. Netting Rate")

    metric_8_Netting_Rate = get_metric_8_netting_rate(mode="test")
    latest_metric_8_Netting_Rate = get_latest_value_by_date(
        metric_8_Netting_Rate, date_col="day"
    )
    col1, col2 = st.columns(2)
    with col1:
        st.dataframe(latest_metric_8_Netting_Rate, hide_index=True)
    with col2:
        st.line_chart(
            metric_8_Netting_Rate,
            y=["netting_rate_1h_percentage", "netting_rate_24h_percentage"],
            x="day",
        )

    # ----------------------------------------------------------------------------
    st.markdown("## 9. Total Rebalancing Fee")
    metric_9_Total_rebalancing_fee = get_raw_data_from_postgres_by_sql(
        "Metric_9_Total_rebalancing_fee", mode="test"
    )
    latest_metric_9_Total_rebalancing_fee = get_latest_value_by_date(
        metric_9_Total_rebalancing_fee, date_col="day"
    )
    col1, col2 = st.columns(2)
    with col1:
        st.dataframe(latest_metric_9_Total_rebalancing_fee, hide_index=True)
    with col2:
        st.line_chart(
            metric_9_Total_rebalancing_fee,
            y=[
                "protocol_fee",
                "rebalancing_fee",
                "discount",
            ],
            x="day",
        )

    # ----------------------------------------------------------------------------
    st.markdown("## 10. Settlement Rate 6hrs")
    Metric_10_1_Settlement_Rate_6hrs = get_raw_data_from_postgres_by_sql(
        "Metric_10_1_Settlement_Rate_6hrs", mode="test"
    )
    latest_metric_10_1_Settlement_Rate_6hrs = get_latest_value_by_date(
        Metric_10_1_Settlement_Rate_6hrs, date_col="day"
    )
    col1, col2 = st.columns(2)
    with col1:
        st.dataframe(latest_metric_10_1_Settlement_Rate_6hrs, hide_index=True)
    with col2:
        st.line_chart(
            Metric_10_1_Settlement_Rate_6hrs, y="settlement_rate_percentage", x="day"
        )

    # ----------------------------------------------------------------------------

    st.markdown("## 10. Settlement Rate 24hrs")
    Metric_10_2_Settlement_Rate_24hrs = get_raw_data_from_postgres_by_sql(
        "Metric_10_2_Settlement_Rate_24hrs", mode="test"
    )
    latest_metric_10_2_Settlement_Rate_24hrs = get_latest_value_by_date(
        Metric_10_2_Settlement_Rate_24hrs, date_col="day"
    )
    col1, col2 = st.columns(2)
    with col1:
        st.dataframe(latest_metric_10_2_Settlement_Rate_24hrs, hide_index=True)
    with col2:
        st.line_chart(
            Metric_10_2_Settlement_Rate_24hrs, y="settlement_rate_percentage", x="day"
        )

    # ----------------------------------------------------------------------------

    st.markdown("## 11. Total Protocol Revenue")
    metric_11_Total_Protocol_Revenue = get_raw_data_from_postgres_by_sql(
        "Metric_11_Total_Protocol_Revenue", mode="test"
    )

    latest_metric_11_Total_Protocol_Revenue = get_latest_value_by_date(
        metric_11_Total_Protocol_Revenue, date_col="day"
    )
    col1, col2 = st.columns(2)
    with col1:
        st.dataframe(latest_metric_11_Total_Protocol_Revenue, hide_index=True)
    with col2:
        st.line_chart(metric_11_Total_Protocol_Revenue, y="protocol_fee", x="day")

    # ----------------------------------------------------------------------------
    st.markdown("## 12. Settlement Time")
    metric_12_Settlement_Time = get_raw_data_from_postgres_by_sql(
        "Metric_12_Settlement_Time", mode="test"
    )
    latest_metric_12_Settlement_Time = get_latest_value_by_date(
        metric_12_Settlement_Time, date_col="day"
    )
    col1, col2 = st.columns(2)
    with col1:
        st.dataframe(latest_metric_12_Settlement_Time, hide_index=True)
    with col2:
        st.line_chart(metric_12_Settlement_Time, y="avg_settlement_time", x="day")
    # ----------------------------------------------------------------------------

    st.markdown("## 13. Wallet Retention Rate")
    metric_13_Wallet_Retention_Rate = get_raw_data_from_postgres_by_sql(
        "Metric_13_Wallet_Retention_Rate", mode="test"
    )
    create_weekly_cohort_plot(metric_13_Wallet_Retention_Rate)

    # ----------------------------------------------------------------------------
    st.markdown("## 14. Average Intent Size")
    metric_14_Average_Intent_Size = get_raw_data_from_postgres_by_sql(
        "Metric_14_Average_Intent_Size", mode="test"
    )
    latest_metric_14_Average_Intent_Size = get_latest_value_by_date(
        metric_14_Average_Intent_Size, date_col="day"
    )
    col1, col2 = st.columns(2)
    with col1:
        st.dataframe(latest_metric_14_Average_Intent_Size, hide_index=True)
    with col2:
        st.line_chart(metric_14_Average_Intent_Size, y="avg_intent_size", x="day")

    # # ----------------------------------------------------------------------------
    st.markdown("## 15. Number of Intents")
    metric_15_Number_of_Intents = get_raw_data_from_postgres_by_sql(
        "Metric_15_Number_of_Intents", mode="test"
    )
    latest_metric_15_Number_of_Intents = get_latest_value_by_date(
        metric_15_Number_of_Intents, date_col="day"
    )
    col1, col2 = st.columns(2)
    with col1:
        st.dataframe(latest_metric_15_Number_of_Intents, hide_index=True)
    with col2:
        st.line_chart(metric_15_Number_of_Intents, y="total_intents", x="day")


if __name__ == "__main__":
    main()
