import pandas as pd
import streamlit as st
import plotly.express as px
from setup import (
    get_db_url,
    create_engine,
    get_latest_value_by_date,
    get_raw_data_from_postgres_by_sql,
    get_agg_data_from_sql_template,
    apply_date_filter_to_df,
    sql_template_filter_date,
)
from jinja2 import Template
from datetime import datetime, timedelta
import pytz
import logging
from sqlalchemy.exc import SQLAlchemyError


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


@st.cache_data(ttl=3600)
def get_metric_8_agg_netting_rate(mode: str, date_filter: dict) -> pd.DataFrame:
    sql_query = """
        WITH raw AS (
            SELECT
            COUNT(i.id) AS total_intents,
            COUNT(CASE
                WHEN (i.settlement_timestamp - i.origin_timestamp <= 3600)
                AND i.settlement_status = 'SETTLED' 
                AND CAST(i.origin_ttl AS INTEGER) = 0
                THEN i.id
            END) AS count_of_intents_within_1h,
            COUNT(CASE
                WHEN (i.settlement_timestamp - i.origin_timestamp <= 86400)
                AND i.settlement_status = 'SETTLED' 
                AND CAST(i.origin_ttl AS INTEGER) = 0
                THEN i.id
            END) AS count_of_intents_within_24h
        FROM public.intents i
        WHERE DATE_TRUNC('day', to_timestamp(i.origin_timestamp))  >= DATE('{{ from_date }}') AND DATE_TRUNC('day', to_timestamp(i.origin_timestamp))  <= DATE('{{ to_date }}')
        )

        SELECT
            -- # netting rate 1h
            ROUND(count_of_intents_within_1h * 100.0 / total_intents, 2) AS netting_rate_1h_percentage,
            -- # netting rate 24h
            ROUND(count_of_intents_within_24h * 100.0 / total_intents, 2) AS netting_rate_24h_percentage
        FROM raw;
    """

    try:

        query = Template(sql_query).render(date_filter)

        logging.info(f"Generated SQL: {query}")
        db_url = get_db_url(mode)
        engine = create_engine(db_url)
        with engine.connect() as connection:
            df = pd.read_sql_query(query, connection)
        return df
    except SQLAlchemyError as e:
        logging.error(f"Database query failed: {e}")
        raise
    except Exception as e:
        logging.error(f"An unexpected error occurred: {e}")
        raise


def main() -> None:
    """Steps:
    1. get all raw data for each metric on daily basis- 13 metrics,
    2.  create a formated data based on the metrics
    3. apply filters on the data
    4. plot the data into line chart and big number
    """
    st.title("Everclear Testnet")

    st.sidebar.header("Filters")
    st.sidebar.subheader("Time Range")

    # last 30 days
    default_start, default_end = (
        datetime.now(pytz.utc) - timedelta(days=30),
        datetime.now(pytz.utc) - timedelta(days=1),
    )

    from_date = st.sidebar.date_input(
        "Start Date(inclusive)",
        value=default_start,
        max_value=default_end,
        key="start_date",
    )
    to_date = st.sidebar.date_input(
        "End Date(inclusive)",
        value=default_end,
        min_value=default_start,
        max_value=default_end,
        key="end_date",
    )

    # ----------------------------------------------------------------------------
    st.markdown("## 1. Settlement Rate 24h")

    agg_metric_1_Settlement_Rate_24h = get_agg_data_from_sql_template(
        "agg_metric_1_Settlement_Rate_24h",
        date_filter={"from_date": from_date, "to_date": to_date},
        mode="test",
    )

    metric_1_Settlement_Rate_24h = apply_date_filter_to_df(
        get_raw_data_from_postgres_by_sql("Metric_1_Settlement_Rate_24h", mode="test"),
        from_date=from_date,
        to_date=to_date,
    )

    col1, col2 = st.columns(2)
    with col1:
        # big number
        st.metric(
            "Settlement Rate 24h",
            value=f"{agg_metric_1_Settlement_Rate_24h['prct_of_settled_count'].values[0]:.2f}%",
        )
    with col2:
        # plot line chart
        st.line_chart(metric_1_Settlement_Rate_24h, y="prct_of_settled_count", x="day")

    # ----------------------------------------------------------------------------

    st.markdown("## 2. Invoices 1h Retention Rate")
    agg_metric_2_Invoices_1h_Retention_Rate = get_agg_data_from_sql_template(
        "agg_metric_2_Invoices_1h_Retention_Rate",
        date_filter={"from_date": from_date, "to_date": to_date},
        mode="test",
    )
    metric_2_Invoices_1h_Retention_Rate = apply_date_filter_to_df(
        get_raw_data_from_postgres_by_sql(
            "Metric_2_Invoices_1h_Retention_Rate", mode="test"
        ),
        from_date=from_date,
        to_date=to_date,
    )
    col1, col2 = st.columns(2)
    with col1:
        # big number
        st.metric(
            "Invoices 1h Retention Rate",
            value=f"{agg_metric_2_Invoices_1h_Retention_Rate['retention_rate'].values[0]:.2f}%",
        )
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
    agg_metric_3_Epoch_Discount = get_agg_data_from_sql_template(
        "agg_metric_3_Epoch_Discount",
        date_filter={"from_date": from_date, "to_date": to_date},
        mode="test",
    )
    st.write(agg_metric_3_Epoch_Discount)
    metric_3_Epoch_Discount = apply_date_filter_to_df(
        get_raw_data_from_postgres_by_sql("Metric_3_Epoch_Discount", mode="test"),
        from_date=from_date,
        to_date=to_date,
    )

    col1, col2 = st.columns(2)
    with col1:
        # big number
        st.metric(
            "Epoch Discount",
            value=f"{agg_metric_3_Epoch_Discount['discount_epoch'].values[0]:.0f} epochs",
        )
    with col2:
        st.line_chart(
            metric_3_Epoch_Discount,
            y="discount_epoch",
            x="day",
        )

    # ----------------------------------------------------------------------------
    st.markdown("## 4. Volume by Market Maker")
    agg_metric_4_Volume_by_Market_Maker = get_agg_data_from_sql_template(
        "agg_metric_4_Volume_by_Market_Maker",
        date_filter={"from_date": from_date, "to_date": to_date},
        mode="test",
    )
    metric_4_volume_by_market_maker = apply_date_filter_to_df(
        get_raw_data_from_postgres_by_sql(
            "Metric_4_Volume_by_Market_Maker", mode="test"
        ),
        from_date=from_date,
        to_date=to_date,
    )

    col1, col2 = st.columns(2)
    with col1:
        # big number
        st.metric(
            "Volume by Market Maker",
            value=f"{agg_metric_4_Volume_by_Market_Maker['volume_by_market_maker'].values[0]:.2f} USD",
        )
    with col2:
        st.line_chart(
            metric_4_volume_by_market_maker, y="volume_by_market_maker", x="day"
        )

    # ----------------------------------------------------------------------------
    st.markdown("## 5. AVG Discount Value Invoice")
    agg_metric_5_AVG_Discount_Value_Invoice = get_agg_data_from_sql_template(
        "agg_metric_5_AVG_Discount_Value_Invoice",
        date_filter={"from_date": from_date, "to_date": to_date},
        mode="test",
    )
    metric_5_AVG_Discount_Value_Invoice = apply_date_filter_to_df(
        get_raw_data_from_postgres_by_sql(
            "Metric_5_AVG_Discount_Value_Invoice", mode="test"
        ),
        from_date=from_date,
        to_date=to_date,
    )

    col1, col2 = st.columns(2)
    with col1:
        st.metric(
            "AVG Discount Value Invoice",
            value=f"{agg_metric_5_AVG_Discount_Value_Invoice['discount_value'].values[0]:.2f} USD",
        )
    with col2:
        st.line_chart(
            metric_5_AVG_Discount_Value_Invoice,
            y="discount_value",
            x="day",
        )

    # ----------------------------------------------------------------------------
    st.markdown("## 6. APY for MM")
    metric_6_APY_for_MM = apply_date_filter_to_df(
        get_raw_data_from_postgres_by_sql("Metric_6_APY_for_MM", mode="test"),
        from_date=from_date,
        to_date=to_date,
    )

    agg_metric_6_APY_for_MM = get_agg_data_from_sql_template(
        "agg_metric_6_APY_for_MM",
        date_filter={"from_date": from_date, "to_date": to_date},
        mode="test",
    )

    col1, col2 = st.columns(2)
    with col1:
        st.metric(
            "APY for MM",
            value=f"{agg_metric_6_APY_for_MM['apy'].values[0]:.2f}%",
        )
    with col2:
        st.line_chart(metric_6_APY_for_MM, y="apy", x="day")

    # ----------------------------------------------------------------------------
    st.markdown("## 7. Clearing Volume")
    agg_metric_7_Clearing_Volume = get_agg_data_from_sql_template(
        "agg_metric_7_Clearing_Volume",
        date_filter={"from_date": from_date, "to_date": to_date},
        mode="test",
    )

    metric_7_Clearing_Volume = apply_date_filter_to_df(
        get_raw_data_from_postgres_by_sql("Metric_7_Clearing_Volume", mode="test"),
        from_date=from_date,
        to_date=to_date,
    )

    col1, col2 = st.columns(2)
    with col1:
        st.metric(
            "Clearing Volume: Netted",
            value=f"{agg_metric_7_Clearing_Volume['netted_volume'].values[0]:.2f} USD",
        )
        st.metric(
            "Clearing Volume: MM",
            value=f"{agg_metric_7_Clearing_Volume['market_maker_volume'].values[0]:.2f} USD",
        )
    with col2:
        st.line_chart(
            metric_7_Clearing_Volume,
            y=["netted_volume", "market_maker_volume"],
            x="day",
        )

    # ----------------------------------------------------------------------------
    st.markdown("## 8. Netting Rate")
    agg_metric_8_Netting_Rate = get_metric_8_agg_netting_rate(
        mode="test",
        date_filter={"from_date": from_date, "to_date": to_date},
    )

    metric_8_Netting_Rate = apply_date_filter_to_df(
        get_metric_8_netting_rate(mode="test"),
        from_date=from_date,
        to_date=to_date,
    )

    col1, col2 = st.columns(2)
    with col1:

        st.metric(
            "Netting Rate 1h",
            value=f"{agg_metric_8_Netting_Rate['netting_rate_1h_percentage'].values[0]:.2f}%",
        )
        st.metric(
            "Netting Rate 24h",
            value=f"{agg_metric_8_Netting_Rate['netting_rate_24h_percentage'].values[0]:.2f}%",
        )
    with col2:
        st.line_chart(
            metric_8_Netting_Rate,
            y=["netting_rate_1h_percentage", "netting_rate_24h_percentage"],
            x="day",
        )

    # --     --------------------------------------------------------------------------
    st.markdown("## 9. Total Rebalancing Fee")
    agg_metric_9_Total_Rebalancing_Fee = get_agg_data_from_sql_template(
        "agg_metric_9_Total_rebalancing_fee",
        date_filter={"from_date": from_date, "to_date": to_date},
        mode="test",
    )
    metric_9_Total_rebalancing_fee = apply_date_filter_to_df(
        get_raw_data_from_postgres_by_sql(
            "Metric_9_Total_rebalancing_fee", mode="test"
        ),
        from_date=from_date,
        to_date=to_date,
    )

    col1, col2 = st.columns(2)
    with col1:
        st.metric(
            "Total Rebalancing Fee",
            value=f"{agg_metric_9_Total_Rebalancing_Fee['rebalancing_fee'].values[0]:.2f} USD",
        )
    with col2:
        st.line_chart(
            metric_9_Total_rebalancing_fee,
            y="rebalancing_fee",
            x="day",
        )

    # ----------------------------------------------------------------------------
    st.markdown("## 10. Settlement Rate")
    agg_metric_10_Settlement_Rate = get_agg_data_from_sql_template(
        "agg_metric_10_Settlement_Rate",
        date_filter={"from_date": from_date, "to_date": to_date},
        mode="test",
    )

    metric_10_Settlement_Rate = apply_date_filter_to_df(
        get_raw_data_from_postgres_by_sql("Metric_10_Settlement_Rate", mode="test"),
        from_date=from_date,
        to_date=to_date,
    )
    col1, col2 = st.columns(2)
    with col1:
        st.metric(
            "Settlement Rate 6h",
            value=f"{agg_metric_10_Settlement_Rate['settlement_rate_6h_percentage'].values[0]:.2f}%",
        )
        st.metric(
            "Settlement Rate 24h",
            value=f"{agg_metric_10_Settlement_Rate['settlement_rate_24h_percentage'].values[0]:.2f}%",
        )
    with col2:
        st.line_chart(
            metric_10_Settlement_Rate,
            y=["settlement_rate_6h_percentage", "settlement_rate_24h_percentage"],
            x="day",
        )

    # ----------------------------------------------------------------------------

    st.markdown("## 11. Total Protocol Revenue")
    agg_metric_11_Total_Protocol_Revenue = get_agg_data_from_sql_template(
        "agg_metric_11_Total_Protocol_Revenue",
        date_filter={"from_date": from_date, "to_date": to_date},
        mode="test",
    )
    metric_11_Total_Protocol_Revenue = apply_date_filter_to_df(
        get_raw_data_from_postgres_by_sql(
            "Metric_11_Total_Protocol_Revenue", mode="test"
        ),
        from_date=from_date,
        to_date=to_date,
    )

    col1, col2 = st.columns(2)
    with col1:
        st.metric(
            "Total Protocol Revenue",
            value=f"{agg_metric_11_Total_Protocol_Revenue['protocol_fee'].values[0]:.2f} USD",
        )
    with col2:
        st.line_chart(metric_11_Total_Protocol_Revenue, y="protocol_fee", x="day")

    # ----------------------------------------------------------------------------
    st.markdown("## 12. Settlement Time")
    agg_metric_12_Settlement_Time = get_agg_data_from_sql_template(
        "agg_metric_12_Settlement_Time",
        date_filter={"from_date": from_date, "to_date": to_date},
        mode="test",
    )
    metric_12_Settlement_Time = apply_date_filter_to_df(
        get_raw_data_from_postgres_by_sql("Metric_12_Settlement_Time", mode="test"),
        from_date=from_date,
        to_date=to_date,
    )

    col1, col2 = st.columns(2)
    with col1:
        st.metric(
            "Settlement Time 1h",
            value=f"{agg_metric_12_Settlement_Time['overall_avg_settlement_time'].values[0]:.2f} hours",
        )
        st.metric(
            "Settlement Time 1h",
            value=f"{agg_metric_12_Settlement_Time['mm_avg_settlement_time'].values[0]:.2f} hours",
        )
        st.metric(
            "Settlement Time 1h",
            value=f"{agg_metric_12_Settlement_Time['netting_avg_settlement_time'].values[0]:.2f} hours",
        )
    with col2:
        st.line_chart(
            metric_12_Settlement_Time,
            y=[
                "overall_avg_settlement_time",
                "mm_avg_settlement_time",
                "netting_avg_settlement_time",
            ],
            x="day",
        )
    # ----------------------------------------------------------------------------

    st.markdown("## 13. Wallet Retention Rate")
    metric_13_Wallet_Retention_Rate = get_raw_data_from_postgres_by_sql(
        "Metric_13_Wallet_Retention_Rate", mode="test"
    )
    create_weekly_cohort_plot(metric_13_Wallet_Retention_Rate)

    # ----------------------------------------------------------------------------
    st.markdown("## 14. Average Intent Size")
    agg_metric_14_Average_Intent_Size = get_agg_data_from_sql_template(
        "agg_metric_14_Average_Intent_Size",
        date_filter={"from_date": from_date, "to_date": to_date},
        mode="test",
    )
    metric_14_Average_Intent_Size = apply_date_filter_to_df(
        get_raw_data_from_postgres_by_sql("Metric_14_Average_Intent_Size", mode="test"),
        from_date=from_date,
        to_date=to_date,
    )

    col1, col2 = st.columns(2)
    with col1:
        st.metric(
            "Average Intent Size",
            value=f"{agg_metric_14_Average_Intent_Size['avg_intent_size'].values[0]:.2f} USD",
        )
    with col2:
        st.line_chart(metric_14_Average_Intent_Size, y="avg_intent_size", x="day")

    # # ----------------------------------------------------------------------------
    st.markdown("## 15. Number of Intents")
    agg_metric_15_Number_of_Intents = get_agg_data_from_sql_template(
        "agg_metric_15_Number_of_Intents",
        date_filter={"from_date": from_date, "to_date": to_date},
        mode="test",
    )
    metric_15_Number_of_Intents = apply_date_filter_to_df(
        get_raw_data_from_postgres_by_sql("Metric_15_Number_of_Intents", mode="test"),
        from_date=from_date,
        to_date=to_date,
    )
    col1, col2 = st.columns(2)
    with col1:
        st.metric(
            "Number of Intents",
            value=f"{agg_metric_15_Number_of_Intents['total_intents'].values[0]:.2f}",
        )
    with col2:
        st.line_chart(metric_15_Number_of_Intents, y="total_intents", x="day")


if __name__ == "__main__":
    main()
