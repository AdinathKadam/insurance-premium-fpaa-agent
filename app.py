import streamlit as st
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go

from data_access.db import (
    get_available_months,
    get_year_split,
    get_kpi_snapshot,
    get_nbw_renewal_trend,
    get_top_states,
    get_channel_mix,
    get_product_mix,
    get_actual_forecast_vs_plan,
    get_control_totals,
)


st.set_page_config(
    page_title="Insurance Premium FP&A Decision Support Assistant",
    layout="wide"
)


def fmt_currency(x):
    if x is None or pd.isna(x):
        return "—"
    return f"${x:,.0f}"


def fmt_number(x):
    if x is None or pd.isna(x):
        return "—"
    return f"{x:,.0f}"


def fmt_pct(x):
    if x is None or pd.isna(x):
        return "—"
    return f"{x:.1%}"


def prepare_exec_trend(actual_df: pd.DataFrame, plan_fcst_df: pd.DataFrame) -> pd.DataFrame:
    """
    Build a clean Jan-Dec executive trend table with:
    - 2024 Actual
    - 2025 Actual
    - 2026 Actual (Jan-Apr only)
    - 2026 Forecast (May-Dec only)
    - 2026 Plan (Jan-Dec)
    """
    month_lookup = pd.DataFrame({
        "month_num": list(range(1, 13)),
        "month_label": ["Jan", "Feb", "Mar", "Apr", "May", "Jun",
                        "Jul", "Aug", "Sep", "Oct", "Nov", "Dec"]
    })

    actuals = actual_df.copy()
    actuals["month_start"] = pd.to_datetime(actuals["month_start"])
    actuals["year_num"] = actuals["month_start"].dt.year
    actuals["month_num"] = actuals["month_start"].dt.month

    actual_2024 = (
        actuals[actuals["year_num"] == 2024][["month_num", "gwp"]]
        .groupby("month_num", as_index=False)
        .sum()
        .rename(columns={"gwp": "actual_2024"})
    )

    actual_2025 = (
        actuals[actuals["year_num"] == 2025][["month_num", "gwp"]]
        .groupby("month_num", as_index=False)
        .sum()
        .rename(columns={"gwp": "actual_2025"})
    )

    actual_2026 = (
        actuals[(actuals["year_num"] == 2026) & (actuals["month_num"] <= 4)][["month_num", "gwp"]]
        .groupby("month_num", as_index=False)
        .sum()
        .rename(columns={"gwp": "actual_2026"})
    )

    pf = plan_fcst_df.copy()
    pf["month_start"] = pd.to_datetime(pf["month_start"])
    pf["month_num"] = pf["month_start"].dt.month

    plan_2026 = (
        pf[["month_num", "plan_gwp"]]
        .drop_duplicates()
        .rename(columns={"plan_gwp": "plan_2026"})
    )

    forecast_2026 = (
        pf[pf["month_num"] >= 5][["month_num", "forecast_gwp"]]
        .drop_duplicates()
        .rename(columns={"forecast_gwp": "forecast_2026"})
    )

    exec_df = month_lookup.copy()
    exec_df = exec_df.merge(actual_2024, on="month_num", how="left")
    exec_df = exec_df.merge(actual_2025, on="month_num", how="left")
    exec_df = exec_df.merge(actual_2026, on="month_num", how="left")
    exec_df = exec_df.merge(plan_2026, on="month_num", how="left")
    exec_df = exec_df.merge(forecast_2026, on="month_num", how="left")

    return exec_df.sort_values("month_num")


st.title("Insurance Premium FP&A Decision Support Assistant")
st.caption("Synthetic demo only. No company financial, policyholder, claims, operational, or internal data used.")

# Sidebar
st.sidebar.header("Control Panel")

months_df = get_available_months()
month_options = months_df["month_start"].astype(str).tolist()

default_month = "2026-04-01"
default_index = month_options.index(default_month) if default_month in month_options else len(month_options) - 1

selected_month = st.sidebar.selectbox(
    "Select Reporting Month",
    month_options,
    index=default_index
)

selected_month_display = pd.to_datetime(selected_month).strftime("%B %Y")

st.sidebar.markdown("### Demo Scope")
st.sidebar.write("- GWP")
st.sidebar.write("- NBW / Renewal")
st.sidebar.write("- Unique pet counts")
st.sidebar.write("- Average premium")
st.sidebar.write("- State / channel / product mix")
st.sidebar.write("- Plan and forecast comparison")

# Load data
kpi = get_kpi_snapshot(selected_month)
nbw_trend = get_nbw_renewal_trend()
top_states = get_top_states(selected_month)
channel_mix = get_channel_mix(selected_month)
product_mix = get_product_mix(selected_month)
actual_fcst_plan = get_actual_forecast_vs_plan()
year_split = get_year_split()
controls = get_control_totals()

exec_trend = prepare_exec_trend(nbw_trend, actual_fcst_plan)

# KPI cards
st.subheader(f"Executive Summary — {selected_month_display}")

c1, c2, c3, c4, c5 = st.columns(5)

with c1:
    st.metric("Total GWP", fmt_currency(kpi.get("gwp")), f"vs Plan {fmt_pct(kpi.get('gwp_vs_plan_pct'))}")
with c2:
    st.metric("NBW GWP", fmt_currency(kpi.get("nbw_gwp")), f"vs Plan {fmt_pct(kpi.get('nbw_vs_plan_pct'))}")
with c3:
    st.metric("Renewal GWP", fmt_currency(kpi.get("renewal_gwp")), f"vs Plan {fmt_pct(kpi.get('renewal_vs_plan_pct'))}")
with c4:
    st.metric("NBW Unique Pets", fmt_number(kpi.get("nbw_unique_pets")), f"vs Plan {fmt_pct(kpi.get('nbw_unique_pets_vs_plan_pct'))}")
with c5:
    st.metric("Renewal Unique Pets", fmt_number(kpi.get("renewal_unique_pets")), f"vs Plan {fmt_pct(kpi.get('renewal_unique_pets_vs_plan_pct'))}")

c6, c7, c8, c9, c10 = st.columns(5)

with c6:
    st.metric("Avg NBW Premium", fmt_currency(kpi.get("avg_nbw_premium")), f"vs Plan {fmt_pct(kpi.get('avg_nbw_vs_plan_pct'))}")
with c7:
    st.metric("Avg Renewal Premium", fmt_currency(kpi.get("avg_renewal_premium")), f"vs Plan {fmt_pct(kpi.get('avg_renewal_vs_plan_pct'))}")
with c8:
    st.metric("GWP MoM %", fmt_pct(kpi.get("gwp_mom_pct")))
with c9:
    st.metric("GWP YoY %", fmt_pct(kpi.get("gwp_yoy_pct")))
with c10:
    st.metric("Renewal Pet Share", fmt_pct(kpi.get("renewal_pet_share")))

tab1, tab2, tab3, tab4, tab5, tab6 = st.tabs([
    "Executive Trend",
    "NBW & Renewal",
    "State / Channel Drivers",
    "Plan & Forecast",
    "Management Commentary",
    "Controls"
])

with tab1:
    st.markdown("### GWP Trend: 2024 Actual, 2025 Actual, 2026 Actual, Forecast, and Plan")

    fig = go.Figure()

    fig.add_trace(go.Scatter(
        x=exec_trend["month_label"],
        y=exec_trend["actual_2024"],
        mode="lines+markers",
        name="2024 Actual"
    ))

    fig.add_trace(go.Scatter(
        x=exec_trend["month_label"],
        y=exec_trend["actual_2025"],
        mode="lines+markers",
        name="2025 Actual"
    ))

    fig.add_trace(go.Scatter(
        x=exec_trend["month_label"],
        y=exec_trend["actual_2026"],
        mode="lines+markers",
        name="2026 Actual"
    ))

    fig.add_trace(go.Scatter(
        x=exec_trend["month_label"],
        y=exec_trend["forecast_2026"],
        mode="lines+markers",
        name="2026 Forecast"
    ))

    fig.add_trace(go.Scatter(
        x=exec_trend["month_label"],
        y=exec_trend["plan_2026"],
        mode="lines+markers",
        name="2026 Plan"
    ))

    fig.update_layout(
        height=500,
        xaxis_title="Month",
        yaxis_title="GWP",
        legend_title="Series"
    )

    st.plotly_chart(fig, width="stretch")

    st.markdown("### Year Split Validation")
    year_split_display = year_split.copy()
    year_split_display["row_count"] = year_split_display["row_count"].apply(fmt_number)
    year_split_display["total_gwp"] = year_split_display["total_gwp"].apply(fmt_currency)
    st.dataframe(year_split_display, width="stretch")

with tab2:
    st.markdown("### NBW vs Renewal GWP Trend")

    trend_nr = nbw_trend.copy()
    trend_nr["month_start"] = pd.to_datetime(trend_nr["month_start"])

    # Keep actual data only through Apr 2026
    trend_nr["nbw_gwp_clean"] = trend_nr.apply(
        lambda r: r["nbw_gwp"] if not (r["month_start"].year == 2026 and r["month_start"].month > 4) else None,
        axis=1
    )
    trend_nr["renewal_gwp_clean"] = trend_nr.apply(
        lambda r: r["renewal_gwp"] if not (r["month_start"].year == 2026 and r["month_start"].month > 4) else None,
        axis=1
    )
    trend_nr["nbw_unique_pets_clean"] = trend_nr.apply(
        lambda r: r["nbw_unique_pets"] if not (r["month_start"].year == 2026 and r["month_start"].month > 4) else None,
        axis=1
    )
    trend_nr["renewal_unique_pets_clean"] = trend_nr.apply(
        lambda r: r["renewal_unique_pets"] if not (r["month_start"].year == 2026 and r["month_start"].month > 4) else None,
        axis=1
    )
    trend_nr["avg_nbw_premium_clean"] = trend_nr.apply(
        lambda r: r["avg_nbw_premium"] if not (r["month_start"].year == 2026 and r["month_start"].month > 4) else None,
        axis=1
    )
    trend_nr["avg_renewal_premium_clean"] = trend_nr.apply(
        lambda r: r["avg_renewal_premium"] if not (r["month_start"].year == 2026 and r["month_start"].month > 4) else None,
        axis=1
    )

    fig1 = go.Figure()
    fig1.add_trace(go.Bar(
        x=trend_nr["month_start"], y=trend_nr["nbw_gwp_clean"], name="NBW GWP"
    ))
    fig1.add_trace(go.Bar(
        x=trend_nr["month_start"], y=trend_nr["renewal_gwp_clean"], name="Renewal GWP"
    ))
    fig1.update_layout(
        barmode="group",
        height=450,
        xaxis_title="Month",
        yaxis_title="GWP"
    )
    st.plotly_chart(fig1, width="stretch")

    st.markdown("### NBW vs Renewal Unique Pets")

    fig2 = go.Figure()
    fig2.add_trace(go.Scatter(
        x=trend_nr["month_start"], y=trend_nr["nbw_unique_pets_clean"],
        mode="lines+markers", name="NBW Unique Pets"
    ))
    fig2.add_trace(go.Scatter(
        x=trend_nr["month_start"], y=trend_nr["renewal_unique_pets_clean"],
        mode="lines+markers", name="Renewal Unique Pets"
    ))
    fig2.update_layout(
        height=450,
        xaxis_title="Month",
        yaxis_title="Unique Pet Count"
    )
    st.plotly_chart(fig2, width="stretch")

    st.markdown("### Average Premium Trend")

    fig3 = go.Figure()
    fig3.add_trace(go.Scatter(
        x=trend_nr["month_start"], y=trend_nr["avg_nbw_premium_clean"],
        mode="lines+markers", name="Avg NBW Premium"
    ))
    fig3.add_trace(go.Scatter(
        x=trend_nr["month_start"], y=trend_nr["avg_renewal_premium_clean"],
        mode="lines+markers", name="Avg Renewal Premium"
    ))
    fig3.update_layout(
        height=450,
        xaxis_title="Month",
        yaxis_title="Average Premium"
    )
    st.plotly_chart(fig3, width="stretch")

with tab3:
    left, right = st.columns(2)

    with left:
        st.markdown("### Top 10 States by GWP")
        fig_states = px.bar(
            top_states.sort_values("current_gwp", ascending=True),
            x="current_gwp",
            y="state",
            orientation="h"
        )
        fig_states.update_layout(
            height=500,
            xaxis_title="GWP",
            yaxis_title="State"
        )
        st.plotly_chart(fig_states, width="stretch")

    with right:
        st.markdown("### Channel Mix")
        fig_channel = px.bar(
            channel_mix.sort_values("gwp", ascending=False),
            x="iph_channel_5",
            y="gwp"
        )
        fig_channel.update_layout(
            height=500,
            xaxis_title="IPH Channel (5)",
            yaxis_title="GWP"
        )
        st.plotly_chart(fig_channel, width="stretch")

    st.markdown("### State YoY Table")
    top_states_display = top_states.copy()
    top_states_display["current_gwp"] = top_states_display["current_gwp"].apply(fmt_currency)
    top_states_display["py_gwp"] = top_states_display["py_gwp"].apply(fmt_currency)
    top_states_display["yoy_pct"] = top_states_display["yoy_pct"].apply(fmt_pct)
    top_states_display["share_pct"] = top_states_display["share_pct"].apply(fmt_pct)
    st.dataframe(top_states_display, width="stretch")

    st.markdown("### Product Mix")
    fig_product = px.bar(
        product_mix.sort_values("gwp", ascending=False),
        x="product",
        y="gwp"
    )
    fig_product.update_layout(
        height=400,
        xaxis_title="Product",
        yaxis_title="GWP"
    )
    st.plotly_chart(fig_product, width="stretch")

with tab4:
    st.markdown("### Actual + Forecast vs Plan")

    afp = actual_fcst_plan.copy()
    afp["month_start"] = pd.to_datetime(afp["month_start"])
    afp["actual_or_forecast_gwp"] = afp["actual_gwp"].fillna(0) + afp["forecast_gwp"].fillna(0)

    fig_afp = go.Figure()
    fig_afp.add_trace(go.Bar(
        x=afp["month_start"], y=afp["plan_gwp"], name="Plan GWP"
    ))
    fig_afp.add_trace(go.Scatter(
        x=afp["month_start"], y=afp["actual_or_forecast_gwp"],
        mode="lines+markers", name="Actual + Forecast GWP"
    ))
    fig_afp.update_layout(
        height=500,
        xaxis_title="Month",
        yaxis_title="GWP"
    )
    st.plotly_chart(fig_afp, width="stretch")

    st.markdown("### Monthly Plan / Forecast Detail")
    afp_display = afp.copy()

    currency_cols = [
        "actual_gwp", "actual_nbw_gwp", "actual_renewal_gwp",
        "forecast_gwp", "forecast_nbw_gwp", "forecast_renewal_gwp",
        "plan_gwp", "plan_nbw_gwp", "plan_renewal_gwp",
        "actual_or_forecast_gwp"
    ]

    number_cols = [
        "actual_nbw_unique_pets", "actual_renewal_unique_pets",
        "forecast_nbw_unique_pets", "forecast_renewal_unique_pets",
        "plan_nbw_unique_pets", "plan_renewal_unique_pets"
    ]

    for col in currency_cols:
        if col in afp_display.columns:
            afp_display[col] = afp_display[col].apply(fmt_currency)

    for col in number_cols:
        if col in afp_display.columns:
            afp_display[col] = afp_display[col].apply(fmt_number)

    st.dataframe(afp_display, width="stretch")

with tab5:
    st.markdown("### Draft Management Commentary")

    commentary = f"""
### {selected_month_display} Summary

- Total GWP for the month is **{fmt_currency(kpi.get('gwp'))}**, with month-over-month movement of **{fmt_pct(kpi.get('gwp_mom_pct'))}** and year-over-year movement of **{fmt_pct(kpi.get('gwp_yoy_pct'))}**.
- NBW GWP is **{fmt_currency(kpi.get('nbw_gwp'))}**, while Renewal GWP is **{fmt_currency(kpi.get('renewal_gwp'))}**.
- NBW Unique Pets are **{fmt_number(kpi.get('nbw_unique_pets'))}**, and Renewal Unique Pets are **{fmt_number(kpi.get('renewal_unique_pets'))}**.
- Average NBW Premium is **{fmt_currency(kpi.get('avg_nbw_premium'))}**, while Average Renewal Premium is **{fmt_currency(kpi.get('avg_renewal_premium'))}**.
- GWP versus plan is **{fmt_pct(kpi.get('gwp_vs_plan_pct'))}**, with NBW versus plan at **{fmt_pct(kpi.get('nbw_vs_plan_pct'))}** and Renewal versus plan at **{fmt_pct(kpi.get('renewal_vs_plan_pct'))}**.
- Renewal Pet Share currently stands at **{fmt_pct(kpi.get('renewal_pet_share'))}** and should be treated as a **synthetic proxy indicator**, not an official retention measure.

### Management Focus Areas

- Review whether current GWP performance is primarily driven by **new business** or **renewals**.
- Review the **top states** and **channel mix** to identify concentration and growth drivers.
- Compare **2026 actuals plus forecast** against the **full-year plan** to assess expected finish.
- Use this synthetic workflow as a prototype for how an internal insurance FP&A assistant could support **management-call preparation**.
"""
    st.markdown(commentary)

    st.info("Retention-style metrics shown here are synthetic proxy indicators for demonstration purposes.")

with tab6:
    st.markdown("### Source and Control Information")
    st.write("Source tables used:")
    for t in controls["source_tables"]:
        st.write(f"- {t}")

    st.markdown("### Table Counts")
    controls_display = controls["table_counts"].copy()
    controls_display["row_count"] = controls_display["row_count"].apply(fmt_number)
    st.dataframe(controls_display, width="stretch")

    st.warning("Synthetic demo only. No company financial, policyholder, claims, operational, or internal data used.")