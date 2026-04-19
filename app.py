import streamlit as st
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go

from data_access.db import (
    get_available_months,
    get_year_split,
    get_kpi_snapshot,
    get_ytd_snapshot,
    get_nbw_renewal_trend,
    get_top_states,
    get_channel_mix,
    get_product_mix,
    get_actual_forecast_vs_plan,
    get_plan_comparison_2025_2026,
    get_weekly_performance,
    get_growth_drivers,
    get_control_totals,
)
from services.commentary import (
    generate_management_commentary,
    generate_assistant_answer,
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


def plan_delta_label(x):
    if x is None or pd.isna(x):
        return "2026 Plan only"
    return f"vs Plan {fmt_pct(x)}"


def prepare_exec_trend(actual_df: pd.DataFrame, plan_fcst_df: pd.DataFrame) -> pd.DataFrame:
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

selected_month_dt = pd.to_datetime(selected_month)
selected_month_display = selected_month_dt.strftime("%B %Y")

ytd_start = pd.Timestamp(year=selected_month_dt.year, month=1, day=1)
ytd_end = selected_month_dt + pd.offsets.MonthEnd(0)

st.sidebar.markdown("### Derived Date Logic")
st.sidebar.write(f"YTD Start: {ytd_start.date()}")
st.sidebar.write(f"YTD End: {ytd_end.date()}")

st.sidebar.markdown("### Demo Scope")
st.sidebar.write("- GWP")
st.sidebar.write("- NBW / Renewal")
st.sidebar.write("- Unique pet counts")
st.sidebar.write("- Average premium")
st.sidebar.write("- Weekly, Monthly, YTD")
st.sidebar.write("- State / channel / product mix")
st.sidebar.write("- Plan and forecast comparison")
st.sidebar.write("- Growth drivers / marketing insights")
st.sidebar.write("- AI assistants")

# Load data
kpi = get_kpi_snapshot(selected_month)
ytd_kpi = get_ytd_snapshot(selected_month)
nbw_trend = get_nbw_renewal_trend()
top_states = get_top_states(selected_month)
channel_mix = get_channel_mix(selected_month)
product_mix = get_product_mix(selected_month)
actual_fcst_plan = get_actual_forecast_vs_plan()
plan_comparison = get_plan_comparison_2025_2026()
weekly_perf = get_weekly_performance(selected_month)
growth_drivers = get_growth_drivers(selected_month)
year_split = get_year_split()
controls = get_control_totals()

exec_trend = prepare_exec_trend(nbw_trend, actual_fcst_plan)

# Executive summary cards
st.subheader(f"Executive Summary — {selected_month_display}")

c1, c2, c3, c4, c5 = st.columns(5)

with c1:
    st.metric("Total GWP", fmt_currency(kpi.get("gwp")), plan_delta_label(kpi.get("gwp_vs_plan_pct")))
with c2:
    st.metric("NBW GWP", fmt_currency(kpi.get("nbw_gwp")), plan_delta_label(kpi.get("nbw_vs_plan_pct")))
with c3:
    st.metric("Renewal GWP", fmt_currency(kpi.get("renewal_gwp")), plan_delta_label(kpi.get("renewal_vs_plan_pct")))
with c4:
    st.metric("NBW Unique Pets", fmt_number(kpi.get("nbw_unique_pets")), plan_delta_label(kpi.get("nbw_unique_pets_vs_plan_pct")))
with c5:
    st.metric("Renewal Unique Pets", fmt_number(kpi.get("renewal_unique_pets")), plan_delta_label(kpi.get("renewal_unique_pets_vs_plan_pct")))

c6, c7, c8, c9, c10 = st.columns(5)

with c6:
    st.metric("Avg NBW Premium", fmt_currency(kpi.get("avg_nbw_premium")), plan_delta_label(kpi.get("avg_nbw_vs_plan_pct")))
with c7:
    st.metric("Avg Renewal Premium", fmt_currency(kpi.get("avg_renewal_premium")), plan_delta_label(kpi.get("avg_renewal_vs_plan_pct")))
with c8:
    st.metric("GWP MoM %", fmt_pct(kpi.get("gwp_mom_pct")))
with c9:
    st.metric("GWP YoY %", fmt_pct(kpi.get("gwp_yoy_pct")))
with c10:
    st.metric("Renewal Pet Share", fmt_pct(kpi.get("renewal_pet_share")))

tab1, tab2, tab3, tab4, tab5, tab6, tab7, tab8, tab9, tab10 = st.tabs([
    "Executive Trend",
    "Weekly Performance",
    "NBW & Renewal",
    "YTD Overview",
    "State / Channel Drivers",
    "Plan & Forecast",
    "Growth Drivers",
    "Management Commentary",
    "AI Assistants",
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
    st.markdown(f"### Weekly Performance — {selected_month_display}")

    weekly = weekly_perf.copy()
    if not weekly.empty:
        weekly["week_start"] = pd.to_datetime(weekly["week_start"])
        weekly["week_label"] = weekly["week_start"].dt.strftime("%b %d, %Y").str.replace(" 0", " ", regex=False)

        best_week_row = weekly.loc[weekly["gwp"].idxmax()]
        weakest_week_row = weekly.loc[weekly["gwp"].idxmin()]
        avg_weekly_gwp = weekly["gwp"].mean()
        best_nbw_row = weekly.loc[weekly["nbw_gwp"].idxmax()]
        best_renewal_row = weekly.loc[weekly["renewal_gwp"].idxmax()]

        w1, w2, w3, w4, w5 = st.columns(5)
        with w1:
            st.metric("Best Week GWP", fmt_currency(best_week_row["gwp"]))
        with w2:
            st.metric("Weakest Week GWP", fmt_currency(weakest_week_row["gwp"]))
        with w3:
            st.metric("Avg Weekly GWP", fmt_currency(avg_weekly_gwp))
        with w4:
            st.metric("Highest NBW Week", best_nbw_row["week_label"])
        with w5:
            st.metric("Highest Renewal Week", best_renewal_row["week_label"])

        fig_week = go.Figure()
        fig_week.add_trace(go.Bar(
            x=weekly["week_label"],
            y=weekly["nbw_gwp"],
            name="Weekly NBW GWP"
        ))
        fig_week.add_trace(go.Bar(
            x=weekly["week_label"],
            y=weekly["renewal_gwp"],
            name="Weekly Renewal GWP"
        ))
        fig_week.add_trace(go.Scatter(
            x=weekly["week_label"],
            y=weekly["gwp"],
            mode="lines+markers",
            name="Weekly Total GWP"
        ))
        fig_week.update_layout(
            barmode="group",
            height=500,
            xaxis_title="Week Start",
            yaxis_title="GWP",
            xaxis=dict(type="category")
        )
        st.plotly_chart(fig_week, width="stretch")

        weekly_display = weekly.copy()
        weekly_display["week_start"] = weekly_display["week_start"].dt.strftime("%Y-%m-%d")

        currency_cols = ["gwp", "nbw_gwp", "renewal_gwp", "avg_nbw_premium", "avg_renewal_premium"]
        number_cols = ["nbw_unique_pets", "renewal_unique_pets"]
        pct_cols = [
            "nbw_mix_pct", "renewal_mix_pct",
            "gwp_wow_pct", "nbw_gwp_wow_pct", "renewal_gwp_wow_pct",
            "nbw_unique_pets_wow_pct", "renewal_unique_pets_wow_pct"
        ]

        for col in currency_cols:
            if col in weekly_display.columns:
                weekly_display[col] = weekly_display[col].apply(fmt_currency)

        for col in number_cols:
            if col in weekly_display.columns:
                weekly_display[col] = weekly_display[col].apply(fmt_number)

        for col in pct_cols:
            if col in weekly_display.columns:
                weekly_display[col] = weekly_display[col].apply(fmt_pct)

        st.markdown("### Weekly Detail")
        st.dataframe(weekly_display, width="stretch")
    else:
        st.warning("No weekly data available for the selected month.")

with tab3:
    st.markdown("### NBW vs Renewal GWP Trend")

    trend_nr = nbw_trend.copy()
    trend_nr["month_start"] = pd.to_datetime(trend_nr["month_start"])

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

with tab4:
    st.markdown(f"### YTD Overview — {selected_month_dt.year} through {selected_month_display}")

    y1, y2, y3, y4, y5 = st.columns(5)

    with y1:
        st.metric("YTD GWP", fmt_currency(ytd_kpi.get("gwp")), plan_delta_label(ytd_kpi.get("gwp_vs_plan_pct")))
    with y2:
        st.metric("YTD NBW GWP", fmt_currency(ytd_kpi.get("nbw_gwp")), plan_delta_label(ytd_kpi.get("nbw_vs_plan_pct")))
    with y3:
        st.metric("YTD Renewal GWP", fmt_currency(ytd_kpi.get("renewal_gwp")), plan_delta_label(ytd_kpi.get("renewal_vs_plan_pct")))
    with y4:
        st.metric("YTD NBW Unique Pets", fmt_number(ytd_kpi.get("nbw_unique_pets")))
    with y5:
        st.metric("YTD Renewal Unique Pets", fmt_number(ytd_kpi.get("renewal_unique_pets")))

    y6, y7, y8, y9 = st.columns(4)

    with y6:
        st.metric("YTD Avg NBW Premium", fmt_currency(ytd_kpi.get("avg_nbw_premium")))
    with y7:
        st.metric("YTD Avg Renewal Premium", fmt_currency(ytd_kpi.get("avg_renewal_premium")))
    with y8:
        st.metric("YTD YoY %", fmt_pct(ytd_kpi.get("gwp_yoy_pct")))
    with y9:
        st.metric("YTD Renewal Pet Share", fmt_pct(ytd_kpi.get("renewal_pet_share")))

    ytd_compare_df = pd.DataFrame({
        "metric": ["GWP", "NBW GWP", "Renewal GWP"],
        "YTD Actual": [
            ytd_kpi.get("gwp"),
            ytd_kpi.get("nbw_gwp"),
            ytd_kpi.get("renewal_gwp"),
        ],
        "YTD Plan": [
            ytd_kpi.get("plan_gwp"),
            ytd_kpi.get("plan_nbw_gwp"),
            ytd_kpi.get("plan_renewal_gwp"),
        ],
        "Prior YTD": [
            ytd_kpi.get("py_gwp"),
            ytd_kpi.get("py_nbw_gwp"),
            ytd_kpi.get("py_renewal_gwp"),
        ]
    })

    fig_ytd = go.Figure()
    fig_ytd.add_trace(go.Bar(name="YTD Actual", x=ytd_compare_df["metric"], y=ytd_compare_df["YTD Actual"]))
    fig_ytd.add_trace(go.Bar(name="YTD Plan", x=ytd_compare_df["metric"], y=ytd_compare_df["YTD Plan"]))
    fig_ytd.add_trace(go.Bar(name="Prior YTD", x=ytd_compare_df["metric"], y=ytd_compare_df["Prior YTD"]))
    fig_ytd.update_layout(
        barmode="group",
        height=450,
        xaxis_title="Metric",
        yaxis_title="Value"
    )
    st.plotly_chart(fig_ytd, width="stretch")

with tab5:
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

with tab6:
    st.markdown("### 2025 Actual vs 2026 Plan vs 2026 Actual + Forecast")

    comp = plan_comparison.copy()
    fig_comp = go.Figure()

    fig_comp.add_trace(go.Scatter(
        x=comp["month_label"],
        y=comp["actual_2025_gwp"],
        mode="lines+markers",
        name="2025 Actual"
    ))

    fig_comp.add_trace(go.Scatter(
        x=comp["month_label"],
        y=comp["plan_2026_gwp"],
        mode="lines+markers",
        name="2026 Plan"
    ))

    fig_comp.add_trace(go.Scatter(
        x=comp["month_label"],
        y=comp["actual_plus_forecast_2026_gwp"],
        mode="lines+markers",
        name="2026 Actual + Forecast"
    ))

    fig_comp.update_layout(
        height=500,
        xaxis_title="Month",
        yaxis_title="GWP"
    )
    st.plotly_chart(fig_comp, width="stretch")

    fy_2025_actual = comp["actual_2025_gwp"].sum(skipna=True)
    fy_2026_plan = comp["plan_2026_gwp"].sum(skipna=True)
    fy_2026_af = comp["actual_plus_forecast_2026_gwp"].sum(skipna=True)

    p1, p2, p3 = st.columns(3)
    with p1:
        st.metric("FY2025 Actual", fmt_currency(fy_2025_actual))
    with p2:
        st.metric("FY2026 Plan", fmt_currency(fy_2026_plan))
    with p3:
        st.metric("FY2026 Actual + Forecast", fmt_currency(fy_2026_af))

    st.markdown("### Monthly Plan / Forecast Detail")

    afp = actual_fcst_plan.copy()
    afp["month_start"] = pd.to_datetime(afp["month_start"])
    afp["actual_or_forecast_gwp"] = afp["actual_gwp"].fillna(0) + afp["forecast_gwp"].fillna(0)
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

with tab7:
    st.markdown("### Growth Drivers / Channel Insights")

    gd = growth_drivers.copy()
    gd_top = gd.head(5).copy()

    left, right = st.columns(2)

    with left:
        st.markdown("### Top Channels by Total GWP")
        fig_gd_total = px.bar(
            gd_top.sort_values("total_gwp", ascending=False),
            x="iph_channel_5",
            y="total_gwp"
        )
        fig_gd_total.update_layout(
            height=450,
            xaxis_title="Channel",
            yaxis_title="Total GWP"
        )
        st.plotly_chart(fig_gd_total, width="stretch")

    with right:
        st.markdown("### NBW vs Renewal Mix by Top Channels")
        fig_gd_mix = go.Figure()
        fig_gd_mix.add_trace(go.Bar(
            x=gd_top["iph_channel_5"],
            y=gd_top["nbw_gwp"],
            name="NBW GWP"
        ))
        fig_gd_mix.add_trace(go.Bar(
            x=gd_top["iph_channel_5"],
            y=gd_top["renewal_gwp"],
            name="Renewal GWP"
        ))
        fig_gd_mix.update_layout(
            barmode="group",
            height=450,
            xaxis_title="Channel",
            yaxis_title="GWP"
        )
        st.plotly_chart(fig_gd_mix, width="stretch")

    st.markdown("### Growth Driver Detail")
    gd_display = gd.copy()
    gd_display["total_gwp"] = gd_display["total_gwp"].apply(fmt_currency)
    gd_display["nbw_gwp"] = gd_display["nbw_gwp"].apply(fmt_currency)
    gd_display["renewal_gwp"] = gd_display["renewal_gwp"].apply(fmt_currency)
    gd_display["nbw_unique_pets"] = gd_display["nbw_unique_pets"].apply(fmt_number)
    gd_display["renewal_unique_pets"] = gd_display["renewal_unique_pets"].apply(fmt_number)
    gd_display["nbw_mix_pct"] = gd_display["nbw_mix_pct"].apply(fmt_pct)
    gd_display["renewal_mix_pct"] = gd_display["renewal_mix_pct"].apply(fmt_pct)
    st.dataframe(gd_display, width="stretch")

with tab8:
    st.markdown("### Management Commentary")

    cache_key = f"commentary::{selected_month}"

    if cache_key not in st.session_state:
        default_text, default_source = generate_management_commentary(
            selected_month_display=selected_month_display,
            kpi=kpi,
            top_states=top_states,
            channel_mix=channel_mix,
            use_ai=False
        )
        st.session_state[cache_key] = {
            "text": default_text,
            "source": default_source
        }

    btn_col1, btn_col2 = st.columns([1, 1])

    with btn_col1:
        if st.button("Generate AI Commentary", key=f"generate_ai_{selected_month}"):
            with st.spinner("Generating commentary with AWS Bedrock..."):
                text, source = generate_management_commentary(
                    selected_month_display=selected_month_display,
                    kpi=kpi,
                    top_states=top_states,
                    channel_mix=channel_mix,
                    use_ai=True
                )
                st.session_state[cache_key] = {
                    "text": text,
                    "source": source
                }

    with btn_col2:
        if st.button("Use Default Commentary", key=f"use_default_{selected_month}"):
            text, source = generate_management_commentary(
                selected_month_display=selected_month_display,
                kpi=kpi,
                top_states=top_states,
                channel_mix=channel_mix,
                use_ai=False
            )
            st.session_state[cache_key] = {
                "text": text,
                "source": source
            }

    st.caption(f"Commentary source: {st.session_state[cache_key]['source']}")
    st.markdown(st.session_state[cache_key]["text"])

    st.info("Retention-style metrics shown here are synthetic proxy indicators for demonstration purposes.")

with tab9:
    st.markdown("### AI Assistants")

    assistant_choice = st.selectbox(
        "Choose Assistant",
        [
            "Variance Assistant",
            "Weekly Performance Assistant",
            "Growth Drivers Assistant",
            "Management Call Assistant"
        ]
    )

    assistant_map = {
        "Variance Assistant": "variance",
        "Weekly Performance Assistant": "weekly",
        "Growth Drivers Assistant": "growth",
        "Management Call Assistant": "management"
    }

    sample_questions = {
        "Variance Assistant": [
            "Why is this month above or below plan?",
            "Is the variance driven more by NBW or Renewal?",
            "Which states or channels are the biggest variance drivers?"
        ],
        "Weekly Performance Assistant": [
            "What changed week over week?",
            "Which week was strongest and which was weakest?",
            "Was this month more acquisition-led or renewal-led week to week?"
        ],
        "Growth Drivers Assistant": [
            "Which channels are driving growth?",
            "Which states or products are strongest this month?",
            "Which channels have the highest NBW mix?"
        ],
        "Management Call Assistant": [
            "Summarize this month for leadership.",
            "What should management focus on?",
            "What are the main risks or watchouts?"
        ]
    }

    st.markdown("#### Suggested Questions")
    for q in sample_questions[assistant_choice]:
        st.write(f"- {q}")

    default_question = sample_questions[assistant_choice][0]
    user_question = st.text_area(
        "Ask a question",
        value=default_question,
        height=120
    )

    if st.button("Generate Assistant Answer"):
        with st.spinner("Generating AI answer..."):
            answer, source = generate_assistant_answer(
                assistant_type=assistant_map[assistant_choice],
                user_question=user_question,
                selected_month_display=selected_month_display,
                kpi=kpi,
                weekly_perf=weekly_perf,
                top_states=top_states,
                channel_mix=channel_mix,
                product_mix=product_mix,
                growth_drivers=growth_drivers,
                plan_comparison=plan_comparison
            )
            st.caption(f"Assistant source: {source}")
            st.markdown(answer)

with tab10:
    st.markdown("### Source and Control Information")
    st.write("Source tables used:")
    for t in controls["source_tables"]:
        st.write(f"- {t}")

    st.markdown("### Table Counts")
    controls_display = controls["table_counts"].copy()
    controls_display["row_count"] = controls_display["row_count"].apply(fmt_number)
    st.dataframe(controls_display, width="stretch")

    st.warning("Synthetic demo only. No company financial, policyholder, claims, operational, or internal data used.")