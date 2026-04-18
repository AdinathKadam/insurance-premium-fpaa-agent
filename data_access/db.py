import os
from typing import Dict, Any

import pandas as pd
import psycopg2
from dotenv import load_dotenv


load_dotenv()

DB_HOST = os.getenv("DB_HOST")
DB_PORT = os.getenv("DB_PORT", "5432")
DB_NAME = os.getenv("DB_NAME")
DB_USER = os.getenv("DB_USER")
DB_PASSWORD = os.getenv("DB_PASSWORD")


def get_connection():
    if not all([DB_HOST, DB_NAME, DB_USER, DB_PASSWORD]):
        raise ValueError("Missing Neon DB credentials in .env")

    return psycopg2.connect(
        host=DB_HOST,
        port=DB_PORT,
        dbname=DB_NAME,
        user=DB_USER,
        password=DB_PASSWORD,
        sslmode="require"
    )


def run_query(sql: str, params=None) -> pd.DataFrame:
    with get_connection() as conn:
        return pd.read_sql_query(sql, conn, params=params)


def get_available_months() -> pd.DataFrame:
    sql = """
    SELECT DISTINCT
        DATE_TRUNC('month', "Report Date")::date AS month_start
    FROM fact_written_details
    ORDER BY month_start;
    """
    return run_query(sql)


def get_year_split() -> pd.DataFrame:
    sql = """
    SELECT
        EXTRACT(YEAR FROM "Report Date")::int AS report_year,
        COUNT(*) AS row_count,
        ROUND(SUM("Written Amount"), 2) AS total_gwp
    FROM fact_written_details
    GROUP BY 1
    ORDER BY 1;
    """
    return run_query(sql)


def get_kpi_snapshot(selected_month: str) -> Dict[str, Any]:
    sql = """
    WITH curr AS (
        SELECT *
        FROM vw_monthly_premium_kpis
        WHERE month_start = %(selected_month)s::date
    ),
    prev AS (
        SELECT *
        FROM vw_monthly_premium_kpis
        WHERE month_start = (%(selected_month)s::date - INTERVAL '1 month')::date
    ),
    py AS (
        SELECT *
        FROM vw_monthly_premium_kpis
        WHERE month_start = (%(selected_month)s::date - INTERVAL '1 year')::date
    ),
    plan_total AS (
        SELECT
            metric_name,
            SUM(plan_amount) AS plan_amount
        FROM fact_plan
        WHERE plan_month = %(selected_month)s::date
          AND dimension_type = 'Total'
        GROUP BY metric_name
    )
    SELECT
        curr.month_start,
        curr.gwp,
        curr.nbw_gwp,
        curr.renewal_gwp,
        curr.nbw_unique_pets,
        curr.renewal_unique_pets,
        curr.avg_nbw_premium,
        curr.avg_renewal_premium,
        prev.gwp AS prev_gwp,
        py.gwp AS py_gwp,
        MAX(CASE WHEN plan_total.metric_name = 'GWP' THEN plan_total.plan_amount END) AS plan_gwp,
        MAX(CASE WHEN plan_total.metric_name = 'NBW_GWP' THEN plan_total.plan_amount END) AS plan_nbw_gwp,
        MAX(CASE WHEN plan_total.metric_name = 'RENEWAL_GWP' THEN plan_total.plan_amount END) AS plan_renewal_gwp,
        MAX(CASE WHEN plan_total.metric_name = 'NBW_UNIQUE_PETS' THEN plan_total.plan_amount END) AS plan_nbw_unique_pets,
        MAX(CASE WHEN plan_total.metric_name = 'RENEWAL_UNIQUE_PETS' THEN plan_total.plan_amount END) AS plan_renewal_unique_pets,
        MAX(CASE WHEN plan_total.metric_name = 'AVG_NBW_PREMIUM' THEN plan_total.plan_amount END) AS plan_avg_nbw_premium,
        MAX(CASE WHEN plan_total.metric_name = 'AVG_RENEWAL_PREMIUM' THEN plan_total.plan_amount END) AS plan_avg_renewal_premium
    FROM curr
    LEFT JOIN prev ON TRUE
    LEFT JOIN py ON TRUE
    LEFT JOIN plan_total ON TRUE
    GROUP BY
        curr.month_start,
        curr.gwp,
        curr.nbw_gwp,
        curr.renewal_gwp,
        curr.nbw_unique_pets,
        curr.renewal_unique_pets,
        curr.avg_nbw_premium,
        curr.avg_renewal_premium,
        prev.gwp,
        py.gwp;
    """

    df = run_query(sql, {"selected_month": selected_month})

    if df.empty:
        return {}

    row = df.iloc[0].to_dict()
    selected_year = pd.to_datetime(selected_month).year

    def pct_change(current, base):
        if base is None or pd.isna(base) or base == 0:
            return None
        return (current - base) / base

    row["gwp_mom_pct"] = pct_change(row["gwp"], row["prev_gwp"])
    row["gwp_yoy_pct"] = pct_change(row["gwp"], row["py_gwp"])

    if selected_year == 2026:
        row["gwp_vs_plan_pct"] = pct_change(row["gwp"], row["plan_gwp"])
        row["nbw_vs_plan_pct"] = pct_change(row["nbw_gwp"], row["plan_nbw_gwp"])
        row["renewal_vs_plan_pct"] = pct_change(row["renewal_gwp"], row["plan_renewal_gwp"])
        row["nbw_unique_pets_vs_plan_pct"] = pct_change(row["nbw_unique_pets"], row["plan_nbw_unique_pets"])
        row["renewal_unique_pets_vs_plan_pct"] = pct_change(row["renewal_unique_pets"], row["plan_renewal_unique_pets"])
        row["avg_nbw_vs_plan_pct"] = pct_change(row["avg_nbw_premium"], row["plan_avg_nbw_premium"])
        row["avg_renewal_vs_plan_pct"] = pct_change(row["avg_renewal_premium"], row["plan_avg_renewal_premium"])
    else:
        row["gwp_vs_plan_pct"] = None
        row["nbw_vs_plan_pct"] = None
        row["renewal_vs_plan_pct"] = None
        row["nbw_unique_pets_vs_plan_pct"] = None
        row["renewal_unique_pets_vs_plan_pct"] = None
        row["avg_nbw_vs_plan_pct"] = None
        row["avg_renewal_vs_plan_pct"] = None

    total_unique_pets = (row.get("nbw_unique_pets") or 0) + (row.get("renewal_unique_pets") or 0)
    row["renewal_pet_share"] = ((row.get("renewal_unique_pets") or 0) / total_unique_pets) if total_unique_pets > 0 else None
    row["nbw_mix_pct"] = ((row.get("nbw_gwp") or 0) / row["gwp"]) if row.get("gwp") else None
    row["renewal_mix_pct"] = ((row.get("renewal_gwp") or 0) / row["gwp"]) if row.get("gwp") else None

    return row


def get_nbw_renewal_trend() -> pd.DataFrame:
    sql = """
    SELECT
        month_start,
        gwp,
        nbw_gwp,
        renewal_gwp,
        nbw_unique_pets,
        renewal_unique_pets,
        avg_nbw_premium,
        avg_renewal_premium
    FROM vw_monthly_premium_kpis
    ORDER BY month_start;
    """
    return run_query(sql)


def get_top_states(selected_month: str, top_n: int = 10) -> pd.DataFrame:
    sql = """
    WITH curr AS (
        SELECT
            "State" AS state,
            SUM("Written Amount") AS current_gwp
        FROM fact_written_details
        WHERE DATE_TRUNC('month', "Report Date")::date = %(selected_month)s::date
        GROUP BY 1
    ),
    py AS (
        SELECT
            "State" AS state,
            SUM("Written Amount") AS py_gwp
        FROM fact_written_details
        WHERE DATE_TRUNC('month', "Report Date")::date =
              (%(selected_month)s::date - INTERVAL '1 year')::date
        GROUP BY 1
    ),
    total_curr AS (
        SELECT SUM(current_gwp) AS total_gwp FROM curr
    )
    SELECT
        c.state,
        ROUND(c.current_gwp, 2) AS current_gwp,
        ROUND(COALESCE(py.py_gwp, 0), 2) AS py_gwp,
        CASE
            WHEN COALESCE(py.py_gwp, 0) = 0 THEN NULL
            ELSE ROUND((c.current_gwp - py.py_gwp) / py.py_gwp, 6)
        END AS yoy_pct,
        ROUND(c.current_gwp / NULLIF(t.total_gwp, 0), 6) AS share_pct
    FROM curr c
    LEFT JOIN py
        ON c.state = py.state
    CROSS JOIN total_curr t
    ORDER BY c.current_gwp DESC
    LIMIT %(top_n)s;
    """
    return run_query(sql, {"selected_month": selected_month, "top_n": top_n})


def get_channel_mix(selected_month: str) -> pd.DataFrame:
    sql = """
    WITH base AS (
        SELECT
            "IPH Channel (5)" AS iph_channel_5,
            SUM("Written Amount") AS gwp
        FROM fact_written_details
        WHERE DATE_TRUNC('month', "Report Date")::date = %(selected_month)s::date
        GROUP BY 1
    ),
    total_base AS (
        SELECT SUM(gwp) AS total_gwp FROM base
    )
    SELECT
        b.iph_channel_5,
        ROUND(b.gwp, 2) AS gwp,
        ROUND(b.gwp / NULLIF(t.total_gwp, 0), 6) AS share_pct
    FROM base b
    CROSS JOIN total_base t
    ORDER BY b.gwp DESC;
    """
    return run_query(sql, {"selected_month": selected_month})


def get_product_mix(selected_month: str) -> pd.DataFrame:
    sql = """
    WITH base AS (
        SELECT
            "PRODUCT" AS product,
            SUM("Written Amount") AS gwp
        FROM fact_written_details
        WHERE DATE_TRUNC('month', "Report Date")::date = %(selected_month)s::date
        GROUP BY 1
    ),
    total_base AS (
        SELECT SUM(gwp) AS total_gwp FROM base
    )
    SELECT
        b.product,
        ROUND(b.gwp, 2) AS gwp,
        ROUND(b.gwp / NULLIF(t.total_gwp, 0), 6) AS share_pct
    FROM base b
    CROSS JOIN total_base t
    ORDER BY b.gwp DESC;
    """
    return run_query(sql, {"selected_month": selected_month})


def get_actual_forecast_vs_plan() -> pd.DataFrame:
    sql = """
    WITH actual_2026 AS (
        SELECT
            month_start,
            gwp,
            nbw_gwp,
            renewal_gwp,
            nbw_unique_pets,
            renewal_unique_pets
        FROM vw_monthly_premium_kpis
        WHERE EXTRACT(YEAR FROM month_start) = 2026
    ),
    forecast_total AS (
        SELECT
            forecast_month::date AS month_start,
            MAX(CASE WHEN metric_name = 'GWP' THEN forecast_amount END) AS forecast_gwp,
            MAX(CASE WHEN metric_name = 'NBW_GWP' THEN forecast_amount END) AS forecast_nbw_gwp,
            MAX(CASE WHEN metric_name = 'RENEWAL_GWP' THEN forecast_amount END) AS forecast_renewal_gwp,
            MAX(CASE WHEN metric_name = 'NBW_UNIQUE_PETS' THEN forecast_amount END) AS forecast_nbw_unique_pets,
            MAX(CASE WHEN metric_name = 'RENEWAL_UNIQUE_PETS' THEN forecast_amount END) AS forecast_renewal_unique_pets
        FROM fact_forecast
        WHERE dimension_type = 'Total'
        GROUP BY 1
    ),
    plan_total AS (
        SELECT
            plan_month::date AS month_start,
            MAX(CASE WHEN metric_name = 'GWP' THEN plan_amount END) AS plan_gwp,
            MAX(CASE WHEN metric_name = 'NBW_GWP' THEN plan_amount END) AS plan_nbw_gwp,
            MAX(CASE WHEN metric_name = 'RENEWAL_GWP' THEN plan_amount END) AS plan_renewal_gwp,
            MAX(CASE WHEN metric_name = 'NBW_UNIQUE_PETS' THEN plan_amount END) AS plan_nbw_unique_pets,
            MAX(CASE WHEN metric_name = 'RENEWAL_UNIQUE_PETS' THEN plan_amount END) AS plan_renewal_unique_pets
        FROM fact_plan
        WHERE dimension_type = 'Total'
        GROUP BY 1
    )
    SELECT
        p.month_start,
        a.gwp AS actual_gwp,
        a.nbw_gwp AS actual_nbw_gwp,
        a.renewal_gwp AS actual_renewal_gwp,
        a.nbw_unique_pets AS actual_nbw_unique_pets,
        a.renewal_unique_pets AS actual_renewal_unique_pets,
        f.forecast_gwp,
        f.forecast_nbw_gwp,
        f.forecast_renewal_gwp,
        f.forecast_nbw_unique_pets,
        f.forecast_renewal_unique_pets,
        p.plan_gwp,
        p.plan_nbw_gwp,
        p.plan_renewal_gwp,
        p.plan_nbw_unique_pets,
        p.plan_renewal_unique_pets
    FROM plan_total p
    LEFT JOIN actual_2026 a
        ON a.month_start = p.month_start
    LEFT JOIN forecast_total f
        ON f.month_start = p.month_start
    ORDER BY p.month_start;
    """
    return run_query(sql)


def get_plan_comparison_2025_2026() -> pd.DataFrame:
    sql = """
    WITH actual_2025 AS (
        SELECT
            EXTRACT(MONTH FROM month_start)::int AS month_num,
            SUM(gwp) AS actual_2025_gwp
        FROM vw_monthly_premium_kpis
        WHERE EXTRACT(YEAR FROM month_start) = 2025
        GROUP BY 1
    ),
    actual_2026 AS (
        SELECT
            EXTRACT(MONTH FROM month_start)::int AS month_num,
            SUM(gwp) AS actual_2026_gwp
        FROM vw_monthly_premium_kpis
        WHERE EXTRACT(YEAR FROM month_start) = 2026
        GROUP BY 1
    ),
    forecast_2026 AS (
        SELECT
            EXTRACT(MONTH FROM forecast_month)::int AS month_num,
            SUM(CASE WHEN metric_name = 'GWP' THEN forecast_amount END) AS forecast_2026_gwp
        FROM fact_forecast
        WHERE dimension_type = 'Total'
        GROUP BY 1
    ),
    plan_2026 AS (
        SELECT
            EXTRACT(MONTH FROM plan_month)::int AS month_num,
            SUM(CASE WHEN metric_name = 'GWP' THEN plan_amount END) AS plan_2026_gwp
        FROM fact_plan
        WHERE dimension_type = 'Total'
        GROUP BY 1
    ),
    month_lookup AS (
        SELECT *
        FROM (VALUES
            (1, 'Jan'), (2, 'Feb'), (3, 'Mar'), (4, 'Apr'),
            (5, 'May'), (6, 'Jun'), (7, 'Jul'), (8, 'Aug'),
            (9, 'Sep'), (10, 'Oct'), (11, 'Nov'), (12, 'Dec')
        ) AS m(month_num, month_label)
    )
    SELECT
        m.month_num,
        m.month_label,
        a25.actual_2025_gwp,
        p26.plan_2026_gwp,
        a26.actual_2026_gwp,
        f26.forecast_2026_gwp,
        COALESCE(a26.actual_2026_gwp, 0) + COALESCE(f26.forecast_2026_gwp, 0) AS actual_plus_forecast_2026_gwp
    FROM month_lookup m
    LEFT JOIN actual_2025 a25 ON m.month_num = a25.month_num
    LEFT JOIN plan_2026 p26 ON m.month_num = p26.month_num
    LEFT JOIN actual_2026 a26 ON m.month_num = a26.month_num
    LEFT JOIN forecast_2026 f26 ON m.month_num = f26.month_num
    ORDER BY m.month_num;
    """
    return run_query(sql)


def get_weekly_performance(selected_month: str) -> pd.DataFrame:
    sql = """
    WITH weekly_base AS (
        SELECT
            DATE_TRUNC('week', "Report Date")::date AS week_start,
            SUM("Written Amount") AS gwp,
            SUM(CASE WHEN "TRANS CODE" = 'New Policy Line' THEN "Written Amount" ELSE 0 END) AS nbw_gwp,
            SUM(CASE WHEN "TRANS CODE" = 'Renew Policy' THEN "Written Amount" ELSE 0 END) AS renewal_gwp,
            COUNT(DISTINCT CASE
                WHEN "TRANS CODE" = 'New Policy Line'
                 AND COALESCE("Returned Pet", 0) <> 1
                THEN "Pet ID"
            END) AS nbw_unique_pets,
            COUNT(DISTINCT CASE
                WHEN "TRANS CODE" = 'Renew Policy'
                 AND COALESCE("Returned Pet", 0) <> 1
                THEN "Pet ID"
            END) AS renewal_unique_pets
        FROM fact_written_details
        WHERE DATE_TRUNC('month', "Report Date")::date = %(selected_month)s::date
        GROUP BY 1
    )
    SELECT
        week_start,
        gwp,
        nbw_gwp,
        renewal_gwp,
        nbw_unique_pets,
        renewal_unique_pets,
        CASE
            WHEN nbw_unique_pets = 0 THEN NULL
            ELSE ROUND(nbw_gwp / nbw_unique_pets, 2)
        END AS avg_nbw_premium,
        CASE
            WHEN renewal_unique_pets = 0 THEN NULL
            ELSE ROUND(renewal_gwp / renewal_unique_pets, 2)
        END AS avg_renewal_premium,
        CASE
            WHEN gwp = 0 THEN NULL
            ELSE ROUND(nbw_gwp / gwp, 6)
        END AS nbw_mix_pct,
        CASE
            WHEN gwp = 0 THEN NULL
            ELSE ROUND(renewal_gwp / gwp, 6)
        END AS renewal_mix_pct
    FROM weekly_base
    ORDER BY week_start;
    """
    df = run_query(sql, {"selected_month": selected_month})

    if df.empty:
        return df

    df = df.sort_values("week_start").reset_index(drop=True)

    for col in ["gwp", "nbw_gwp", "renewal_gwp", "nbw_unique_pets", "renewal_unique_pets"]:
        df[f"{col}_wow_pct"] = df[col].pct_change()

    return df


def get_growth_drivers(selected_month: str) -> pd.DataFrame:
    sql = """
    WITH base AS (
        SELECT
            "IPH Channel (5)" AS iph_channel_5,
            SUM("Written Amount") AS total_gwp,
            SUM(CASE WHEN "TRANS CODE" = 'New Policy Line' THEN "Written Amount" ELSE 0 END) AS nbw_gwp,
            SUM(CASE WHEN "TRANS CODE" = 'Renew Policy' THEN "Written Amount" ELSE 0 END) AS renewal_gwp,
            COUNT(DISTINCT CASE
                WHEN "TRANS CODE" = 'New Policy Line'
                 AND COALESCE("Returned Pet", 0) <> 1
                THEN "Pet ID"
            END) AS nbw_unique_pets,
            COUNT(DISTINCT CASE
                WHEN "TRANS CODE" = 'Renew Policy'
                 AND COALESCE("Returned Pet", 0) <> 1
                THEN "Pet ID"
            END) AS renewal_unique_pets
        FROM fact_written_details
        WHERE DATE_TRUNC('month', "Report Date")::date = %(selected_month)s::date
        GROUP BY 1
    )
    SELECT
        iph_channel_5,
        ROUND(total_gwp, 2) AS total_gwp,
        ROUND(nbw_gwp, 2) AS nbw_gwp,
        ROUND(renewal_gwp, 2) AS renewal_gwp,
        nbw_unique_pets,
        renewal_unique_pets,
        CASE WHEN total_gwp = 0 THEN NULL ELSE ROUND(nbw_gwp / total_gwp, 6) END AS nbw_mix_pct,
        CASE WHEN total_gwp = 0 THEN NULL ELSE ROUND(renewal_gwp / total_gwp, 6) END AS renewal_mix_pct
    FROM base
    ORDER BY total_gwp DESC;
    """
    return run_query(sql, {"selected_month": selected_month})


def get_control_totals() -> Dict[str, Any]:
    sql = """
    SELECT 'fact_written_details' AS table_name, COUNT(*) AS row_count FROM fact_written_details
    UNION ALL
    SELECT 'fact_plan' AS table_name, COUNT(*) AS row_count FROM fact_plan
    UNION ALL
    SELECT 'fact_forecast' AS table_name, COUNT(*) AS row_count FROM fact_forecast
    UNION ALL
    SELECT 'dim_calendar' AS table_name, COUNT(*) AS row_count FROM dim_calendar;
    """
    df = run_query(sql)
    return {
        "table_counts": df,
        "source_tables": [
            "fact_written_details",
            "fact_plan",
            "fact_forecast",
            "dim_calendar",
            "vw_monthly_premium_kpis"
        ]
    }