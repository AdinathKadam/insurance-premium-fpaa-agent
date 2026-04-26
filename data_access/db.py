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


def get_date_bounds() -> pd.DataFrame:
    sql = """
    SELECT
        MIN("Report Date")::date AS min_date,
        MAX("Report Date")::date AS max_date
    FROM fact_written_details;
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


def _pct_change(current, base):
    if base is None or pd.isna(base) or base == 0:
        return None
    return (current - base) / base


def _get_vendor_column_expr() -> str:
    sql = """
    SELECT column_name
    FROM information_schema.columns
    WHERE table_name = 'fact_written_details'
      AND column_name IN ('Vendor (5)', 'Vendor (6)', 'Vendor')
    ORDER BY
        CASE
            WHEN column_name = 'Vendor (5)' THEN 1
            WHEN column_name = 'Vendor (6)' THEN 2
            WHEN column_name = 'Vendor' THEN 3
            ELSE 4
        END
    LIMIT 1;
    """
    df = run_query(sql)
    if df.empty:
        raise ValueError(
            "No vendor column found in fact_written_details. Expected one of: Vendor (5), Vendor (6), Vendor."
        )

    col = df.iloc[0]["column_name"]
    return f'"{col}"'


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

    row["gwp_mom_pct"] = _pct_change(row["gwp"], row["prev_gwp"])
    row["gwp_yoy_pct"] = _pct_change(row["gwp"], row["py_gwp"])

    if selected_year == 2026:
        row["gwp_vs_plan_pct"] = _pct_change(row["gwp"], row["plan_gwp"])
        row["nbw_vs_plan_pct"] = _pct_change(row["nbw_gwp"], row["plan_nbw_gwp"])
        row["renewal_vs_plan_pct"] = _pct_change(row["renewal_gwp"], row["plan_renewal_gwp"])
        row["nbw_unique_pets_vs_plan_pct"] = _pct_change(row["nbw_unique_pets"], row["plan_nbw_unique_pets"])
        row["renewal_unique_pets_vs_plan_pct"] = _pct_change(row["renewal_unique_pets"], row["plan_renewal_unique_pets"])
        row["avg_nbw_vs_plan_pct"] = _pct_change(row["avg_nbw_premium"], row["plan_avg_nbw_premium"])
        row["avg_renewal_vs_plan_pct"] = _pct_change(row["avg_renewal_premium"], row["plan_avg_renewal_premium"])
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


def get_kpi_snapshot_range(start_date: str, end_date: str) -> Dict[str, Any]:
    sql = """
    WITH actual AS (
        SELECT
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
        WHERE "Report Date"::date BETWEEN %(start_date)s::date AND %(end_date)s::date
    ),
    plan AS (
        SELECT
            SUM(CASE WHEN metric_name = 'GWP' THEN plan_amount ELSE 0 END) AS plan_gwp,
            SUM(CASE WHEN metric_name = 'NBW_GWP' THEN plan_amount ELSE 0 END) AS plan_nbw_gwp,
            SUM(CASE WHEN metric_name = 'RENEWAL_GWP' THEN plan_amount ELSE 0 END) AS plan_renewal_gwp,
            SUM(CASE WHEN metric_name = 'NBW_UNIQUE_PETS' THEN plan_amount ELSE 0 END) AS plan_nbw_unique_pets,
            SUM(CASE WHEN metric_name = 'RENEWAL_UNIQUE_PETS' THEN plan_amount ELSE 0 END) AS plan_renewal_unique_pets
        FROM fact_plan
        WHERE dimension_type = 'Total'
          AND plan_month::date BETWEEN DATE_TRUNC('month', %(start_date)s::date)::date
                                  AND DATE_TRUNC('month', %(end_date)s::date)::date
    ),
    forecast AS (
        SELECT
            SUM(CASE WHEN metric_name = 'GWP' THEN forecast_amount ELSE 0 END) AS forecast_gwp,
            SUM(CASE WHEN metric_name = 'NBW_GWP' THEN forecast_amount ELSE 0 END) AS forecast_nbw_gwp,
            SUM(CASE WHEN metric_name = 'RENEWAL_GWP' THEN forecast_amount ELSE 0 END) AS forecast_renewal_gwp
        FROM fact_forecast
        WHERE dimension_type = 'Total'
          AND forecast_month::date BETWEEN DATE_TRUNC('month', %(start_date)s::date)::date
                                      AND DATE_TRUNC('month', %(end_date)s::date)::date
    )
    SELECT
        a.gwp,
        a.nbw_gwp,
        a.renewal_gwp,
        a.nbw_unique_pets,
        a.renewal_unique_pets,

        CASE WHEN a.nbw_unique_pets = 0 THEN NULL ELSE ROUND(a.nbw_gwp / a.nbw_unique_pets, 2) END AS avg_nbw_premium,
        CASE WHEN a.renewal_unique_pets = 0 THEN NULL ELSE ROUND(a.renewal_gwp / a.renewal_unique_pets, 2) END AS avg_renewal_premium,

        p.plan_gwp,
        p.plan_nbw_gwp,
        p.plan_renewal_gwp,
        p.plan_nbw_unique_pets,
        p.plan_renewal_unique_pets,

        f.forecast_gwp,
        f.forecast_nbw_gwp,
        f.forecast_renewal_gwp
    FROM actual a
    CROSS JOIN plan p
    CROSS JOIN forecast f;
    """

    df = run_query(sql, {"start_date": start_date, "end_date": end_date})

    if df.empty:
        return {}

    row = df.iloc[0].to_dict()

    row["gwp_vs_plan_pct"] = _pct_change(row.get("gwp"), row.get("plan_gwp"))
    row["nbw_vs_plan_pct"] = _pct_change(row.get("nbw_gwp"), row.get("plan_nbw_gwp"))
    row["renewal_vs_plan_pct"] = _pct_change(row.get("renewal_gwp"), row.get("plan_renewal_gwp"))
    row["nbw_unique_pets_vs_plan_pct"] = _pct_change(row.get("nbw_unique_pets"), row.get("plan_nbw_unique_pets"))
    row["renewal_unique_pets_vs_plan_pct"] = _pct_change(row.get("renewal_unique_pets"), row.get("plan_renewal_unique_pets"))

    total_unique_pets = (row.get("nbw_unique_pets") or 0) + (row.get("renewal_unique_pets") or 0)
    row["renewal_pet_share"] = ((row.get("renewal_unique_pets") or 0) / total_unique_pets) if total_unique_pets > 0 else None
    row["nbw_mix_pct"] = ((row.get("nbw_gwp") or 0) / row["gwp"]) if row.get("gwp") else None
    row["renewal_mix_pct"] = ((row.get("renewal_gwp") or 0) / row["gwp"]) if row.get("gwp") else None

    row["gwp_mom_pct"] = None
    row["gwp_yoy_pct"] = None
    row["avg_nbw_vs_plan_pct"] = None
    row["avg_renewal_vs_plan_pct"] = None

    return row


def get_ytd_snapshot(selected_month: str) -> Dict[str, Any]:
    sql = """
    WITH params AS (
        SELECT
            %(selected_month)s::date AS selected_month,
            DATE_TRUNC('year', %(selected_month)s::date)::date AS ytd_start,
            (%(selected_month)s::date + INTERVAL '1 month' - INTERVAL '1 day')::date AS ytd_end,
            (DATE_TRUNC('year', %(selected_month)s::date) - INTERVAL '1 year')::date AS py_ytd_start,
            ((%(selected_month)s::date + INTERVAL '1 month' - INTERVAL '1 day') - INTERVAL '1 year')::date AS py_ytd_end
    ),
    actual_ytd AS (
        SELECT
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
        FROM fact_written_details f
        CROSS JOIN params p
        WHERE f."Report Date"::date BETWEEN p.ytd_start AND p.ytd_end
    ),
    py_ytd AS (
        SELECT
            SUM("Written Amount") AS py_gwp,
            SUM(CASE WHEN "TRANS CODE" = 'New Policy Line' THEN "Written Amount" ELSE 0 END) AS py_nbw_gwp,
            SUM(CASE WHEN "TRANS CODE" = 'Renew Policy' THEN "Written Amount" ELSE 0 END) AS py_renewal_gwp
        FROM fact_written_details f
        CROSS JOIN params p
        WHERE f."Report Date"::date BETWEEN p.py_ytd_start AND p.py_ytd_end
    ),
    plan_ytd AS (
        SELECT
            SUM(CASE WHEN metric_name = 'GWP' THEN plan_amount END) AS plan_gwp,
            SUM(CASE WHEN metric_name = 'NBW_GWP' THEN plan_amount END) AS plan_nbw_gwp,
            SUM(CASE WHEN metric_name = 'RENEWAL_GWP' THEN plan_amount END) AS plan_renewal_gwp
        FROM fact_plan fp
        CROSS JOIN params p
        WHERE fp.dimension_type = 'Total'
          AND fp.plan_month::date BETWEEN p.ytd_start AND p.ytd_end
    )
    SELECT
        a.gwp,
        a.nbw_gwp,
        a.renewal_gwp,
        a.nbw_unique_pets,
        a.renewal_unique_pets,
        CASE
            WHEN a.nbw_unique_pets = 0 THEN NULL
            ELSE ROUND(a.nbw_gwp / a.nbw_unique_pets, 2)
        END AS avg_nbw_premium,
        CASE
            WHEN a.renewal_unique_pets = 0 THEN NULL
            ELSE ROUND(a.renewal_gwp / a.renewal_unique_pets, 2)
        END AS avg_renewal_premium,
        py.py_gwp,
        py.py_nbw_gwp,
        py.py_renewal_gwp,
        p.plan_gwp,
        p.plan_nbw_gwp,
        p.plan_renewal_gwp
    FROM actual_ytd a
    CROSS JOIN py_ytd py
    CROSS JOIN plan_ytd p;
    """

    df = run_query(sql, {"selected_month": selected_month})

    if df.empty:
        return {}

    row = df.iloc[0].to_dict()
    selected_year = pd.to_datetime(selected_month).year

    row["gwp_yoy_pct"] = _pct_change(row["gwp"], row["py_gwp"])

    if selected_year == 2026:
        row["gwp_vs_plan_pct"] = _pct_change(row["gwp"], row["plan_gwp"])
        row["nbw_vs_plan_pct"] = _pct_change(row["nbw_gwp"], row["plan_nbw_gwp"])
        row["renewal_vs_plan_pct"] = _pct_change(row["renewal_gwp"], row["plan_renewal_gwp"])
    else:
        row["gwp_vs_plan_pct"] = None
        row["nbw_vs_plan_pct"] = None
        row["renewal_vs_plan_pct"] = None

    total_unique_pets = (row.get("nbw_unique_pets") or 0) + (row.get("renewal_unique_pets") or 0)
    row["renewal_pet_share"] = ((row.get("renewal_unique_pets") or 0) / total_unique_pets) if total_unique_pets > 0 else None

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


def get_weekly_performance_range(start_date: str, end_date: str) -> pd.DataFrame:
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
        WHERE "Report Date"::date BETWEEN %(start_date)s::date AND %(end_date)s::date
        GROUP BY 1
    )
    SELECT
        week_start,
        gwp,
        nbw_gwp,
        renewal_gwp,
        nbw_unique_pets,
        renewal_unique_pets,
        CASE WHEN nbw_unique_pets = 0 THEN NULL ELSE ROUND(nbw_gwp / nbw_unique_pets, 2) END AS avg_nbw_premium,
        CASE WHEN renewal_unique_pets = 0 THEN NULL ELSE ROUND(renewal_gwp / renewal_unique_pets, 2) END AS avg_renewal_premium,
        CASE WHEN gwp = 0 THEN NULL ELSE ROUND(nbw_gwp / gwp, 6) END AS nbw_mix_pct,
        CASE WHEN gwp = 0 THEN NULL ELSE ROUND(renewal_gwp / gwp, 6) END AS renewal_mix_pct
    FROM weekly_base
    ORDER BY week_start;
    """

    df = run_query(sql, {"start_date": start_date, "end_date": end_date})

    if df.empty:
        return df

    df = df.sort_values("week_start").reset_index(drop=True)

    for col in ["gwp", "nbw_gwp", "renewal_gwp", "nbw_unique_pets", "renewal_unique_pets"]:
        df[f"{col}_wow_pct"] = df[col].pct_change()

    return df


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


def get_top_states_range(start_date: str, end_date: str, top_n: int = 10) -> pd.DataFrame:
    sql = """
    WITH curr AS (
        SELECT
            "State" AS state,
            SUM("Written Amount") AS current_gwp
        FROM fact_written_details
        WHERE "Report Date"::date BETWEEN %(start_date)s::date AND %(end_date)s::date
        GROUP BY 1
    ),
    total_curr AS (
        SELECT SUM(current_gwp) AS total_gwp FROM curr
    )
    SELECT
        c.state,
        ROUND(c.current_gwp, 2) AS current_gwp,
        NULL::numeric AS py_gwp,
        NULL::numeric AS yoy_pct,
        ROUND(c.current_gwp / NULLIF(t.total_gwp, 0), 6) AS share_pct
    FROM curr c
    CROSS JOIN total_curr t
    ORDER BY c.current_gwp DESC
    LIMIT %(top_n)s;
    """
    return run_query(sql, {"start_date": start_date, "end_date": end_date, "top_n": top_n})


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


def get_channel_mix_range(start_date: str, end_date: str) -> pd.DataFrame:
    sql = """
    WITH base AS (
        SELECT
            "IPH Channel (5)" AS iph_channel_5,
            SUM("Written Amount") AS gwp
        FROM fact_written_details
        WHERE "Report Date"::date BETWEEN %(start_date)s::date AND %(end_date)s::date
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
    return run_query(sql, {"start_date": start_date, "end_date": end_date})


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


def get_product_mix_range(start_date: str, end_date: str) -> pd.DataFrame:
    sql = """
    WITH base AS (
        SELECT
            "PRODUCT" AS product,
            SUM("Written Amount") AS gwp
        FROM fact_written_details
        WHERE "Report Date"::date BETWEEN %(start_date)s::date AND %(end_date)s::date
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
    return run_query(sql, {"start_date": start_date, "end_date": end_date})


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


def get_actual_plan_forecast_range(start_date: str, end_date: str) -> pd.DataFrame:
    sql = """
    WITH params AS (
        SELECT
            %(start_date)s::date AS start_date,
            %(end_date)s::date AS end_date,
            DATE_TRUNC('month', %(start_date)s::date)::date AS start_month,
            DATE_TRUNC('month', %(end_date)s::date)::date AS end_month
    ),
    actual AS (
        SELECT
            DATE_TRUNC('month', f."Report Date")::date AS month_start,
            SUM(f."Written Amount") AS actual_gwp,
            SUM(CASE WHEN f."TRANS CODE" = 'New Policy Line' THEN f."Written Amount" ELSE 0 END) AS actual_nbw_gwp,
            SUM(CASE WHEN f."TRANS CODE" = 'Renew Policy' THEN f."Written Amount" ELSE 0 END) AS actual_renewal_gwp,
            COUNT(DISTINCT CASE
                WHEN f."TRANS CODE" = 'New Policy Line'
                 AND COALESCE(f."Returned Pet", 0) <> 1
                THEN f."Pet ID"
            END) AS actual_nbw_unique_pets,
            COUNT(DISTINCT CASE
                WHEN f."TRANS CODE" = 'Renew Policy'
                 AND COALESCE(f."Returned Pet", 0) <> 1
                THEN f."Pet ID"
            END) AS actual_renewal_unique_pets
        FROM fact_written_details f
        CROSS JOIN params p
        WHERE f."Report Date"::date BETWEEN p.start_date AND p.end_date
        GROUP BY 1
    ),
    plan AS (
        SELECT
            fp.plan_month::date AS month_start,
            SUM(CASE WHEN fp.metric_name = 'GWP' THEN fp.plan_amount ELSE 0 END) AS plan_gwp,
            SUM(CASE WHEN fp.metric_name = 'NBW_GWP' THEN fp.plan_amount ELSE 0 END) AS plan_nbw_gwp,
            SUM(CASE WHEN fp.metric_name = 'RENEWAL_GWP' THEN fp.plan_amount ELSE 0 END) AS plan_renewal_gwp,
            SUM(CASE WHEN fp.metric_name = 'NBW_UNIQUE_PETS' THEN fp.plan_amount ELSE 0 END) AS plan_nbw_unique_pets,
            SUM(CASE WHEN fp.metric_name = 'RENEWAL_UNIQUE_PETS' THEN fp.plan_amount ELSE 0 END) AS plan_renewal_unique_pets
        FROM fact_plan fp
        CROSS JOIN params p
        WHERE fp.dimension_type = 'Total'
          AND fp.plan_month::date BETWEEN p.start_month AND p.end_month
        GROUP BY 1
    ),
    forecast AS (
        SELECT
            ff.forecast_month::date AS month_start,
            SUM(CASE WHEN ff.metric_name = 'GWP' THEN ff.forecast_amount ELSE 0 END) AS forecast_gwp,
            SUM(CASE WHEN ff.metric_name = 'NBW_GWP' THEN ff.forecast_amount ELSE 0 END) AS forecast_nbw_gwp,
            SUM(CASE WHEN ff.metric_name = 'RENEWAL_GWP' THEN ff.forecast_amount ELSE 0 END) AS forecast_renewal_gwp,
            SUM(CASE WHEN ff.metric_name = 'NBW_UNIQUE_PETS' THEN ff.forecast_amount ELSE 0 END) AS forecast_nbw_unique_pets,
            SUM(CASE WHEN ff.metric_name = 'RENEWAL_UNIQUE_PETS' THEN ff.forecast_amount ELSE 0 END) AS forecast_renewal_unique_pets
        FROM fact_forecast ff
        CROSS JOIN params p
        WHERE ff.dimension_type = 'Total'
          AND ff.forecast_month::date BETWEEN p.start_month AND p.end_month
        GROUP BY 1
    ),
    months AS (
        SELECT month_start FROM actual
        UNION
        SELECT month_start FROM plan
        UNION
        SELECT month_start FROM forecast
    )
    SELECT
        m.month_start,
        ROUND(COALESCE(a.actual_gwp, 0), 2) AS actual_gwp,
        ROUND(COALESCE(a.actual_nbw_gwp, 0), 2) AS actual_nbw_gwp,
        ROUND(COALESCE(a.actual_renewal_gwp, 0), 2) AS actual_renewal_gwp,
        COALESCE(a.actual_nbw_unique_pets, 0) AS actual_nbw_unique_pets,
        COALESCE(a.actual_renewal_unique_pets, 0) AS actual_renewal_unique_pets,
        ROUND(COALESCE(p.plan_gwp, 0), 2) AS plan_gwp,
        ROUND(COALESCE(p.plan_nbw_gwp, 0), 2) AS plan_nbw_gwp,
        ROUND(COALESCE(p.plan_renewal_gwp, 0), 2) AS plan_renewal_gwp,
        COALESCE(p.plan_nbw_unique_pets, 0) AS plan_nbw_unique_pets,
        COALESCE(p.plan_renewal_unique_pets, 0) AS plan_renewal_unique_pets,
        ROUND(COALESCE(f.forecast_gwp, 0), 2) AS forecast_gwp,
        ROUND(COALESCE(f.forecast_nbw_gwp, 0), 2) AS forecast_nbw_gwp,
        ROUND(COALESCE(f.forecast_renewal_gwp, 0), 2) AS forecast_renewal_gwp,
        COALESCE(f.forecast_nbw_unique_pets, 0) AS forecast_nbw_unique_pets,
        COALESCE(f.forecast_renewal_unique_pets, 0) AS forecast_renewal_unique_pets,
        ROUND(COALESCE(a.actual_gwp, 0) + COALESCE(f.forecast_gwp, 0), 2) AS actual_plus_forecast_gwp,
        ROUND(COALESCE(a.actual_gwp, 0) - COALESCE(p.plan_gwp, 0), 2) AS actual_vs_plan_gwp,
        CASE
            WHEN COALESCE(p.plan_gwp, 0) = 0 THEN NULL
            ELSE ROUND((COALESCE(a.actual_gwp, 0) - COALESCE(p.plan_gwp, 0)) / p.plan_gwp, 6)
        END AS actual_vs_plan_pct
    FROM months m
    LEFT JOIN actual a ON m.month_start = a.month_start
    LEFT JOIN plan p ON m.month_start = p.month_start
    LEFT JOIN forecast f ON m.month_start = f.month_start
    ORDER BY m.month_start;
    """

    return run_query(sql, {"start_date": start_date, "end_date": end_date})


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


def get_drilldown_table(start_date: str, end_date: str, group_by: str) -> pd.DataFrame:
    vendor_expr = _get_vendor_column_expr()

    group_map = {
        "Week": 'DATE_TRUNC(\'week\', "Report Date")::date',
        "State": '"State"',
        "Channel": '"IPH Channel (5)"',
        "Vendor": vendor_expr,
        "Product": '"PRODUCT"',
    }

    if group_by not in group_map:
        raise ValueError(f"Invalid drilldown group: {group_by}")

    group_expr = group_map[group_by]

    sql = f"""
    SELECT
        {group_expr} AS drilldown_group,
        ROUND(SUM("Written Amount"), 2) AS actual_gwp,
        ROUND(SUM(CASE WHEN "TRANS CODE" = 'New Policy Line' THEN "Written Amount" ELSE 0 END), 2) AS nbw_gwp,
        ROUND(SUM(CASE WHEN "TRANS CODE" = 'Renew Policy' THEN "Written Amount" ELSE 0 END), 2) AS renewal_gwp,
        COUNT(DISTINCT CASE
            WHEN "TRANS CODE" = 'New Policy Line'
             AND COALESCE("Returned Pet", 0) <> 1
            THEN "Pet ID"
        END) AS nbw_unique_pets,
        COUNT(DISTINCT CASE
            WHEN "TRANS CODE" = 'Renew Policy'
             AND COALESCE("Returned Pet", 0) <> 1
            THEN "Pet ID"
        END) AS renewal_unique_pets,
        CASE
            WHEN COUNT(DISTINCT CASE
                WHEN "TRANS CODE" = 'New Policy Line'
                 AND COALESCE("Returned Pet", 0) <> 1
                THEN "Pet ID"
            END) = 0 THEN NULL
            ELSE ROUND(
                SUM(CASE WHEN "TRANS CODE" = 'New Policy Line' THEN "Written Amount" ELSE 0 END)
                / COUNT(DISTINCT CASE
                    WHEN "TRANS CODE" = 'New Policy Line'
                     AND COALESCE("Returned Pet", 0) <> 1
                    THEN "Pet ID"
                END),
                2
            )
        END AS avg_nbw_premium,
        CASE
            WHEN COUNT(DISTINCT CASE
                WHEN "TRANS CODE" = 'Renew Policy'
                 AND COALESCE("Returned Pet", 0) <> 1
                THEN "Pet ID"
            END) = 0 THEN NULL
            ELSE ROUND(
                SUM(CASE WHEN "TRANS CODE" = 'Renew Policy' THEN "Written Amount" ELSE 0 END)
                / COUNT(DISTINCT CASE
                    WHEN "TRANS CODE" = 'Renew Policy'
                     AND COALESCE("Returned Pet", 0) <> 1
                    THEN "Pet ID"
                END),
                2
            )
        END AS avg_renewal_premium
    FROM fact_written_details
    WHERE "Report Date"::date BETWEEN %(start_date)s::date AND %(end_date)s::date
    GROUP BY 1
    ORDER BY actual_gwp DESC;
    """

    return run_query(sql, {"start_date": start_date, "end_date": end_date})


def get_nbw_renewal_drilldown_range(start_date: str, end_date: str, metric: str, group_by: str) -> pd.DataFrame:
    vendor_expr = _get_vendor_column_expr()

    group_map = {
        "Week": 'DATE_TRUNC(\'week\', "Report Date")::date',
        "State": '"State"',
        "Channel": '"IPH Channel (5)"',
        "Vendor": vendor_expr,
        "Product": '"PRODUCT"',
    }

    if group_by not in group_map:
        raise ValueError(f"Invalid group_by: {group_by}")

    group_expr = group_map[group_by]

    if metric == "NBW":
        amount_case = """CASE WHEN "TRANS CODE" = 'New Policy Line' THEN "Written Amount" ELSE 0 END"""
        pet_case = """CASE WHEN "TRANS CODE" = 'New Policy Line' AND COALESCE("Returned Pet", 0) <> 1 THEN "Pet ID" END"""
    elif metric == "Renewal":
        amount_case = """CASE WHEN "TRANS CODE" = 'Renew Policy' THEN "Written Amount" ELSE 0 END"""
        pet_case = """CASE WHEN "TRANS CODE" = 'Renew Policy' AND COALESCE("Returned Pet", 0) <> 1 THEN "Pet ID" END"""
    else:
        amount_case = """"Written Amount" """
        pet_case = """CASE WHEN COALESCE("Returned Pet", 0) <> 1 THEN "Pet ID" END"""

    sql = f"""
    WITH base AS (
        SELECT
            {group_expr} AS drilldown_group,
            SUM({amount_case}) AS gwp,
            COUNT(DISTINCT {pet_case}) AS unique_pets
        FROM fact_written_details
        WHERE "Report Date"::date BETWEEN %(start_date)s::date AND %(end_date)s::date
        GROUP BY 1
    ),
    total_base AS (
        SELECT SUM(gwp) AS total_gwp FROM base
    )
    SELECT
        drilldown_group,
        ROUND(gwp, 2) AS gwp,
        unique_pets,
        CASE WHEN unique_pets = 0 THEN NULL ELSE ROUND(gwp / unique_pets, 2) END AS avg_premium,
        ROUND(gwp / NULLIF(total_gwp, 0), 6) AS share_pct
    FROM base
    CROSS JOIN total_base
    ORDER BY gwp DESC;
    """

    return run_query(sql, {"start_date": start_date, "end_date": end_date})


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


def get_growth_drivers_range(start_date: str, end_date: str) -> pd.DataFrame:
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
        WHERE "Report Date"::date BETWEEN %(start_date)s::date AND %(end_date)s::date
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
    return run_query(sql, {"start_date": start_date, "end_date": end_date})


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