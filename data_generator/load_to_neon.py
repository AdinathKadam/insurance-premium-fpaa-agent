import os
import csv

import psycopg2
from dotenv import load_dotenv


load_dotenv()

DB_HOST = os.getenv("DB_HOST")
DB_PORT = os.getenv("DB_PORT", "5432")
DB_NAME = os.getenv("DB_NAME")
DB_USER = os.getenv("DB_USER")
DB_PASSWORD = os.getenv("DB_PASSWORD")

DATA_DIR = os.path.join("data", "synthetic")


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


def create_tables(conn):
    ddl = """
    DROP VIEW IF EXISTS vw_monthly_premium_kpis;
    DROP TABLE IF EXISTS fact_written_details;
    DROP TABLE IF EXISTS fact_plan;
    DROP TABLE IF EXISTS fact_forecast;
    DROP TABLE IF EXISTS dim_calendar;

    CREATE TABLE fact_written_details (
        "Policy Number" TEXT,
        "Campaign Code" TEXT,
        "Campaign Owner" TEXT,
        "Pet Line" TEXT,
        "TRANS DATE" DATE,
        "TRANS CODE" TEXT,
        "CANCEL CODE" TEXT,
        "First Start Date" DATE,
        "Cover Start Date" DATE,
        "Cover End Date" DATE,
        "Company" TEXT,
        "U/W Combined" TEXT,
        "State" TEXT,
        "Written Amount" NUMERIC(18,2),
        "Earned Premium" NUMERIC(18,2),
        "Species" TEXT,
        "Enrollment Age" NUMERIC(10,2),
        "Base Plan" TEXT,
        "Policy Limit" TEXT,
        "Limit Type" TEXT,
        "Wellness" INTEGER,
        "Accident" INTEGER,
        "Cancer" INTEGER,
        "Feline" INTEGER,
        "Exam Fees" INTEGER,
        "Prescriptions" INTEGER,
        "PT/Rehab" INTEGER,
        "Holistic" INTEGER,
        "RAC" INTEGER,
        "Pet ID" TEXT,
        "Channel" TEXT,
        "Sub Channel" TEXT,
        "Channel Type" TEXT,
        "Hospital Affiliate Name" TEXT,
        "Returned Pet" INTEGER,
        "Report Date" DATE,
        "'Report Date' Month" TEXT,
        "'First Report Date' Month" TEXT,
        "Match" TEXT,
        "Concatenate" TEXT,
        "PRODUCT" TEXT,
        "IPH Channel (5)" TEXT,
        "IPH Channel (6)" TEXT,
        "Vendor (5)" TEXT,
        "Vendor (6)" TEXT
    );

    CREATE TABLE fact_plan (
        plan_month DATE,
        metric_name TEXT,
        dimension_type TEXT,
        dimension_value TEXT,
        plan_amount NUMERIC(18,2)
    );

    CREATE TABLE fact_forecast (
        forecast_month DATE,
        metric_name TEXT,
        dimension_type TEXT,
        dimension_value TEXT,
        forecast_amount NUMERIC(18,2)
    );

    CREATE TABLE dim_calendar (
        date DATE,
        week_start DATE,
        month_start DATE,
        month_name TEXT,
        month_number INTEGER,
        quarter TEXT,
        year INTEGER,
        is_month_end INTEGER
    );
    """

    with conn.cursor() as cur:
        cur.execute(ddl)

    conn.commit()
    print("Created tables")


def copy_csv(conn, table_name: str, csv_file: str):
    path = os.path.join(DATA_DIR, csv_file)

    if not os.path.exists(path):
        raise FileNotFoundError(f"Missing file: {path}")

    with open(path, "r", encoding="utf-8", newline="") as f:
        reader = csv.reader(f)
        headers = next(reader)

    quoted_headers = ", ".join([f'"{h}"' for h in headers])

    sql = f"""
    COPY {table_name} ({quoted_headers})
    FROM STDIN
    WITH CSV HEADER
    """

    with conn.cursor() as cur:
        with open(path, "r", encoding="utf-8") as f:
            cur.copy_expert(sql, f)

    conn.commit()
    print(f"Loaded {csv_file} into {table_name}")


def create_indexes_and_views(conn):
    sql = """
    CREATE INDEX IF NOT EXISTS idx_fwd_report_date
        ON fact_written_details ("Report Date");

    CREATE INDEX IF NOT EXISTS idx_fwd_trans_code
        ON fact_written_details ("TRANS CODE");

    CREATE INDEX IF NOT EXISTS idx_fwd_state
        ON fact_written_details ("State");

    CREATE INDEX IF NOT EXISTS idx_fwd_pet_id
        ON fact_written_details ("Pet ID");

    CREATE INDEX IF NOT EXISTS idx_fwd_product
        ON fact_written_details ("PRODUCT");

    CREATE INDEX IF NOT EXISTS idx_fwd_channel5
        ON fact_written_details ("IPH Channel (5)");

    CREATE INDEX IF NOT EXISTS idx_plan_month_metric
        ON fact_plan (plan_month, metric_name, dimension_type);

    CREATE INDEX IF NOT EXISTS idx_forecast_month_metric
        ON fact_forecast (forecast_month, metric_name, dimension_type);

    CREATE VIEW vw_monthly_premium_kpis AS
    SELECT
        DATE_TRUNC('month', "Report Date")::date AS month_start,

        SUM("Written Amount") AS gwp,

        SUM(CASE
            WHEN "TRANS CODE" = 'New Policy Line'
            THEN "Written Amount" ELSE 0
        END) AS nbw_gwp,

        SUM(CASE
            WHEN "TRANS CODE" = 'Renew Policy'
            THEN "Written Amount" ELSE 0
        END) AS renewal_gwp,

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
            END) = 0
            THEN NULL
            ELSE
                SUM(CASE
                    WHEN "TRANS CODE" = 'New Policy Line'
                    THEN "Written Amount" ELSE 0
                END)
                / COUNT(DISTINCT CASE
                    WHEN "TRANS CODE" = 'New Policy Line'
                     AND COALESCE("Returned Pet", 0) <> 1
                    THEN "Pet ID"
                END)
        END AS avg_nbw_premium,

        CASE
            WHEN COUNT(DISTINCT CASE
                WHEN "TRANS CODE" = 'Renew Policy'
                 AND COALESCE("Returned Pet", 0) <> 1
                THEN "Pet ID"
            END) = 0
            THEN NULL
            ELSE
                SUM(CASE
                    WHEN "TRANS CODE" = 'Renew Policy'
                    THEN "Written Amount" ELSE 0
                END)
                / COUNT(DISTINCT CASE
                    WHEN "TRANS CODE" = 'Renew Policy'
                     AND COALESCE("Returned Pet", 0) <> 1
                    THEN "Pet ID"
                END)
        END AS avg_renewal_premium,

        COUNT(*) AS row_count
    FROM fact_written_details
    GROUP BY 1;
    """

    with conn.cursor() as cur:
        cur.execute(sql)

    conn.commit()
    print("Created indexes and vw_monthly_premium_kpis")


def verify_load(conn):
    sql = """
    SELECT 'fact_written_details' AS table_name, COUNT(*) AS row_count FROM fact_written_details
    UNION ALL
    SELECT 'fact_plan' AS table_name, COUNT(*) AS row_count FROM fact_plan
    UNION ALL
    SELECT 'fact_forecast' AS table_name, COUNT(*) AS row_count FROM fact_forecast
    UNION ALL
    SELECT 'dim_calendar' AS table_name, COUNT(*) AS row_count FROM dim_calendar;
    """

    with conn.cursor() as cur:
        cur.execute(sql)
        rows = cur.fetchall()

    print("\nLoad verification:")
    for table_name, row_count in rows:
        print(f"- {table_name}: {row_count:,} rows")


def main():
    conn = get_connection()

    try:
        create_tables(conn)

        copy_csv(conn, "fact_written_details", "fact_written_details.csv")
        copy_csv(conn, "fact_plan", "fact_plan.csv")
        copy_csv(conn, "fact_forecast", "fact_forecast.csv")
        copy_csv(conn, "dim_calendar", "dim_calendar.csv")

        create_indexes_and_views(conn)
        verify_load(conn)

    finally:
        conn.close()


if __name__ == "__main__":
    main()