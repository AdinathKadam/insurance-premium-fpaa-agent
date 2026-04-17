import argparse
import math
import os
from datetime import datetime

import numpy as np
import pandas as pd


OUTPUT_DIR = os.path.join("data", "synthetic")

ALL_STATES = [
    "AL", "AK", "AZ", "AR", "CA", "CO", "CT", "DE", "FL", "GA",
    "HI", "ID", "IL", "IN", "IA", "KS", "KY", "LA", "ME", "MD",
    "MA", "MI", "MN", "MS", "MO", "MT", "NE", "NV", "NH", "NJ",
    "NM", "NY", "NC", "ND", "OH", "OK", "OR", "PA", "RI", "SC",
    "SD", "TN", "TX", "UT", "VT", "VA", "WA", "WV", "WI", "WY"
]

CHANNELS = ["DTC", "Aggregator", "Veterinary", "Employee Benefits", "Partner", ""]
SUB_CHANNELS = [
    "Referral Program", "Paid Search", "Organic Search", "Vet Referrals",
    "Employer Direct", "Affinity Partner", "Comparison Site", "Direct Mail", ""
]
CHANNEL_TYPES = [
    "Digital", "Offline", "Veterinary Providers", "Employee Benefits",
    "Partner Network", "Referral", ""
]
HOSPITAL_AFFILIATES = [
    "North Star Animal Hospital", "Paw Health Network", "Metro Vet Group",
    "Healthy Pets Clinic", "Community Animal Care", "Independent Vet Partner", ""
]

CAMPAIGN_OWNERS = [
    "Marketing", "Partnerships", "Veterinary Growth",
    "Employee Benefits", "Digital Acquisition", "Retention"
]

COMPANIES = ["APIC", "IAIC"]
SPECIES = ["Dog", "Cat"]

BASE_PLANS = [
    "Accident Only",
    "Accident & Illness",
    "Complete Coverage",
    "Essential Care"
]

POLICY_LIMITS = ["5,000", "10,000", "Unlimited"]
LIMIT_TYPES = ["Annual", "Per Incident"]

TRANS_CODES = [
    "New Policy Line",
    "Renew Policy",
    "Mid Term Adjustment",
    "Cancel Policy Line",
    "Reinstate Policy Line"
]

CANCEL_CODES = [
    "",
    "Customer Request",
    "Non Payment",
    "Moved Carrier",
    "Coverage Not Needed",
    "Underwriting"
]

BREED_GROUPS = [
    "Small Breed",
    "Medium Breed",
    "Large Breed",
    "Mixed Breed",
    "Domestic Shorthair",
    "Domestic Longhair"
]


def ensure_dirs() -> None:
    os.makedirs(OUTPUT_DIR, exist_ok=True)


def random_dates(rng: np.random.Generator, start: str, end: str, size: int) -> np.ndarray:
    start_dt = np.datetime64(start)
    end_dt = np.datetime64(end)
    days = (end_dt - start_dt).astype(int)
    offsets = rng.integers(0, days + 1, size=size)
    return start_dt + offsets.astype("timedelta64[D]")


def derive_product(wellness: np.ndarray, accident: np.ndarray) -> np.ndarray:
    product = np.empty(len(wellness), dtype=object)

    product[(wellness == 0) & (accident == 0)] = "AI"
    product[(wellness == 0) & (accident == 1)] = "AO"
    product[(wellness == 1) & (accident == 0)] = "AI-WELL"
    product[(wellness == 1) & (accident == 1)] = "AO-WELL"

    return product


def map_iph_channel_5(channel: str, sub_channel: str, channel_type: str) -> str:
    if channel == "":
        return "DTC (non-Aggs)"

    if channel == "DTC":
        return "DTC"
    if channel == "Aggregator":
        return "Aggregator"
    if channel == "Veterinary":
        return "Vet"
    if channel == "Employee Benefits":
        return "Employee Benefits"
    if channel == "Partner":
        return "Partner"

    return "Other"


def map_iph_channel_6(channel: str, sub_channel: str, channel_type: str) -> str:
    if channel == "":
        return "DTC (non-Aggs) - Add"

    if channel == "DTC" and sub_channel == "Referral Program":
        return "Referred by Friend"
    if channel == "DTC":
        return "DTC Direct"
    if channel == "Aggregator":
        return "Aggregator"
    if channel == "Veterinary":
        return "Vet / All Other Vets"
    if channel == "Employee Benefits":
        return "EB Direct Bill"
    if channel == "Partner":
        return "Affinity / Partner"

    return "Other"


def map_vendor_5(channel: str, sub_channel: str, hospital: str) -> str:
    if channel == "Veterinary":
        return hospital if hospital else "All Other Vets"
    if channel == "Aggregator":
        return "Aggregator Marketplace"
    if channel == "DTC" and sub_channel == "Referral Program":
        return "Referred by Friend"
    if channel == "Employee Benefits":
        return "Employee Benefits"
    if channel == "Partner":
        return "Partner Network"

    return "DTC / Other"


def map_vendor_6(channel: str, sub_channel: str, hospital: str) -> str:
    if channel == "Veterinary":
        return hospital if hospital else "Vet / Other"
    if channel == "Aggregator":
        return sub_channel if sub_channel else "Aggregator"
    if channel == "DTC" and sub_channel == "Referral Program":
        return "Referred by Friend"
    if channel == "DTC":
        return "DTC"
    if channel == "Employee Benefits":
        return "EB Discount Sites"
    if channel == "Partner":
        return "Affinity Partner"

    return "Other"


def make_written_amount(
    rng: np.random.Generator,
    trans_code: np.ndarray,
    species: np.ndarray,
    product: np.ndarray,
    state: np.ndarray,
    size: int
) -> np.ndarray:
    base = rng.normal(loc=420, scale=95, size=size)

    dog_adj = np.where(species == "Dog", 1.12, 0.88)

    product_adj = np.select(
        [
            product == "AO",
            product == "AI",
            product == "AI-WELL",
            product == "AO-WELL"
        ],
        [
            0.78,
            1.00,
            1.18,
            1.32
        ],
        default=1.00
    )

    high_cost_state_adj = np.where(
        np.isin(state, ["CA", "NY", "NJ", "MA", "WA", "FL", "TX"]),
        1.10,
        1.00
    )

    amount = base * dog_adj * product_adj * high_cost_state_adj
    amount = np.maximum(amount, 85)

    amount = np.where(
        trans_code == "New Policy Line",
        amount * rng.normal(1.03, 0.06, size),
        amount
    )

    amount = np.where(
        trans_code == "Renew Policy",
        amount * rng.normal(1.08, 0.05, size),
        amount
    )

    amount = np.where(
        trans_code == "Mid Term Adjustment",
        amount * rng.normal(0.18, 0.12, size),
        amount
    )

    amount = np.where(
        trans_code == "Reinstate Policy Line",
        amount * rng.normal(0.45, 0.12, size),
        amount
    )

    amount = np.where(
        trans_code == "Cancel Policy Line",
        -amount * rng.normal(0.35, 0.15, size),
        amount
    )

    return np.round(amount, 2)


def generate_fact_written_details(total_rows: int, chunk_size: int, seed: int) -> None:
    rng = np.random.default_rng(seed)
    output_path = os.path.join(OUTPUT_DIR, "fact_written_details.csv")

    if os.path.exists(output_path):
        os.remove(output_path)

    periods = [
        ("2024-01-01", "2024-12-31", 0.38),
        ("2025-01-01", "2025-12-31", 0.42),
        ("2026-01-01", "2026-04-30", 0.20),
    ]

    first_write = True
    policy_counter = 1
    pet_universe = max(total_rows // 3, 1)

    total_written = 0

    for period_index, (start_date, end_date, weight) in enumerate(periods):
        if period_index < len(periods) - 1:
            period_rows = int(total_rows * weight)
        else:
            period_rows = total_rows - total_written

        total_written += period_rows
        chunks = math.ceil(period_rows / chunk_size)

        for chunk_idx in range(chunks):
            size = min(chunk_size, period_rows - chunk_idx * chunk_size)

            trans_date = random_dates(rng, start_date, end_date, size)
            report_date = trans_date + rng.integers(0, 5, size=size).astype("timedelta64[D]")

            first_start_date = trans_date - rng.integers(0, 365, size=size).astype("timedelta64[D]")
            cover_start_date = trans_date
            cover_end_date = trans_date + rng.integers(250, 370, size=size).astype("timedelta64[D]")

            policy_ids = np.array(
                [f"POL{policy_counter + i:010d}" for i in range(size)],
                dtype=object
            )
            pet_ids = np.array(
                [f"PET{rng.integers(1, pet_universe):010d}" for _ in range(size)],
                dtype=object
            )
            policy_counter += size

            state = rng.choice(ALL_STATES, size=size)
            channel = rng.choice(
                CHANNELS,
                size=size,
                p=[0.33, 0.18, 0.22, 0.12, 0.12, 0.03]
            )
            sub_channel = rng.choice(SUB_CHANNELS, size=size)
            channel_type = rng.choice(CHANNEL_TYPES, size=size)
            hospital_affiliate = rng.choice(HOSPITAL_AFFILIATES, size=size)

            species = rng.choice(SPECIES, size=size, p=[0.68, 0.32])
            breed_group = rng.choice(BREED_GROUPS, size=size)

            trans_code = rng.choice(
                TRANS_CODES,
                size=size,
                p=[0.24, 0.54, 0.11, 0.07, 0.04]
            )

            returned_pet = np.where(
                (trans_code == "Cancel Policy Line") & (rng.random(size) < 0.16),
                1,
                0
            )

            cancel_code = np.where(
                trans_code == "Cancel Policy Line",
                rng.choice(CANCEL_CODES[1:], size=size),
                ""
            )

            company = rng.choice(COMPANIES, size=size, p=[0.42, 0.58])
            uw_combined = np.where(company == "APIC", "APIC", "IAIC")

            wellness = rng.choice([0, 1], size=size, p=[0.72, 0.28])
            accident = rng.choice([0, 1], size=size, p=[0.18, 0.82])
            cancer = rng.choice([0, 1], size=size, p=[0.74, 0.26])
            feline = np.where(species == "Cat", 1, 0)
            exam_fees = rng.choice([0, 1], size=size, p=[0.52, 0.48])
            prescriptions = rng.choice([0, 1], size=size, p=[0.58, 0.42])
            pt_rehab = rng.choice([0, 1], size=size, p=[0.78, 0.22])
            holistic = rng.choice([0, 1], size=size, p=[0.82, 0.18])
            rac = rng.choice([0, 1], size=size, p=[0.88, 0.12])

            product = derive_product(wellness, accident)
            written_amount = make_written_amount(rng, trans_code, species, product, state, size)
            earned_premium = np.round(written_amount * rng.normal(0.92, 0.05, size=size), 2)

            base_plan = rng.choice(BASE_PLANS, size=size)
            policy_limit = rng.choice(POLICY_LIMITS, size=size)
            limit_type = rng.choice(LIMIT_TYPES, size=size)

            enrollment_age = np.round(rng.gamma(shape=2.1, scale=2.0, size=size), 1)
            enrollment_age = np.clip(enrollment_age, 0.1, 14.0)

            campaign_owner = rng.choice(CAMPAIGN_OWNERS, size=size)
            campaign_code = np.array(
                [f"CMP-{rng.integers(1000, 9999)}" for _ in range(size)],
                dtype=object
            )
            pet_line = rng.choice(
                ["Pet Insurance", "Wellness Add-On", "Accident Coverage"],
                size=size
            )

            report_month = pd.to_datetime(report_date).strftime("%B")
            first_report_month = pd.to_datetime(first_start_date).strftime("%B")
            match = np.where(report_month == first_report_month, "Yes", "No")

            concatenate = []
            iph_channel_5 = []
            iph_channel_6 = []
            vendor_5 = []
            vendor_6 = []

            for ch, sub, ctype, hosp in zip(channel, sub_channel, channel_type, hospital_affiliate):
                if ch == "":
                    concatenate.append("DTC (non-Aggs) - Add")
                else:
                    concatenate.append(f"{ch}{sub}{ctype}{hosp}")

                iph_channel_5.append(map_iph_channel_5(ch, sub, ctype))
                iph_channel_6.append(map_iph_channel_6(ch, sub, ctype))
                vendor_5.append(map_vendor_5(ch, sub, hosp))
                vendor_6.append(map_vendor_6(ch, sub, hosp))

            df = pd.DataFrame({
                "Policy Number": policy_ids,
                "Campaign Code": campaign_code,
                "Campaign Owner": campaign_owner,
                "Pet Line": pet_line,
                "TRANS DATE": pd.to_datetime(trans_date).date,
                "TRANS CODE": trans_code,
                "CANCEL CODE": cancel_code,
                "First Start Date": pd.to_datetime(first_start_date).date,
                "Cover Start Date": pd.to_datetime(cover_start_date).date,
                "Cover End Date": pd.to_datetime(cover_end_date).date,
                "Company": company,
                "U/W Combined": uw_combined,
                "State": state,
                "Written Amount": written_amount,
                "Earned Premium": earned_premium,
                "Species": species,
                "Enrollment Age": enrollment_age,
                "Base Plan": base_plan,
                "Policy Limit": policy_limit,
                "Limit Type": limit_type,
                "Wellness": wellness,
                "Accident": accident,
                "Cancer": cancer,
                "Feline": feline,
                "Exam Fees": exam_fees,
                "Prescriptions": prescriptions,
                "PT/Rehab": pt_rehab,
                "Holistic": holistic,
                "RAC": rac,
                "Pet ID": pet_ids,
                "Channel": channel,
                "Sub Channel": sub_channel,
                "Channel Type": channel_type,
                "Hospital Affiliate Name": hospital_affiliate,
                "Returned Pet": returned_pet,
                "Report Date": pd.to_datetime(report_date).date,
                "'Report Date' Month": report_month,
                "'First Report Date' Month": first_report_month,
                "Match": match,
                "Concatenate": concatenate,
                "PRODUCT": product,
                "IPH Channel (5)": iph_channel_5,
                "IPH Channel (6)": iph_channel_6,
                "Vendor (5)": vendor_5,
                "Vendor (6)": vendor_6,
            })

            df.to_csv(
                output_path,
                mode="w" if first_write else "a",
                index=False,
                header=first_write
            )

            first_write = False
            print(f"Written details chunk complete: {len(df):,} rows")

    print(f"Created {output_path}")


def generate_dim_calendar() -> None:
    dates = pd.date_range("2024-01-01", "2026-12-31", freq="D")

    df = pd.DataFrame({
        "date": dates.date,
        "week_start": (dates - pd.to_timedelta(dates.weekday, unit="D")).date,
        "month_start": dates.to_period("M").to_timestamp().date,
        "month_name": dates.strftime("%B"),
        "month_number": dates.month,
        "quarter": "Q" + dates.quarter.astype(str),
        "year": dates.year,
        "is_month_end": dates.is_month_end.astype(int),
    })

    output_path = os.path.join(OUTPUT_DIR, "dim_calendar.csv")
    df.to_csv(output_path, index=False)
    print(f"Created {output_path}")


def generate_plan_and_forecast(seed: int) -> None:
    rng = np.random.default_rng(seed + 99)

    plan_months = pd.date_range("2026-01-01", "2026-12-01", freq="MS")
    forecast_months = pd.date_range("2026-05-01", "2026-12-01", freq="MS")

    metrics = [
        "GWP",
        "NBW_GWP",
        "RENEWAL_GWP",
        "NBW_UNIQUE_PETS",
        "RENEWAL_UNIQUE_PETS",
        "AVG_NBW_PREMIUM",
        "AVG_RENEWAL_PREMIUM"
    ]

    dimension_sets = {
        "Total": ["All"],
        "State": ALL_STATES,
        "IPH Channel (5)": ["DTC", "Aggregator", "Vet", "Employee Benefits", "Partner", "DTC (non-Aggs)"],
        "IPH Channel (6)": ["DTC Direct", "Aggregator", "Vet / All Other Vets", "EB Direct Bill", "Affinity / Partner", "Referred by Friend"],
        "Vendor (5)": ["DTC / Other", "Aggregator Marketplace", "All Other Vets", "Employee Benefits", "Partner Network", "Referred by Friend"],
        "Vendor (6)": ["DTC", "Aggregator", "Vet / Other", "EB Discount Sites", "Affinity Partner", "Referred by Friend"],
        "PRODUCT": ["AI", "AO", "AI-WELL", "AO-WELL"],
        "Species": ["Dog", "Cat"]
    }

    def base_plan_amount(metric: str, month_index: int) -> float:
        seasonality = 1 + 0.08 * np.sin((month_index / 12) * 2 * np.pi)
        growth = 1 + (month_index * 0.012)

        if metric == "GWP":
            return 52_000_000 * seasonality * growth
        if metric == "NBW_GWP":
            return 17_000_000 * seasonality * growth
        if metric == "RENEWAL_GWP":
            return 35_000_000 * seasonality * growth
        if metric == "NBW_UNIQUE_PETS":
            return 38_000 * seasonality * growth
        if metric == "RENEWAL_UNIQUE_PETS":
            return 86_000 * seasonality * growth
        if metric == "AVG_NBW_PREMIUM":
            return 455 * (1 + month_index * 0.002)
        if metric == "AVG_RENEWAL_PREMIUM":
            return 505 * (1 + month_index * 0.002)

        return 0

    def dimension_weight(dimension_type: str, dimension_value: str) -> float:
        if dimension_type == "Total":
            return 1.0

        if dimension_type == "State":
            high = {"CA", "TX", "FL", "NY", "PA"}
            mid = {"IL", "OH", "GA", "NC", "AZ", "WA", "MI", "NJ", "VA", "MA"}

            if dimension_value in high:
                return rng.uniform(0.035, 0.065)
            if dimension_value in mid:
                return rng.uniform(0.018, 0.035)
            return rng.uniform(0.006, 0.018)

        if dimension_type == "IPH Channel (5)":
            weights = {
                "DTC": 0.30,
                "Aggregator": 0.18,
                "Vet": 0.25,
                "Employee Benefits": 0.12,
                "Partner": 0.12,
                "DTC (non-Aggs)": 0.03
            }
            return weights.get(dimension_value, 0.01)

        if dimension_type == "IPH Channel (6)":
            weights = {
                "DTC Direct": 0.23,
                "Aggregator": 0.18,
                "Vet / All Other Vets": 0.25,
                "EB Direct Bill": 0.12,
                "Affinity / Partner": 0.12,
                "Referred by Friend": 0.10
            }
            return weights.get(dimension_value, 0.01)

        if dimension_type == "Vendor (5)":
            weights = {
                "DTC / Other": 0.25,
                "Aggregator Marketplace": 0.18,
                "All Other Vets": 0.24,
                "Employee Benefits": 0.12,
                "Partner Network": 0.13,
                "Referred by Friend": 0.08
            }
            return weights.get(dimension_value, 0.01)

        if dimension_type == "Vendor (6)":
            weights = {
                "DTC": 0.24,
                "Aggregator": 0.18,
                "Vet / Other": 0.24,
                "EB Discount Sites": 0.12,
                "Affinity Partner": 0.13,
                "Referred by Friend": 0.09
            }
            return weights.get(dimension_value, 0.01)

        if dimension_type == "PRODUCT":
            weights = {
                "AI": 0.32,
                "AO": 0.18,
                "AI-WELL": 0.28,
                "AO-WELL": 0.22
            }
            return weights.get(dimension_value, 0.01)

        if dimension_type == "Species":
            weights = {
                "Dog": 0.68,
                "Cat": 0.32
            }
            return weights.get(dimension_value, 0.01)

        return 1.0

    plan_rows = []

    for month_idx, month in enumerate(plan_months, start=1):
        for metric in metrics:
            total_metric_plan = base_plan_amount(metric, month_idx)

            for dimension_type, values in dimension_sets.items():
                raw_weights = np.array([dimension_weight(dimension_type, v) for v in values], dtype=float)

                if dimension_type != "Total":
                    raw_weights = raw_weights / raw_weights.sum()
                else:
                    raw_weights = np.array([1.0])

                for dimension_value, weight in zip(values, raw_weights):
                    amount = total_metric_plan * weight
                    amount = amount * rng.normal(1.0, 0.015)

                    plan_rows.append({
                        "plan_month": month.date(),
                        "metric_name": metric,
                        "dimension_type": dimension_type,
                        "dimension_value": dimension_value,
                        "plan_amount": round(float(amount), 2)
                    })

    forecast_rows = []

    for month_idx, month in enumerate(forecast_months, start=5):
        for metric in metrics:
            total_metric_plan = base_plan_amount(metric, month_idx)
            forecast_bias = rng.normal(1.015, 0.035)
            total_metric_forecast = total_metric_plan * forecast_bias

            for dimension_type, values in dimension_sets.items():
                raw_weights = np.array([dimension_weight(dimension_type, v) for v in values], dtype=float)

                if dimension_type != "Total":
                    raw_weights = raw_weights / raw_weights.sum()
                else:
                    raw_weights = np.array([1.0])

                for dimension_value, weight in zip(values, raw_weights):
                    amount = total_metric_forecast * weight
                    amount = amount * rng.normal(1.0, 0.02)

                    forecast_rows.append({
                        "forecast_month": month.date(),
                        "metric_name": metric,
                        "dimension_type": dimension_type,
                        "dimension_value": dimension_value,
                        "forecast_amount": round(float(amount), 2)
                    })

    plan_df = pd.DataFrame(plan_rows)
    forecast_df = pd.DataFrame(forecast_rows)

    plan_path = os.path.join(OUTPUT_DIR, "fact_plan.csv")
    forecast_path = os.path.join(OUTPUT_DIR, "fact_forecast.csv")

    plan_df.to_csv(plan_path, index=False)
    forecast_df.to_csv(forecast_path, index=False)

    print(f"Created {plan_path} with {len(plan_df):,} rows")
    print(f"Created {forecast_path} with {len(forecast_df):,} rows")


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--rows", type=int, default=1_000_000)
    parser.add_argument("--chunk-size", type=int, default=100_000)
    parser.add_argument("--seed", type=int, default=42)
    args = parser.parse_args()

    ensure_dirs()

    generate_fact_written_details(
        total_rows=args.rows,
        chunk_size=args.chunk_size,
        seed=args.seed
    )
    generate_dim_calendar()
    generate_plan_and_forecast(seed=args.seed)

    print("Synthetic insurance Written Details data generation complete.")


if __name__ == "__main__":
    main()