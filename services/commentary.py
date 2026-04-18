from typing import Dict, Any, Tuple

import pandas as pd

from services.bedrock_client import generate_bedrock_text


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


def _top_states_text(top_states: pd.DataFrame, limit: int = 5) -> str:
    if top_states is None or top_states.empty:
        return "No state data available."

    rows = top_states.head(limit)
    lines = []

    for _, r in rows.iterrows():
        lines.append(
            f"- {r['state']}: current GWP {fmt_currency(r['current_gwp'])}, "
            f"YoY {fmt_pct(r['yoy_pct'])}, share {fmt_pct(r['share_pct'])}"
        )

    return "\n".join(lines)


def _channel_mix_text(channel_mix: pd.DataFrame, limit: int = 5) -> str:
    if channel_mix is None or channel_mix.empty:
        return "No channel mix data available."

    rows = channel_mix.head(limit)
    lines = []

    for _, r in rows.iterrows():
        lines.append(
            f"- {r['iph_channel_5']}: GWP {fmt_currency(r['gwp'])}, share {fmt_pct(r['share_pct'])}"
        )

    return "\n".join(lines)


def _product_mix_text(product_mix: pd.DataFrame, limit: int = 5) -> str:
    if product_mix is None or product_mix.empty:
        return "No product mix data available."

    rows = product_mix.head(limit)
    lines = []

    for _, r in rows.iterrows():
        lines.append(
            f"- {r['product']}: GWP {fmt_currency(r['gwp'])}, share {fmt_pct(r['share_pct'])}"
        )

    return "\n".join(lines)


def _weekly_text(weekly_perf: pd.DataFrame, limit: int = 5) -> str:
    if weekly_perf is None or weekly_perf.empty:
        return "No weekly data available."

    rows = weekly_perf.head(limit)
    lines = []

    for _, r in rows.iterrows():
        week_label = pd.to_datetime(r["week_start"]).strftime("%Y-%m-%d")
        lines.append(
            f"- Week {week_label}: GWP {fmt_currency(r['gwp'])}, "
            f"NBW {fmt_currency(r['nbw_gwp'])}, Renewal {fmt_currency(r['renewal_gwp'])}, "
            f"NBW mix {fmt_pct(r['nbw_mix_pct'])}, Renewal mix {fmt_pct(r['renewal_mix_pct'])}"
        )

    return "\n".join(lines)


def _growth_text(growth_drivers: pd.DataFrame, limit: int = 5) -> str:
    if growth_drivers is None or growth_drivers.empty:
        return "No growth driver data available."

    rows = growth_drivers.head(limit)
    lines = []

    for _, r in rows.iterrows():
        lines.append(
            f"- {r['iph_channel_5']}: total GWP {fmt_currency(r['total_gwp'])}, "
            f"NBW GWP {fmt_currency(r['nbw_gwp'])}, Renewal GWP {fmt_currency(r['renewal_gwp'])}, "
            f"NBW mix {fmt_pct(r['nbw_mix_pct'])}"
        )

    return "\n".join(lines)


def build_commentary_prompt(
    selected_month_display: str,
    kpi: Dict[str, Any],
    top_states: pd.DataFrame,
    channel_mix: pd.DataFrame
) -> str:
    prompt = f"""
You are an FP&A analyst writing a concise executive monthly management commentary.

Write a polished business summary for {selected_month_display}.
Use a professional finance tone.
Be specific, concise, and decision-oriented.
Do not mention synthetic data in the main bullets.
Do not invent metrics that are not provided.
Do not use emojis.
Prefer short paragraphs or bullets.

Available KPIs:
- Total GWP: {fmt_currency(kpi.get('gwp'))}
- GWP MoM %: {fmt_pct(kpi.get('gwp_mom_pct'))}
- GWP YoY %: {fmt_pct(kpi.get('gwp_yoy_pct'))}
- GWP vs Plan %: {fmt_pct(kpi.get('gwp_vs_plan_pct'))}
- NBW GWP: {fmt_currency(kpi.get('nbw_gwp'))}
- NBW vs Plan %: {fmt_pct(kpi.get('nbw_vs_plan_pct'))}
- Renewal GWP: {fmt_currency(kpi.get('renewal_gwp'))}
- Renewal vs Plan %: {fmt_pct(kpi.get('renewal_vs_plan_pct'))}
- NBW Unique Pets: {fmt_number(kpi.get('nbw_unique_pets'))}
- Renewal Unique Pets: {fmt_number(kpi.get('renewal_unique_pets'))}
- Avg NBW Premium: {fmt_currency(kpi.get('avg_nbw_premium'))}
- Avg Renewal Premium: {fmt_currency(kpi.get('avg_renewal_premium'))}
- Renewal Pet Share: {fmt_pct(kpi.get('renewal_pet_share'))}

Top states:
{_top_states_text(top_states)}

Channel mix:
{_channel_mix_text(channel_mix)}

Write the response in this structure:
1. Executive Summary
2. Key Drivers
3. Management Focus Areas

Keep it short enough for a management-call prep note.
"""
    return prompt.strip()


def fallback_commentary(selected_month_display: str, kpi: Dict[str, Any]) -> str:
    return f"""
### {selected_month_display} Summary

- Total GWP for the month is **{fmt_currency(kpi.get('gwp'))}**, with month-over-month movement of **{fmt_pct(kpi.get('gwp_mom_pct'))}** and year-over-year movement of **{fmt_pct(kpi.get('gwp_yoy_pct'))}**.
- NBW GWP is **{fmt_currency(kpi.get('nbw_gwp'))}**, while Renewal GWP is **{fmt_currency(kpi.get('renewal_gwp'))}**.
- NBW Unique Pets are **{fmt_number(kpi.get('nbw_unique_pets'))}**, and Renewal Unique Pets are **{fmt_number(kpi.get('renewal_unique_pets'))}**.
- Average NBW Premium is **{fmt_currency(kpi.get('avg_nbw_premium'))}**, while Average Renewal Premium is **{fmt_currency(kpi.get('avg_renewal_premium'))}**.
- Renewal Pet Share currently stands at **{fmt_pct(kpi.get('renewal_pet_share'))}** and should be treated as a proxy indicator.

### Management Focus Areas

- Review whether current GWP performance is primarily driven by new business or renewals.
- Review the top states and channel mix to identify concentration and growth drivers.
- Compare actuals plus forecast against the full-year plan to assess expected finish.
""".strip()


def generate_management_commentary(
    selected_month_display: str,
    kpi: Dict[str, Any],
    top_states: pd.DataFrame,
    channel_mix: pd.DataFrame,
    use_ai: bool = True
) -> Tuple[str, str]:
    if not use_ai:
        return fallback_commentary(selected_month_display, kpi), "Static Fallback"

    try:
        prompt = build_commentary_prompt(
            selected_month_display=selected_month_display,
            kpi=kpi,
            top_states=top_states,
            channel_mix=channel_mix
        )

        ai_text = generate_bedrock_text(
            prompt=prompt,
            max_tokens=700,
            temperature=0.2
        )

        return ai_text, "AWS Bedrock"
    except Exception:
        return fallback_commentary(selected_month_display, kpi), "Static Fallback"


def build_assistant_prompt(
    assistant_type: str,
    user_question: str,
    selected_month_display: str,
    kpi: Dict[str, Any],
    weekly_perf: pd.DataFrame,
    top_states: pd.DataFrame,
    channel_mix: pd.DataFrame,
    product_mix: pd.DataFrame,
    growth_drivers: pd.DataFrame,
    plan_comparison: pd.DataFrame
) -> str:
    assistant_instructions = {
        "variance": "You are a Variance Assistant. Focus on explaining variance versus plan, mix, and major drivers.",
        "weekly": "You are a Weekly Performance Assistant. Focus on week-over-week movement, pacing, and short-term shifts.",
        "growth": "You are a Growth Drivers Assistant. Focus on states, channels, products, and acquisition/renewal mix.",
        "management": "You are a Management Call Assistant. Focus on executive-level summary, focus areas, risks, and next questions."
    }

    instruction = assistant_instructions.get(
        assistant_type,
        "You are a finance decision-support assistant."
    )

    prompt = f"""
{instruction}

Answer the user's question using only the supplied business context.
Be concise, practical, and business-oriented.
Do not invent facts.
If the data is insufficient, say so clearly.

Selected month:
{selected_month_display}

User question:
{user_question}

KPI snapshot:
- Total GWP: {fmt_currency(kpi.get('gwp'))}
- GWP MoM %: {fmt_pct(kpi.get('gwp_mom_pct'))}
- GWP YoY %: {fmt_pct(kpi.get('gwp_yoy_pct'))}
- GWP vs Plan %: {fmt_pct(kpi.get('gwp_vs_plan_pct'))}
- NBW GWP: {fmt_currency(kpi.get('nbw_gwp'))}
- Renewal GWP: {fmt_currency(kpi.get('renewal_gwp'))}
- NBW Unique Pets: {fmt_number(kpi.get('nbw_unique_pets'))}
- Renewal Unique Pets: {fmt_number(kpi.get('renewal_unique_pets'))}
- Avg NBW Premium: {fmt_currency(kpi.get('avg_nbw_premium'))}
- Avg Renewal Premium: {fmt_currency(kpi.get('avg_renewal_premium'))}
- NBW Mix %: {fmt_pct(kpi.get('nbw_mix_pct'))}
- Renewal Mix %: {fmt_pct(kpi.get('renewal_mix_pct'))}
- Renewal Pet Share: {fmt_pct(kpi.get('renewal_pet_share'))}

Weekly performance:
{_weekly_text(weekly_perf)}

Top states:
{_top_states_text(top_states)}

Channel mix:
{_channel_mix_text(channel_mix)}

Product mix:
{_product_mix_text(product_mix)}

Growth drivers:
{_growth_text(growth_drivers)}

Plan comparison context:
- 2025 Actual total: {fmt_currency(plan_comparison['actual_2025_gwp'].sum(skipna=True)) if not plan_comparison.empty else '—'}
- 2026 Plan total: {fmt_currency(plan_comparison['plan_2026_gwp'].sum(skipna=True)) if not plan_comparison.empty else '—'}
- 2026 Actual + Forecast total: {fmt_currency(plan_comparison['actual_plus_forecast_2026_gwp'].sum(skipna=True)) if not plan_comparison.empty else '—'}

Respond with:
1. Direct Answer
2. Supporting Drivers
3. Recommended Follow-up

Keep it crisp.
"""
    return prompt.strip()


def generate_assistant_answer(
    assistant_type: str,
    user_question: str,
    selected_month_display: str,
    kpi: Dict[str, Any],
    weekly_perf: pd.DataFrame,
    top_states: pd.DataFrame,
    channel_mix: pd.DataFrame,
    product_mix: pd.DataFrame,
    growth_drivers: pd.DataFrame,
    plan_comparison: pd.DataFrame
) -> Tuple[str, str]:
    try:
        prompt = build_assistant_prompt(
            assistant_type=assistant_type,
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

        response = generate_bedrock_text(
            prompt=prompt,
            max_tokens=800,
            temperature=0.2
        )

        return response, "AWS Bedrock"
    except Exception as e:
        return f"Assistant unavailable right now. Fallback triggered.\n\nReason: {str(e)}", "Fallback Error"