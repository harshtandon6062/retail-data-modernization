"""Build dashboard/index.html: one self-contained Plotly page (KPI cards + 4 charts).

Reads only the exported/aggregated CSVs (no Spark needed), so it is fast and the
HTML opens offline in any browser - plotly.js is embedded in the file.
"""
import sys
from datetime import datetime
from pathlib import Path

import pandas as pd
import plotly.graph_objects as go
from plotly.offline import get_plotlyjs

ROOT = Path(__file__).resolve().parent.parent
A = ROOT / "reports" / "analytics"
E = ROOT / "exports" / "powerbi"

# Validated categorical palette (fixed order, colour follows the entity)
REGION_COLORS = {"NORTH": "#2a78d6", "SOUTH": "#eb6834", "EAST": "#1baf7a", "WEST": "#eda100"}
BLUE = "#2a78d6"
LAYOUT = dict(template="plotly_white", font=dict(family="Inter, Segoe UI, sans-serif", size=13, color="#0b0b0b"),
              margin=dict(l=60, r=20, t=50, b=50), height=380, autosize=True, hoverlabel=dict(bgcolor="white"),
              paper_bgcolor="#fcfcfb", plot_bgcolor="#fcfcfb")


def inr(x):
    """Indian style: 1,02,94,32,964 -> show as crore (1 crore = 10 million)."""
    return f"₹{x / 1e7:,.2f} Cr"


def kpis():
    fact = pd.read_csv(E / "fact_sales.csv")
    dates = pd.read_csv(E / "dim_date.csv")[["date_key", "year"]]
    f = fact[~fact.order_status.isin(["CANCELLED", "RETURNED"])].merge(dates, on="date_key")
    rev = f.net_amount.sum()
    orders = f.order_id.nunique()
    by_year = f.groupby("year").net_amount.sum()
    yoy = 100 * (by_year.get(2025) - by_year.get(2024)) / by_year.get(2024)
    return [("Net revenue", inr(rev)), ("Orders", f"{orders:,}"),
            ("Customers who ordered", f"{fact.merge(pd.read_csv(E / 'dim_customer.csv'), on='customer_sk').customer_id.nunique():,}"),
            ("Avg order value", f"₹{rev / orders:,.0f}"), ("YoY growth 2025 vs 2024", f"{yoy:+.1f}%")]


def chart_monthly():
    m = pd.read_csv(A / "01_monthly_revenue_trend.csv")
    m = m[m.year_month <= "2025-12"]          # 2026 holds only the CDC demo orders
    fig = go.Figure(go.Scatter(x=m.year_month, y=m.revenue / 1e7, mode="lines+markers",
                               line=dict(color=BLUE, width=2), marker=dict(size=8),
                               hovertemplate="%{x}<br>₹%{y:.2f} Cr<extra></extra>"))
    fig.update_layout(title="Monthly net revenue (₹ crore) - note the Oct/Nov festive peak", **LAYOUT)
    fig.update_yaxes(rangemode="tozero", title=None)
    fig.update_xaxes(type="category", tickangle=-45)
    return fig


def chart_region_year():
    b = pd.read_csv(A / "05_avg_basket_size.csv")
    b = b[b.year <= 2025]
    fig = go.Figure()
    for region, color in REGION_COLORS.items():
        r = b[b.region == region]
        fig.add_bar(name=region.title(), x=r.year.astype(str), y=r.avg_order_value, marker_color=color,
                    hovertemplate=region.title() + " %{x}<br>AOV ₹%{y:,.0f}<extra></extra>")
    fig.update_layout(title="Average order value by region (₹)", barmode="group", bargap=0.3,
                      bargroupgap=0.08, legend=dict(orientation="h", y=1.12, x=1, xanchor="right"), **LAYOUT)
    return fig


def chart_store_yoy():
    s = pd.read_csv(A / "04_store_yoy_growth.csv").dropna(subset=["yoy_growth_pct"])
    s = s.sort_values("yoy_growth_pct")
    fig = go.Figure()
    for region, color in REGION_COLORS.items():
        r = s[s.region == region]
        fig.add_bar(name=region.title(), y=r.store_name, x=r.yoy_growth_pct, orientation="h", marker_color=color,
                    hovertemplate="%{y}<br>YoY %{x:+.1f}%<extra></extra>")
    fig.update_layout(title="Store revenue growth, 2025 vs 2024 (%)", **{**LAYOUT, "height": 560},
                      legend=dict(orientation="h", y=-0.08, x=0.5, xanchor="center"))
    fig.update_yaxes(categoryorder="array", categoryarray=list(s.store_name))
    return fig


def chart_retention():
    c = pd.read_csv(A / "03_customer_retention_cohorts.csv", dtype={"cohort_quarter_start": str})
    # keep the 2 full years; 2026 only holds the handful of CDC demo orders
    c = c[c.cohort_quarter_start <= "2025-10"]
    c = c[c.quarters_since <= 7]
    pivot = c.pivot(index="cohort_quarter_start", columns="quarters_since", values="retention_pct")
    fig = go.Figure(go.Heatmap(z=pivot.values, x=[f"Q+{q}" for q in pivot.columns], y=pivot.index,
                               colorscale=[[0, "#cde2fb"], [0.5, "#5598e7"], [1, "#0d366b"]],
                               text=[["" if pd.isna(v) else f"{v:.0f}%" for v in row] for row in pivot.values],
                               texttemplate="%{text}", xgap=2, ygap=2,
                               hovertemplate="Cohort %{y}, %{x}: %{z:.1f}% active<extra></extra>",
                               colorbar=dict(title="%")))
    fig.update_layout(title="Customer retention by first-purchase quarter (%)", **{**LAYOUT, "height": 560})
    fig.update_yaxes(autorange="reversed", type="category", title="Cohort (quarter start)")
    fig.update_xaxes(type="category")
    return fig


def build():
    cards = "".join(f'<div class="card"><div class="label">{k}</div><div class="value">{v}</div></div>'
                    for k, v in kpis())
    charts = [chart_monthly(), chart_region_year(), chart_store_yoy(), chart_retention()]
    divs = [fig.to_html(full_html=False, include_plotlyjs=False, default_width="100%", config={"displayModeBar": False, "responsive": True})
            for fig in charts]
    html = f"""<!doctype html>
<html lang="en"><head><meta charset="utf-8"><meta name="viewport" content="width=device-width, initial-scale=1">
<title>RetailCo Sales Dashboard</title>
<script>{get_plotlyjs()}</script>
<style>
  :root {{ --bg:#f4f4f2; --surface:#fcfcfb; --ink:#0b0b0b; --muted:#52514e; --border:#e2e1dc; }}
  body {{ margin:0; background:var(--bg); color:var(--ink); font-family:Inter,"Segoe UI",sans-serif; }}
  header {{ padding:20px 16px 4px; max-width:1300px; margin:auto; }}
  h1 {{ font-size:22px; margin:0 0 4px; }} p.sub {{ color:var(--muted); margin:0; font-size:13px; }}
  .kpis {{ display:grid; grid-template-columns:repeat(auto-fit,minmax(170px,1fr)); gap:12px;
           max-width:1300px; margin:16px auto; padding:0 16px; }}
  .card {{ background:var(--surface); border:1px solid var(--border); border-radius:10px; padding:14px 16px; }}
  .label {{ color:var(--muted); font-size:12px; text-transform:uppercase; letter-spacing:.04em; }}
  .value {{ font-size:26px; font-weight:650; margin-top:4px; font-variant-numeric:tabular-nums; }}
  .grid {{ display:grid; grid-template-columns:repeat(auto-fit,minmax(min(100%,560px),1fr)); gap:12px;
           max-width:1300px; margin:0 auto 24px; padding:0 16px; }}
  .panel {{ background:var(--surface); border:1px solid var(--border); border-radius:10px; overflow:hidden; min-width:0; }}
  footer {{ color:var(--muted); font-size:12px; text-align:center; padding:0 16px 24px; }}
</style></head><body>
<header><h1>RetailCo - Sales Dashboard</h1>
<p class="sub">Source: gold star schema (Delta Lake) via PII-free exports. Revenue excludes cancelled/returned orders.
Synthetic data, Jan 2024 - Dec 2025.</p></header>
<section class="kpis">{cards}</section>
<section class="grid">{''.join(f'<div class="panel">{d}</div>' for d in divs)}</section>
<footer>Generated by src/dashboard.py on {datetime.now():%Y-%m-%d %H:%M}. Rebuild in Power BI: docs/POWERBI.md</footer>
<script>
  // charts are drawn before the CSS grid settles; re-fit each one to its panel
  window.addEventListener("load", () =>
    document.querySelectorAll(".js-plotly-plot").forEach(p => Plotly.Plots.resize(p)));
</script>
</body></html>"""
    out = ROOT / "dashboard" / "index.html"
    out.parent.mkdir(exist_ok=True)
    out.write_text(html)
    print(f"  dashboard written: {out.relative_to(ROOT)} ({out.stat().st_size / 1e6:.1f} MB, works offline)")
    for k, v in kpis():
        print(f"    {k:<26} {v}")


if __name__ == "__main__":
    sys.exit(build())
