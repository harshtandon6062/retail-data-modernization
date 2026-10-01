# Rebuild the dashboard in Power BI Desktop

The pipeline exports the gold star schema to `exports/powerbi/*.csv` (`src/export_bi.py`).
PII is not exported (no names / email hash) - the BI model gets the same non-identifying
columns as the analyst view `gold.v_sales_analyst`.

> In a real project Power BI would connect directly to the gold layer
> (Databricks SQL / Synapse / BigQuery connector) in **DirectQuery** or **Import** mode;
> CSV keeps this demo portable.

## 1. Load the data (about 5 minutes)

1. Power BI Desktop -> **Get data -> Text/CSV**, load the five files from `exports/powerbi/`:
   `fact_sales.csv`, `dim_date.csv`, `dim_customer.csv`, `dim_product.csv`, `dim_store.csv`.
2. In **Transform data (Power Query)** check types:
   - `dim_date[full_date]` -> Date; all `*_key` / `customer_sk` -> Whole number;
   - `net_amount`, `gross_amount`, `discount_amount`, `unit_price` -> Fixed decimal number.
3. **Close & Apply**.
4. Select `dim_date` -> **Table tools -> Mark as date table** -> column `full_date`
   (this makes time-intelligence DAX like `SAMEPERIODLASTYEAR` work).

## 2. Star-schema relationships (Model view)

All relationships are **one-to-many (1:*)**, single direction, dimension -> fact:

| From (one side) | To (many side) | Key |
|---|---|---|
| `dim_date[date_key]` | `fact_sales[date_key]` | date |
| `dim_customer[customer_sk]` | `fact_sales[customer_sk]` | SCD2 surrogate key (the customer version valid at order time) |
| `dim_product[product_key]` | `fact_sales[product_key]` | product |
| `dim_store[store_key]` | `fact_sales[store_key]` | store |

```
            dim_date
               |
dim_customer - fact_sales - dim_product
               |
           dim_store
```

Tips: hide the key columns in the fact table (report users should slice by dimension attributes),
and for "current" customer attributes filter `dim_customer[is_current] = TRUE`.

## 3. DAX measures

Create a measure table (Home -> Enter data -> name it `_Measures`) and add:

```DAX
Total Revenue =
CALCULATE (
    SUM ( fact_sales[net_amount] ),
    NOT fact_sales[order_status] IN { "CANCELLED", "RETURNED" }
)
```

```DAX
YoY % =
VAR CurrentRev = [Total Revenue]
VAR PrevRev    = CALCULATE ( [Total Revenue], SAMEPERIODLASTYEAR ( dim_date[full_date] ) )
RETURN
    DIVIDE ( CurrentRev - PrevRev, PrevRev )          -- DIVIDE returns BLANK instead of an error on /0
```

```DAX
Average Order Value =
DIVIDE (
    [Total Revenue],
    CALCULATE (
        DISTINCTCOUNT ( fact_sales[order_id] ),
        NOT fact_sales[order_status] IN { "CANCELLED", "RETURNED" }
    )
)
```

Format: `Total Revenue` and `Average Order Value` as Currency (₹), `YoY %` as Percentage.

## 4. Visuals (same as `dashboard/index.html`)

| Visual | Fields |
|---|---|
| 5 **Card** visuals (KPIs) | Total Revenue, distinct count of `order_id`, distinct count of `dim_customer[customer_id]`, Average Order Value, YoY % (filter year = 2025) |
| **Line chart**: monthly revenue | X: `dim_date[year_month]`, Y: Total Revenue |
| **Clustered column**: AOV by region | X: `dim_date[year]`, Legend: `dim_store[region]`, Y: Average Order Value |
| **Bar chart**: store YoY growth | Y: `dim_store[store_name]`, X: YoY % ; page filter year = 2025 |
| **Matrix** (retention cohorts) | Use `reports/analytics/03_customer_retention_cohorts.csv` as an extra table: Rows cohort, Columns quarters_since, Values retention_pct with conditional formatting |
| **Slicers** | `dim_date[year]`, `dim_store[region]`, `dim_product[category]` |

Expected numbers (check you built it right): Total Revenue for 2025 ≈ ₹49.02 Cr and YoY % for 2025 ≈ +14.2%
(same as the Scala job and the HTML dashboard).

## 5. Row-level security (governance bonus)

Modeling -> **Manage roles** -> role `North Manager` with DAX filter on `dim_store`:
`[region] = "NORTH"`. Test with **View as**. Publish to the Power BI Service and assign users to the role.
