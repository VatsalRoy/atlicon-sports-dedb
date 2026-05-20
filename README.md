# Data Engineering Pipeline — Atlikon 

A production-grade data engineering pipeline built on **Databricks** using **PySpark** and **Delta Lake**, implementing a **Medallion Architecture (Bronze → Silver → Gold)** for an FMCG business with a parent company and a child company (SportsBar).

![Project Architecture](project_architecture.png)

---

## Table of Contents

- [Overview](#overview)
- [Architecture](#architecture)
- [Project Structure](#project-structure)
- [Data Sources](#data-sources)
- [Pipeline Notebooks](#pipeline-notebooks)
- [Data Quality Transformations](#data-quality-transformations)
- [Gold Layer Schema](#gold-layer-schema)
- [Dashboarding](#dashboarding)
- [How to Run](#how-to-run)

---

## Overview

This project consolidates sales order data from two companies into a unified FMCG analytics layer:

- **Parent Company** — data loaded via CSV files directly into the Gold layer using `COPY INTO`.
- **Child Company (SportsBar)** — raw data sourced from **Amazon S3**, processed through Bronze → Silver → Gold, and merged into the parent's Gold tables using Delta Lake `MERGE`.

The final Gold layer powers a BI dashboard with enriched, denormalized order data.

---

## Architecture

```
Parent Company CSVs          Child Company (S3)
       │                            │
       ▼                            ▼
  COPY INTO Gold              Bronze Layer
                                    │
                              Silver Layer
                            (Cleansed & Enriched)
                                    │
                              Gold Layer (SB_*)
                                    │
                         MERGE into Parent Gold Tables
                                    │
                         ┌──────────────────────┐
                         │  fmcg.gold.*         │
                         │  dim_customers       │
                         │  dim_products        │
                         │  dim_gross_price     │
                         │  dim_date            │
                         │  fact_orders         │
                         └──────────────────────┘
                                    │
                         vw_fact_orders_enriched
                                    │
                              BI Dashboard
```

**Catalog:** `fmcg`  
**Schemas:** `bronze`, `silver`, `gold`  
**Storage:** Amazon S3 (`s3://sportsbar-final/`)  
**Table Format:** Delta Lake (with Change Data Feed enabled)

---

## Project Structure

```
project-de-fmcg-atlikon/
│
├── 0_data/
│   ├── 1_parent_company/
│   │   ├── full_load/          # dim_customers, dim_products, dim_gross_price, fact_orders CSVs
│   │   └── incremental_load/   # COPY INTO query for incremental fact_orders
│   └── 2_child_company/
│       ├── full_load/          # customers, products, gross_price subdirectories
│       └── incremental_load/   # orders subdirectory
│
├── 1_codes/
│   ├── 1_setup/
│   │   ├── setup_catalog.ipynb           # Create fmcg catalog + bronze/silver/gold schemas
│   │   ├── utilities.ipynb               # Shared schema variables (bronze/silver/gold)
│   │   └── dim_date_table_creation.ipynb # Generate dim_date (monthly grain, 2024–2025)
│   │
│   ├── 2_dimension_data_processing/
│   │   ├── 1_customers_data_processing.ipynb
│   │   ├── 2_products_data_processing.ipynb
│   │   └── 3_pricing_data_processing.ipynb
│   │
│   └── 3_fact_data_processing/
│       ├── 1_full_load_fact.ipynb
│       └── 2_incremental_load_fact.ipynb
│
├── 2_dashboarding/
│   ├── denormalise_table_query_fmcg.txt  # Gold view SQL
│   └── fmcg_dashboard.pdf                # Dashboard export
│
└── resources/
    ├── project_architecture.png
    └── databricks_project.excalidraw
```

---

## Data Sources

### Parent Company (Full Load)

| File | Description |
|---|---|
| `dim_customers.csv` | Customer master: `customer_code`, `customer`, `market`, `platform`, `channel` |
| `dim_products.csv` | Product master: `product_code`, `division`, `category`, `product`, `variant` |
| `dim_gross_price.csv` | Pricing: `product_code`, `price_inr`, `year` |
| `fact_orders.csv` | Monthly orders: `date`, `product_code`, `customer_code`, `sold_quantity` |

### Child Company — SportsBar (S3: `s3://sportsbar-final/`)

| Source | Description |
|---|---|
| `customers/*.csv` | Customer master with city |
| `products/*.csv` | Product master with category |
| `gross_price/*.csv` | Monthly gross prices per product |
| `orders/landing/*.csv` | Daily order transactions |

---

## Pipeline Notebooks

### 1. Setup (`1_codes/1_setup/`)

| Notebook | Purpose |
|---|---|
| `setup_catalog.ipynb` | Creates `fmcg` catalog and `bronze`, `silver`, `gold` schemas |
| `utilities.ipynb` | Defines shared schema name variables |
| `dim_date_table_creation.ipynb` | Builds `fmcg.gold.dim_date` at monthly grain with year/quarter attributes |

### 2. Dimension Processing (`1_codes/2_dimension_data_processing/`)

Each notebook follows the **Bronze → Silver → Gold → Merge** pattern:

| Notebook | Tables Produced | Merges Into |
|---|---|---|
| `1_customers_data_processing.ipynb` | `bronze.customers`, `silver.customers`, `gold.sb_dim_customers` | `gold.dim_customers` |
| `2_products_data_processing.ipynb` | `bronze.products`, `silver.products`, `gold.sb_dim_products` | `gold.dim_products` |
| `3_pricing_data_processing.ipynb` | `bronze.gross_price`, `silver.gross_price`, `gold.sb_dim_gross_price` | `gold.dim_gross_price` |

### 3. Fact Processing (`1_codes/3_fact_data_processing/`)

| Notebook | Strategy | Description |
|---|---|---|
| `1_full_load_fact.ipynb` | Full Load | Reads all landing CSVs, processes through Bronze → Silver → Gold, aggregates daily → monthly, merges into `gold.fact_orders` |
| `2_incremental_load_fact.ipynb` | Incremental Load | Reads only new landing files, uses staging tables, recalculates affected months, merges into `gold.fact_orders`, cleans up staging |

---

## Data Quality Transformations

### Customers
- Deduplication on `customer_id`
- Trim whitespace from `customer_name`
- Fix city typos (`Bengalore` → `Bengaluru`, `Hyderabadd` → `Hyderabad`, `NewDelhi` → `New Delhi`, etc.)
- Impute missing cities using business-confirmed lookup
- Standardize `customer_name` to title case
- Cast `customer_id` to string
- Derive `customer` column as `"CustomerName-City"`
- Add static attributes: `market = India`, `platform = Sports Bar`, `channel = Acquisition`

### Products
- Deduplication on `product_id`
- Title case fix for `category`
- Fix spelling: `Protien` → `Protein`
- Derive `division` from category mapping
- Extract `variant` from product name using regex
- Generate deterministic `product_code` via SHA-256 of `product_name`
- Fallback invalid `product_id` to `999999`

### Gross Price
- Parse `month` from multiple date formats (`yyyy/MM/dd`, `dd/MM/yyyy`, `yyyy-MM-dd`, `dd-MM-yyyy`)
- Convert `gross_price` to numeric; flip negatives to positive; set non-numeric values to `0`
- Join with `silver.products` to resolve `product_code`
- Aggregate to annual price per product (prioritizing non-zero, latest month)

### Orders (Fact)
- Drop rows with null `order_qty`
- Normalize `customer_id` — non-numeric values replaced with `999999`
- Strip weekday prefix from date strings (e.g., `"Tuesday, July 01, 2025"` → parsed date)
- Parse dates from multiple formats
- Deduplication on `(order_id, date, customer_id, product_id, order_qty)`
- Join with `silver.products` to resolve `product_code`
- Aggregate daily orders to **monthly grain** before merging into parent Gold layer

---

## Gold Layer Schema

### `fmcg.gold.dim_date`

| Column | Type |
|---|---|
| `month_start_date` | date |
| `date_key` | int |
| `year` | int |
| `month_name` | string |
| `month_short_name` | string |
| `quarter` | string |
| `year_quarter` | string |

### `fmcg.gold.fact_orders`

| Column | Type |
|---|---|
| `date` | date (month start) |
| `product_code` | string |
| `customer_code` | string |
| `sold_quantity` | double |

---

## Dashboarding

The Gold view `fmcg.gold.vw_fact_orders_enriched` denormalizes all dimension tables against `fact_orders` for direct BI consumption:

```sql
CREATE OR REPLACE VIEW fmcg.gold.vw_fact_orders_enriched AS (
    SELECT
        fo.date, fo.product_code, fo.customer_code,
        -- Date
        dd.year, dd.month_name, dd.quarter, dd.year_quarter,
        -- Customer
        dc.customer, dc.market, dc.platform, dc.channel,
        -- Product
        dp.division, dp.category, dp.product, dp.variant,
        -- Metrics
        fo.sold_quantity,
        gp.price_inr,
        (fo.sold_quantity * gp.price_inr) AS total_amount_inr
    FROM fmcg.gold.fact_orders fo
    LEFT JOIN fmcg.gold.dim_date dd      ON fo.date = dd.month_start_date
    LEFT JOIN fmcg.gold.dim_customers dc  ON fo.customer_code = dc.customer_code
    LEFT JOIN fmcg.gold.dim_products dp   ON fo.product_code = dp.product_code
    LEFT JOIN fmcg.gold.dim_gross_price gp
           ON fo.product_code = gp.product_code AND YEAR(fo.date) = gp.year
);
```

The dashboard PDF is available at `2_dashboarding/fmcg_dashboard.pdf`.

---

## How to Run

> All notebooks are designed to run on **Databricks** with a Spark cluster and access to the `s3://sportsbar-final/` bucket.

**Execution Order:**

1. **Setup**
   - `1_setup/setup_catalog.ipynb` — create catalog and schemas
   - `1_setup/utilities.ipynb` — run once to register shared variables
   - `1_setup/dim_date_table_creation.ipynb` — populate date dimension

2. **Dimension Data (Child Company)**
   - `2_dimension_data_processing/1_customers_data_processing.ipynb`
   - `2_dimension_data_processing/2_products_data_processing.ipynb`
   - `2_dimension_data_processing/3_pricing_data_processing.ipynb`

3. **Parent Company Data**
   - Use the `COPY INTO` query in `0_data/1_parent_company/full_load/` to load parent CSVs into Gold tables
   - Use the incremental query in `0_data/1_parent_company/incremental_load/` for subsequent loads

4. **Fact Data (Child Company)**
   - First run: `3_fact_data_processing/1_full_load_fact.ipynb`
   - Subsequent runs: `3_fact_data_processing/2_incremental_load_fact.ipynb`

5. **Create the enriched view** using the query in `2_dashboarding/denormalise_table_query_fmcg.txt`

**Notebook Parameters (via Databricks Widgets):**

| Parameter | Default | Description |
|---|---|---|
| `catalog` | `fmcg` | Databricks Unity Catalog name |
| `data_source` | varies | Table name (`customers`, `products`, `gross_price`, `orders`) |
