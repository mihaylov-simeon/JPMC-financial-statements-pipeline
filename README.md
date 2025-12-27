# JPM Financial Statements – Spark Data Pipeline

## Overview

This project implements a complete **Bronze → Silver → Gold** data pipeline,
developed using **Apache Spark (PySpark)**.

It processes raw financial statement data from a JPMorgan Chase balance sheet
dataset, normalizes it into a time-series structure, and produces
business-ready analytics such as **year-over-year (YoY) changes**.

The project demonstrates how real-world financial data can be transformed into
a clean, analysis-ready model suitable for scaling and downstream analytics.

---

## Dataset

**Source:** JPMorgan Chase balance sheet (CSV)

**Raw (Bronze) format:**
- One row per financial category (e.g. Total Assets, Total Debt, Cash)
- One column per reporting date (e.g. `2024-12-31`, `2023-12-31`, ...)

Example:

CATEGORY | 2024-12-31 | 2023-12-31 | 2022-12-31 | ...

This format is human-readable but not suitable for analytics, transformations,
or time-based calculations.

---

## Pipeline Design

### Bronze Layer – Raw Ingestion
- Reads the CSV file as-is
- Does not infer schema to avoid issues caused by automatic format guessing
- Represents the source data without any transformations

### Silver Layer – Normalized Fact Table
- Converts the dataset from **wide** to **long** format
- Produces a clean, normalized schema where:
  - `CATEGORY` = financial metric name
  - `TRX_DT` = reporting date
  - `TRX_AMT` = reported amount

This structure is optimal for transformations, aggregations, and analytics.

### Gold Layer – Business Metrics
- Uses Spark window functions to compute YoY metrics:
  - `YOY_CHG` – absolute change vs previous year
  - `YOY_PCT_CHG` – percentage change vs previous year
- Handles edge cases explicitly:
  - First reporting year with no previous value
  - Division by zero scenarios
- Final dataset is defined explicitly via a final `select`, exposing only the
  required business columns

Final Gold schema:


CATEGORY | TRX_DT | TRX_AMT | YOY_CHG | YOY_PCT_CHG

---

## Key Concepts Demonstrated

- Apache Spark (PySpark)
- Wide → Long transformations
- Window functions (`lag`, `partitionBy`, `orderBy`)
- Financial-safe decimal handling
- Layered data modeling (Bronze / Silver / Gold)
- Explicit schema control and clean data contracts

---

## Project Structure

PROJECT
|---data
| |--- bronze
| | |--- JPM_balance_sheet.csv
| |--- silver
| | |--- financial_statements_jpm_silver/
| | --- gold
| | |--- financial_statements_jpm_gold/
|---src
| |---pipelines
| | |---financial-statements-jpm.py
|
|---README.md
|---requirements.txt

---

## How to Run the pipeline

From the project root:

```bash
puthon src/pipelines/financial-statements-jpm
```

---

## Author

**Simeon Mihaylov**
---
**Data Engineer**
