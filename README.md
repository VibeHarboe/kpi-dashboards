# Ageras KPI Dashboard — dbt Project

A production-grade dbt project modelling KPI dashboards for a multi-market B2B marketplace operating across Denmark, Norway, Sweden, Germany, the Netherlands, and the United States.

This project was built to recreate the analytical work done at Ageras, covering six core KPI areas using Snowflake as the data warehouse and Lightdash/Metabase as the BI layer.

---

## Tech Stack

| Layer | Tool |
|---|---|
| Transformation | dbt 1.8 |
| Warehouse | Snowflake |
| BI / Visualisation | Lightdash / Metabase |
| CI/CD | GitHub Actions |
| Testing | dbt_expectations + dbt built-in tests |

---

## Project Structure

```
ageras_kpi_dbt/
├── seeds/                     # Fictional source data (CSV)
│   ├── customers.csv
│   ├── subscriptions.csv
│   ├── leads.csv
│   ├── invoices.csv
│   ├── partners.csv
│   ├── nps_surveys.csv
│   └── upsell_events.csv
│
├── models/
│   ├── staging/               # 1:1 source cleaning, type casting, derived flags
│   │   ├── stg_customers.sql
│   │   ├── stg_subscriptions.sql
│   │   ├── stg_leads.sql
│   │   ├── stg_invoices.sql
│   │   ├── stg_partners.sql
│   │   ├── stg_nps_surveys.sql
│   │   ├── stg_upsell_events.sql
│   │   └── schema.yml
│   │
│   └── marts/                 # Business-facing KPI tables
│       ├── supply_demand/     → mart_supply_demand
│       ├── churn/             → mart_churn
│       ├── upselling/         → mart_upselling
│       ├── roi_partnerships/  → mart_roi_partnerships
│       ├── debt_collection/   → mart_debt_collection
│       ├── customer_satisfaction/ → mart_customer_satisfaction
│       └── schema.yml
│
├── macros/
│   └── kpi_helpers.sql        # safe_divide, pct, nps_score, aging_bucket, classify_nps
│
├── .github/
│   └── workflows/
│       └── dbt_ci.yml         # CI: seed → build → test → docs (manual trigger)
│
├── dbt_project.yml
├── packages.yml
└── requirements.txt
```

---

## KPI Dashboards

### 1. Supply & Demand (`mart_supply_demand`)
Tracks the balance between inbound lead volume and available supply of accountants/advisors per market.

**Key metrics:**
- Lead volume by country, service type, and month
- Assignment rate and SLA breach rate (target: 0% breaches >24h)
- Conversion rate (lead → customer)
- `has_supply_gap` flag: markets where >10% of leads go unassigned

---

### 2. Churn Prevention (`mart_churn`)
Two views in one model — aggregate churn trends and individual at-risk customer scoring.

**Key metrics:**
- Monthly churn rate by country and plan (target: <2%)
- Churned MRR and average customer lifetime before churn
- Churn reason breakdown (price, competitor, not using)
- At-risk scoring: composite risk score from NPS + overdue invoices + tenure

---

### 3. Upselling & Sales (`mart_upselling`)
Measures effectiveness of plan upgrades and add-on sales across markets and sales reps.

**Key metrics:**
- Incremental revenue from upsells by month / country / sales rep
- Plan upgrade volume vs add-on attach volume
- Upsell attach rate per segment (target: >40% of eligible customers)
- Estimated upsell opportunity MRR still on the table

---

### 4. ROI on Partnerships (`mart_roi_partnerships`)
Evaluates whether each partner organisation generates more revenue than they cost.

**Key metrics:**
- Active MRR attributed per partner
- Total monthly cost (fixed fee + commission)
- Net ROI % and net monthly value
- Payback period in months
- Cost per acquired customer

---

### 5. Debt Collection (`mart_debt_collection`)
Monitors outstanding invoices, collection efficiency, and customer payment risk.

**Key metrics:**
- Days Sales Outstanding (DSO) — target: <15 days
- Overdue rate and recovery rate (target: >85%)
- Aging buckets: 1–30, 31–60, 61–90, 90+ days overdue
- High-risk debtor list with total outstanding amounts

---

### 6. Customer Satisfaction (`mart_customer_satisfaction`)
NPS and CSAT reporting across all markets and customer segments.

**Key metrics:**
- NPS score by country / segment / month (target: >30)
- CSAT % (target: >75%)
- Promoter / passive / detractor breakdown
- Detractor MRR at risk
- Survey response rate by country

---

## Getting Started

### Prerequisites
- Python 3.11+
- Snowflake account with a `TRANSFORMER` role
- dbt CLI

### Setup

```bash
# Clone the repo
git clone https://github.com/your-username/ageras_kpi_dbt.git
cd ageras_kpi_dbt

# Install dependencies
pip install -r requirements.txt
dbt deps

# Configure your Snowflake connection
# Copy profiles.yml and fill in your credentials (do NOT commit this file)
cp profiles.yml.example profiles.yml

# Load seed data
dbt seed

# Build all models
dbt build

# Generate and serve docs
dbt docs generate && dbt docs serve
```

### Running specific KPI areas

```bash
# Run only supply & demand
dbt build --select tag:supply_demand

# Run only churn models
dbt build --select tag:churn

# Run staging only
dbt build --select tag:staging
```

---

## Testing

All models include schema tests. Run with:

```bash
dbt test
```

Test coverage includes:
- `unique` and `not_null` on all primary keys
- `accepted_values` on categorical columns (country, plan_type, status, etc.)
- `relationships` for all foreign key constraints
- `dbt_expectations` range checks on numeric KPIs (NPS score 0–10, commission rate 0–1, etc.)

---

## CI/CD

A GitHub Actions workflow is included and can be triggered manually via `workflow_dispatch`. When run against a live Snowflake environment it will:

1. Install dbt-snowflake
2. Run `dbt seed` with test data
3. Run `dbt build` (compile + run + test)
4. Generate and upload dbt docs as an artifact
5. Drop the ephemeral CI schema on teardown

Secrets required: `SNOWFLAKE_ACCOUNT`, `SNOWFLAKE_USER`, `SNOWFLAKE_PASSWORD`

---

## Variables

Defined in `dbt_project.yml` and used across models:

| Variable | Default | Description |
|---|---|---|
| `churn_lookback_days` | 90 | Window for recent churn analysis |
| `upsell_min_tenure_days` | 30 | Min customer age to be upsell-eligible |
| `debt_overdue_threshold_days` | 30 | Days before invoice flagged high-risk |
| `nps_low_score` | 6 | Max score to classify as detractor |
| `nps_high_score` | 9 | Min score to classify as promoter |

---

## Author

**Vibe Harboe Christensen**
Data Analyst — [github.com/VibeHarboe](https://github.com/VibeHarboe)

*Note: All data in this repository is fictional and created for portfolio purposes. The KPI logic and dashboard structure reflect real analytical work completed at Ageras across six markets.*
