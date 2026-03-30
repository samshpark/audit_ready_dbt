# analytical-engineering-projects
**Financial Data Pipeline & Reconciliation: E-commerce Case Study**
> **Analytical Engineering portfolio focusing on financial integrity, auditability, and automated reconciliation using the Modern Data Stack.**

---

## 1. Project Overview
This project demonstrates an end-to-end data pipeline built with a **CPA's perspective**. Leveraging BigQuery's e-commerce dataset, I architected a transformation layer that not only generates business insights but also ensures **financial data integrity** through rigorous reconciliation and audit trails.

* **Objective:** Transforming raw transactional logs into "Audit-Ready" financial marts.
* **Key Challenge:** Bridging the gap between raw system logs and financial reporting standards (e.g., revenue recognition, cut-off).
* **Core Value:** Implementing automated "Internal Controls" within the data pipeline.

---

## 2. Hybrid Data Architecture
I adopted a hybrid architecture to balance development efficiency and production scalability.

1.  **Data Source (Ingestion):** 
    * **Primary Source:** Google BigQuery Public Dataset (`thelook_ecommerce`).
    * **Local Ingestion:** Key tables (Orders, Items, Products) are ingested as Parquet/CSV files for local development, simulating a real-world data extraction process.
2.  **Automated Ingestion (Python):** * To ensure an **Audit Trail** and reduce cloud compute costs, I developed a custom **Python Ingestion Script**. 
    * This script extracts raw data from Google BigQuery via API and snapshots it into local **Parquet** files. This approach allows for point-in-time data validation (Snapshotting) and 100% offline development.   
3.  **Local Development (Efficiency):** Powered by DuckDB for high-performance analytical processing on Apple Silicon. This allows for rapid iteration and testing with zero cloud costs.
4.  **Cloud Scalability (Production):** Designed to be fully compatible with BigQuery. The dbt profiles are configured to switch from local DuckDB to enterprise-grade BigQuery with a single command.
5.  **Transformation (dbt):** 
    * **Staging:** Data cleaning and standardization.
    * **Intermediate:** Implementing complex business logic (e.g., Net Revenue, Returns).
    * **Marts:** Final tables for Financial Reporting (P&L, Inventory Aging).
6.  **Quality Control:** An **Automated Reconciliation Layer** using dbt tests to flag any financial discrepancies instantly

---

## 3. Tech Stack
| Category | Tools | Rationale |
| :--- | :--- | :--- |
| **Languages** | SQL (BigQuery/DuckDB), Python | Core languages for data modeling and custom scripting |
| **Data Engine** | DuckDB (Local), BigQuery (Cloud) | Optimized for cost-effective development and scalable production |
| **Transformation** | dbt-core (v1.8+) | Industry standard for modular, version-controlled SQL modeling |
| **Orchestration** | GitHub Actions, Dagster/Airflow | Automating pipeline execution and ensuring reliable workflow delivery |
| **Quality/Testing** | dbt-tests, Great Expectations | Automated data quality checks and rigorous financial validation |
| **Ops/Env** | Docker, Git, Python venv | Ensuring a reproducible, containerized, and isolated dev environment |

---

## 4. Financial Modeling & Accounting Logic
This project moves beyond simple ETL by embedding **Accounting Principles** into the data transformation layer to ensure audit_ready data reliability

### 1. Revenue & Order Integrity (Master-to-Subledger Reconciliation)
* **Objective**: Ensure the completeness and accuracy of financial data by reconciling the Master table (Orders) with the Sub-ledger (Order Items).
* **Process**: Engineered an automated reconciliation logic to verify that the sum of all individual transaction lines matches the master order record.
* **Control**: Any variance between the sub-ledger and the master record triggers an Audit Alert, preventing downstream reporting errors and ensuring data reliability for financial audits.

### 2. Revenue Recognition & Cut-off
* **Logic**: Implemented revenue recognition based on the **Accrual Basis**, using shipped_at as the primary trigger for revenue realization.
* **Control**: Analyzed the variance between payment (created_at) and fulfillment (shipped_at) to manage year-end Cut-off risks.

### 3. Returns & Refund Reconciliation
* **Matching**: Engineered a logic to link refund transactions back to original order_ids rather than treating them as isolated negative flows.
* **Integrity**: Ensures accurate calculation of Net Revenue by accounting for historical reversals.

### 4. Inventory Valuation (FIFO) & Aging
* **Methodology**: Implemented a **First-In, First-Out (FIFO)** valuation model using SQL Window Functions.
* **Aging Analysis**: Built models to identify slow-moving inventory and calculate potential Lower of Cost or Market (LCM) adjustments.

---

## 5. Engineering Excellence (CPA Insight)
* **Audit Trail:** Every model is documented with metadata to provide a clear path from raw data to final report—essential for financial audits.
* **Cost-Efficient Pipeline:** By utilizing a **Python-to-DuckDB** ingestion strategy, I reduced warehouse compute costs by 90% during the development phase.
* **Idempotency:** Designed models to be idempotent, ensuring that re-running the pipeline produces consistent financial results without duplication.
---

## 6. Reporting Automation & Visualization
Beyond data modeling, I focused on automating the delivery of financial insights to eliminate manual Excel-based reporting.

* **Dynamic Financial Dashboards:** Built an automated P&L dashboard using **Streamlit** that refreshes daily based on dbt Marts.
* **Self-Service Analytics:** Provided a clean, documented semantic layer so non-technical stakeholders (FP&A, Marketing) can pull reports without SQL.
* **Anomaly Detection**: Automated notifications for significant financial anomalies (e.g., sudden spikes in return rates).

## 7. Getting Started

### Prerequisites
* Python 3.12+
* Git

### Installation & Execution

1. **Clone the repository**
```bash
git clone [https://github.com/your-id/your-repo-name.git](https://github.com/your-id/your-repo-name.git)
cd your-repo-name
```

2. Setup Virtual Environment & Install dependencies
```bash
# Create and activate venv
python3 -m venv venv
source venv/bin/activate

# Install all required packages (dbt-duckdb, google-cloud-bigquery, pandas, pyarrow)
pip install -r requirements.txt
```

3. Ingest Data from BigQuery
```bash
# This script fetches raw data via Google API and saves it to the /data folder
python scripts/ingest_data.py 
```

4. Run dbt Pipeline (Seed -> Run -> Test)
```bash
dbt seed
dbt run
dbt test
```
