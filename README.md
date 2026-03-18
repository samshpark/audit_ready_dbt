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

## 2. Data Architecture
![Architecture Diagram](https://github.com/your-username/your-repo/raw/main/images/architecture.png)

1.  **Source:** BigQuery Public Dataset (`thelook_ecommerce`).
2.  **Storage & Compute:** **DuckDB** for high-performance local analytical processing (optimized for Apple Silicon).
3.  **Transformation (dbt):** * **Staging:** Data cleaning and standardization.
    * **Intermediate:** Implementing complex business logic (e.g., Net Revenue, Returns).
    * **Marts:** Final tables for Financial Reporting (P&L, Inventory Aging).
4.  **Quality Control:** **Automated Reconciliation Layer** using dbt tests.

---

## 3. Tech Stack
| Category | Tools |
| :--- | :--- |
| **Languages** | SQL (BigQuery/DuckDB Dialect), Python |
| **Data Engine** | DuckDB, BigQuery |
| **Transformation** | dbt-core (v1.8+) |
| **Quality/Testing** | dbt-tests, Great Expectations |
| **Ops/Env** | Docker, Git |

---

## 4. Financial Modeling & Accounting Logic
This project moves beyond simple ETL by embedding **Accounting Principles** into the data transformation layer to ensure audit_ready data reliability

### 0. Data Integrity & Reconciliation
* **Process**: Performed a primary reconciliation between the Master table (Orders) and the Sub-ledger (Order Items) to verify completeness.
* **Objective**: Every dollar in the transaction log must be traceable to a master order. Any variance triggers an automated Audit Alert, preventing downstream reporting errors.

### 1. Revenue Recognition & Accrual BasedCut-off
* **Logic**: Implemented revenue recognition based on the **Accrual Basis**, using shipped_at as the primary trigger for revenue realization.
* **Control**: Analyzed the variance between payment (created_at) and fulfillment (shipped_at) to manage year-end Cut-off risks.

### 2. Returns & Refund Reconciliation
* **Matching**: Engineered a logic to link refund transactions back to original order_ids rather than treating them as isolated negative flows.
* **Integrity**: Ensures accurate calculation of Net Revenue by accounting for historical reversals.

### 3. Inventory Valuation (FIFO)
* **Methodology**: Implemented a **First-In, First-Out (FIFO)** valuation model using SQL Window Functions.
* **Aging Analysis**: Built models to identify slow-moving inventory and calculate potential Lower of Cost or Market (LCM) adjustments.

### 4. Automated Reconciliation (Internal Controls)
* **Transaction-to-Order**: Automated check to ensure the sum of order_items matches the master orders record.
* **Sub-ledger to GL**: Built a variance detection model that flags discrepancies between transactional sub-ledgers and General Ledger summaries.

---

## 5. Engineering Excellence (CPA Insight)
* **Audit Trail:** Every model includes descriptions and metadata to ensure data lineage for audit purposes.
* **Variance Detection:** Developed a 'Variance Alert' system that triggers when financial sub-ledgers don't match the general ledger models.
* **Cost Optimization:** Optimized CTEs and materialization strategies to reduce warehouse compute costs.

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

2. Install dependencies
```bash
pip install -r requirements.txt
```

3. Run dbt Pipeline (Seed -> Run -> Test)
```bash
dbt seed
dbt run
dbt test
```
