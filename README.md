# 🛒 NovaCart Data Engineering Pipeline: End-to-End Lakehouse Architecture

## 📖 Project Overview & Business Challenge
NovaCart, a fast-growing e-commerce platform, processes thousands of daily transactions across three main entities: **Orders**, **Products**, and **Payments**. Originally, all transactions were stored in a single operational Azure SQL Database. As transaction volumes scaled, the analytics team faced critical hurdles:
* **Operational Bottlenecks:** Running heavy analytical queries directly on the production database risked crashing customer-facing services.
* **No Historical State Tracking:** When order statuses changed (e.g., 'Placed' to 'Shipped'), previous states were overwritten, preventing cohort analysis.
* **Data Quality Inconsistencies:** Messy frontend data contained duplicates, negative prices, inconsistent casing, and invalid null representations.
* **Lack of Scalability:** Legacy systems could not support future Machine Learning or advanced BI workloads.

**The Solution:** A modern **Lakehouse Architecture on Azure Databricks**. By moving analytics into Delta Lake and implementing a strict Medallion Architecture, this project established a scalable, reliable, and secure analytical platform with 100% incremental efficiency.

---

## 🏗️ Architecture Design & Data Flow

The project's architecture is structured across 11 core layers spanning ingestion, processing, modeling, consumption, orchestration, and monitoring:

```text
[ Azure SQL Database ] ──► (Lakehouse Federation / Zero Replication) ──► [ Databricks Foreign Catalog ]
                                                                                   │
[ Bronze Layer ] ◄── (Incremental Ingestion Loop using control watermarks) ────────┘
 (Raw Delta Tables)
        │
        ▼
[ Silver Layer ] ──► (Window Deduplication, PySpark Cleansing & Schema Standardization)
        │
        ├──► [ Silver Transformed (Good Data) ] 
        └──► [ Silver Quarantine (Bad Data) ] ──► (Operational Audit Team)
        │
        ▼
[ Gold Layer ] ──► (Manual SCD Type 2 Modeling & Category KPI Aggregation)
        │
        ├──► [ Databricks Volumes ] ──► (Parquet Snapshots for Auditing & ML)
        ├──► [ Native BI Dashboard ] ──► (Power BI / Databricks Dashboards)
        └──► [ Legacy SQL Alerts ] ──► (Automated Stakeholder Notifications)
