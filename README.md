## 📖 Case Study: The Challenge at NovaCart

NovaCart, a fast-growing e-commerce platform, processes thousands of daily transactions across three main entities: **Orders**, **Products**, and **Payments** [1, 6]. Originally, all customer transactions were stored in a single, operational **Azure SQL Database** [1].

As transaction volumes scaled, NovaCart's analytics team and business stakeholders began facing critical hurdles:
1. **Operational Bottlenecks (OLTP vs. OLAP):** Running heavy, complex analytical queries directly on the production Azure SQL Database slowed down database operations and risked crashing customer-facing transactional services [7].
2. **No Historical State Tracking:** When customer order statuses changed (e.g., from 'Placed' to 'Shipped' or 'Failed'), the database overwrote previous states [7, 8]. Business analysts could not track metrics over time or perform cohort analysis [2, 7].
3. **Data Quality Inconsistencies:** The data originating from different application frontends contained messy values—duplicate records, negative product prices, inconsistent casing, incorrect decimal formatting (using commas instead of dots), currency symbols (like `$`), and invalid representations of missing values (e.g., `"NA"`, `"null"`, `"?\"`) [2, 9-12].
4. **Lack of Scalable Infrastructure:** The company's legacy query systems were not designed to scale with exponential data growth or support future downstream Machine Learning and advanced BI workloads [2].

### The Solution: A Databricks Lakehouse
To overcome these barriers, the data engineering team designed and deployed a modern **Lakehouse Architecture on Azure Databricks** [2]. By moving analytics away from the live transactional database and into Delta Lake delta tables, NovaCart successfully established a scalable, reliable, and secure analytical platform [2, 13].

---

## 🏗️ Architecture Design & Data Flow

The project's architecture is structured across **11 core layers** spanning ingestion, processing, modeling, consumption, orchestration, and monitoring [3, 14]:

[ Azure SQL Database (Source) ] │ ▼  (Lakehouse Federation via Unity Catalog - Zero Replication Connection) [ Databricks Connections / Foreign Catalog ] │ ▼  (Incremental Ingestion Loop using control watermarks) [ Bronze Layer (Raw Delta Tables + Ingestion Control Metadata) ] │ ▼  (Deduplication Windowing, pySpark Cleansing, and Schema Standardization) [ Silver Layer (Cleaned & Quarantined Patterns + Processing Control Metadata) ] ┌─────┴──────────┐\n         ▼ (Good Data)    ▼ (Bad Data) [ Silver Transformed ]   [ Silver Quarantine Tables ] ──► (Operational Audit Team) │ ▼  (SCD Type 2 Historical Modeling & Category KPI Aggregation) [ Gold Layer (Active Dimensions + SCD Type 2 + Category Performance delta) ] │ ├──► [ Databricks Volumes Snapshots ] ──► (Parquet Snapshots for Auditing & ML) ▼ [ Consumption & Monitoring ] ├──► [ Databricks Native BI Dashboard ] └──► [ Legacy SQL Alerts ] ──► (Automated Stakeholder Email Notifications)
[ Orchestrated via Azure Databricks Jobs ]  &  [ Git Integration via Databricks Repos ]

---

## 🛠️ Medallion Pipeline Implementation Details

### 1. Data Connection: Lakehouse Federation
Rather than performing manual ingestion or copying entire database tables directly, we leveraged **Lakehouse Federation** via Unity Catalog [14, 15]. By establishing a secure link using server authentication, Databricks reads the live operational Azure SQL Database as a **Foreign Catalog** under the DBO schema [15-18]. This offers governed, real-time query pushdowns without exposing the master database directly or wasting storage on premature replicas [15].

### 2. Bronze Layer: Metadata-Driven Ingestion & Watermarking
The Bronze layer ingests raw transactional tables (`Orders`, `Products`, `Payments`) with minimal modifications to preserve original structures for traceability and disaster recovery [4, 6].
* **Control Table Logging:** An `ingestion_control` table tracks the layer, table name, timestamp columns, primary key columns, `last_successful_timestamp`, `last_successful_primary_key`, `last_run_id`, and row counts written [19, 20].
* **Fully Incremental Loop:** Dynamic PySpark helpers (`get_last_successful_watermark` and `upsert_bronze_control`) verify if previous checkpoints exist [21]. On execution, the loop queries the source tables and filters only for newly appended or updated records (`timestamp > last_successful_timestamp` or matching timestamp with `primary_key > last_successful_primary_key`) [21-23].
* **Audit Fields:** Every bronze record is enriched with `bronze_ingested_at`, `bronze_run_id`, and `bronze_source_table` metadata [23, 24].

### 3. Silver Layer: Cleansing, Validation, & The Quarantine Pattern
The Silver layer is the gateway for data quality, standardization, and schema compliance [5]. It reads incremental raw batches from Bronze, using a `processing_control` metadata table to track the progress of each entity run [25, 26].
* **Window-Based Deduplication:** PySpark window functions partition raw data by logical primary keys (e.g., `order_id`) and sort by the latest update date and bronze ingestion timestamp to extract and process only the active state (`row_number == 1`), removing duplicate updates [27-30].
* **Standardization & pySpark Cleansing:**
  - Standardizes text columns (e.g., order status and category) by trimming trailing spaces and converting text to uppercase [11, 28, 31-33].
  - Resolves application-level spelling errors (e.g., maps `"ELECTRIONICS"` to `"ELECTRONICS"`) [11, 32].
  - Normalizes numbers by replacing currency markers (`$`), removing erroneous spaces, replacing comma decimals with dots, and safely casting string quantities into numeric types using `try_cast` (preventing pipeline crashes on invalid formats) [11, 12, 31, 33-35].
  - Converts invalid default placeholders (such as `"NA"`, `"null"`, `"?"`, and blank spaces) into true Spark `null` values [32-34].
* **The Quarantine Pattern:** Validated records are split based on strict data quality rules [36-39]:
  - **Silver Transformed Tables:** Rows with valid IDs, valid categories, and positive pricing are written to transformed tables (e.g., `orders_transformed`) and enriched with calendar properties (year, month, day, day of the week, and date strings) [37, 39-43].
  - **Silver Quarantine Tables:** Malformed rows failing crucial checks (e.g., `customer_id IS NULL`, negative product price, missing payment status) are stamped with an issue description string (e.g., `"verify customer ID"`, `"invalid price"`) and appended to a separate table (e.g., `orders_quarantine`) [36, 39, 42, 44-47]. This allows dedicated operations teams to trace, audit, and fix structural problems at the source application level [48, 49].

### 4. Gold Layer: Analytical Modeling, Manual SCD Type 2, & Backups
The Gold layer represents business-ready, highly aggregated data optimized for reporting, business intelligence, and sharing [5, 50].
* **Manual SCD Type 2 Implementation:** We manually engineered historical tracking using Spark SQL and null-safe equality operators (`<=>`) [5, 51]. When changes occur in active dimensions (such as order status updates) [52]:
  - The active record's `is_current` flag is updated to `false` and the `valid_to_ts` is populated with the change timestamp [52, 53].
  - A new record is inserted with `is_current = true`, `valid_from_ts` as the change timestamp, and `valid_to_ts = null` [54].
  This allows analysts to trace full historical timelines of e-commerce state transitions (e.g., an order moving from `PLACED` to `PAID` or `FAILED`) [8, 55].
* **Category Performance Aggregation:** Joins transformed dimensions and builds a curated summary table (`category_performance`) containing crucial metrics [56, 57]:
  - Total Orders by Category [56, 58]
  - Gross Merchandise Value (GMV) [56, 58]
  - Total Actual Paid Amount [56, 58]
  - Average Payment Completion Ratio [56, 59]
  - Payment Failure Rate [56, 59]
* **Exporting Snapshots (Databricks Volumes):** To allow external BI tools, analysts, and ML models to access the curated gold layer, datasets are written as optimized Parquet files into a Databricks Volume folder structure [50, 60, 61]:
  - `/gold_latest/`: Holds a daily overwritten "latest state snapshot" of Gold tables [62, 63].
  - `/gold_snapshots/`: Retains timestamped historical directories representing separate runs for tracking, deep analysis, and audit backups [63-65].

---

## 📊 Business Intelligence, Alerting, & Orchestration

### Native BI Dashboard
An interactive dashboard is connected directly to Gold tables [66, 67]. It displays key operational charts, including category-level sales metrics, product counts, and payment behavior over time (visualized via pie charts, bar charts, and timeline line plots) [66, 68-70].

### Threshold Monitoring & Alerts
Using Databricks SQL Alerts, the platform monitors operational metrics like order volume [66, 70]. A custom threshold (e.g., order count exceeding 300) is evaluated against gold query metrics [71]. If crossed, it fires an automated, customized HTML email notification to stakeholders with instructions and metadata details [66, 71-73].

### Pipeline Orchestration
The entire workflow is automated via **Azure Databricks Jobs** [14]. Five sequential tasks are orchestrated to run based on upstream success dependencies [14, 74, 75]:
[ Bronze Ingestion ] ──► [ Silver Cleansing ] ──► [ Gold Aggregations ] ──► [ BI Dashboard Update ] ──► [ SQL Alert Evaluation ]
Email notifications are integrated directly into the job run to immediately notify data engineering teams on success or failure events [75, 76].

---

## 💻 Tech Stack Used
* **Compute & Execution:** Azure Databricks, Apache Spark, pySpark [15, 77, 78]
* **Data Lake & Formats:** Delta Lake, Delta Tables, Parquet [4, 50, 78]
* **Catalog & Connections:** Unity Catalog, Lakehouse Federation [15]
* **Transactional Source:** Azure SQL Database, Azure SQL Server [3, 79]
* **Orchestration & Dashboarding:** Databricks Jobs, Databricks SQL Dashboards, Legacy SQL Alerts [3, 14, 66, 70]
* **Software Engineering & CI/CD:** GitHub, Databricks Repos, Git Integration [77]

---

## 📈 Impact & Achievements
* **100% Incremental Efficiency:** Replaced full-table recomputations with incremental loads, preventing compute crashes and reducing cloud resource billing for high-volume transactions [80].
* **Zero Production Downtime:** Moved heavy reporting queries from operational Azure SQL Database into Delta Lake, boosting production transaction performance and eliminating risk of performance bottlenecks [7, 13].
* **Comprehensive Audit Trail:** Implemented the quarantine pattern to isolate messy entries, dramatically improving downstream reporting accuracy while enabling business units to fix data quality issues at the source [2, 48, 49].
* **True History Tracking:** Established SCD Type 2 tracking, allowing NovaCart to accurately audit previous customer transaction changes and perform lifetime behavior analysis [5, 13, 55].
