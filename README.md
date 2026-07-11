# 🏎️ Formula 1 Lakehouse Data Engineering Project — Azure Databricks

**Repository:** [github.com/mtj-08/f1_project_repo](https://github.com/mtj-08/f1_project_repo)

An end-to-end **Lakehouse data engineering pipeline** built on **Azure Databricks**, **Azure Data Lake Storage Gen2 (ADLS Gen2)**, and **Delta Lake**, following the **Medallion Architecture** (Landing → Bronze → Silver → Gold). The project ingests raw Formula 1 racing data, applies schema enforcement and data quality rules, models it into a **dimensional star schema**, and serves it through **Databricks SQL / Lakeview dashboards** for driver and constructor standings analytics.

The project implements **two complete processing paradigms** in parallel — a **Single (Full) Batch Load** pipeline and an **Incremental Batch Load** pipeline with CDC-style merges — to demonstrate both foundational and production-grade batch engineering patterns.

---

## 📌 Key Highlights (Tech Stack / Keywords)

`Azure Databricks` · `Delta Lake` · `PySpark` · `Spark SQL` · `Azure Data Lake Storage Gen2 (ADLS Gen2)` · `Unity Catalog` · `External Locations & Volumes` · `Storage Credentials` · `Medallion Architecture (Bronze/Silver/Gold)` · `Dimensional Modeling (Star Schema)` · `Slowly Changing Data / MERGE (Upsert) / CDC` · `Databricks Workflows & Jobs Orchestration` · `Job/Task Parameters & Conditional Tasks` · `Serverless Compute` · `Git Integration (Databricks Repos)` · `Delta Time Travel` · `ACID Transactions` · `Data Quality & Schema Enforcement` · `Batch Control Table Pattern` · `Databricks Lakeview Dashboards` · `Data Warehousing` · `ELT/ETL Pipeline Design` · `GDPR-aware Data Correction`

---

## 📂 Repository Structure

```
f1_project_repo/
│
├── f1_project_batch/                     # Approach 1: Single / Full Batch Refresh pipeline
│   ├── 00.common/                        # Config + helper notebooks (reusable across all layers)
│   ├── 01.setup/                         # Environment setup: external location, catalog, schemas, volumes
│   ├── 02.bronze/                        # Raw ingestion notebooks (Circuits, Races, Constructors, Drivers, Results, Sprints)
│   ├── 03.silver/                        # Cleansing, standardisation & transformation notebooks
│   ├── 04.gold/                          # Dimensional model: dim_races, dim_drivers, dim_constructors, fact_session_results
│   └── 05.analytics/                     # SQL views: driver/constructor standings, dominance analysis
│
├── f1_project_incremental_batch/         # Approach 2: Incremental Batch pipeline (batch_id driven, MERGE-based)
│   ├── 00.common/                        # Config + helper notebooks (write_to_bronze, write_to_silver, write_to_gold)
│   ├── 01.setup/                         # Environment setup for the incremental catalog/container
│   ├── 02.bronze/                        # Batch-partitioned ingestion (replaceWhere on batch_id)
│   ├── 03.silver/                        # MERGE / upsert transformations with created_/updated_timestamp
│   ├── 04.gold/                          # Incremental dimensional & fact table MERGE logic
│   └── 06.orchestration/                 # Batch control table, identify-next-batch, new-batch, complete-batch notebooks
│
├── F1_Project_Analytics_Dashboard.lvdash.json   # Databricks Lakeview dashboard definition (driver & constructor analytics)
└── .gitattributes
```

> The repo is developed on the **`feature`** branch using **Databricks Repos (Git folder integration)**, with all notebooks committed and pushed directly from the Databricks workspace and merged via pull request.

---

## 🏗️ Solution Architecture

```
Landing (ADLS Gen2 Volume)
      │   raw CSV / JSON files (circuits, races, constructors, drivers, results, sprints)
      ▼
Bronze (Delta Tables — schema enforced, audit columns added)
      │   ingestion_timestamp, source_file metadata, FAILFAST schema validation
      ▼
Silver (Delta Tables — cleaned & conformed)
      │   snake_case naming, dedup on business keys, null handling, title-casing
      ▼
Gold (Delta Tables — dimensional model)
      │   dim_races, dim_drivers, dim_constructors, fact_session_results
      ▼
Gold Views / Lakeview Dashboard
      driver & constructor standings, dominance ("greatness score") analytics
```

Two ADLS Gen2 storage containers were provisioned to isolate the two processing patterns — one for the **full-refresh** pipeline and one for the **incremental** pipeline — each with its own `landing / bronze / silver / gold` directory structure, mounted into Databricks via **Unity Catalog External Locations**, **Storage Credentials**, and **External Volumes**.

---

## 🔧 Environment Setup

- Provisioned **ADLS Gen2 containers** (`f1project` for batch, `formula1-incr` for incremental) with dedicated landing sub-folders.
- Created a **Databricks Storage Credential** and **External Location** to securely link Databricks to ADLS Gen2:
  ```sql
  CREATE EXTERNAL LOCATION IF NOT EXISTS databricksf1projectext_f1project
  URL 'abfss://databricksf1projectext.dfs.core.windows.net/'
  WITH (STORAGE CREDENTIAL `db-project-sc`)
  COMMENT 'External ADLS location for source data';
  ```
- Created a **Unity Catalog catalog** (`formula1`) with a managed location, and **landing / bronze / silver / gold schemas** underneath it.
- Created an **External Volume** over the `landing` path so raw files can be accessed via the `/Volumes/catalog/schema/volume/...` path convention (instead of raw `abfss://` paths), while bronze/silver/gold remain **managed Delta locations**.
- Set up a **custom cluster for development**, with **serverless compute** used for most triggered/scheduled jobs to optimize cost.
- All setup logic is captured in a reusable **`01.Environment Setup`** notebook (in `01.setup/`) so the environment can be torn down and rebuilt consistently.

---

## 🥉 Bronze Layer — Raw Ingestion (`02.bronze/`)

Ingests all 6 raw datasets, each with a different file format, using the Spark **DataFrameReader → DataFrame transformation → DataFrameWriter** pattern:

| Dataset | Format | Notable Handling |
|---|---|---|
| Circuits | CSV | Explicit `StructType` schema (avoids `inferSchema` in production) |
| Races | CSV | Same pattern, additional business keys (season, round) |
| Constructors | Single-line JSON | DDL-style schema string + `.option("mode", "FAILFAST")` |
| Drivers | Nested JSON | Nested `StructType` for `name.givenName` / `name.familyName`, preserved as-is |
| Results | Single-line JSON (multi-file) | Folder-level path read across multiple files |
| Sprints | Multi-line JSON (multi-file) | `.option("multiLine", "true")` |

**Common Bronze logic (`00.common` helpers):**
- A `config` notebook centralizes catalog/schema/table names and paths so nothing is hardcoded in ingestion notebooks (`%run ../00.common/...`).
- A `helper` notebook exposes a reusable `add_ingestion_metadata(df)` function that stamps every record with `ingestion_timestamp` and `source_file` (via Spark's `_metadata.file_path`).
- Spark schema-mismatch handling strategy documented and applied: **PERMISSIVE** vs **DROPMALFORMED** vs **FAILFAST**.
- All 6 datasets are written as **managed Delta tables** (`mode("overwrite")` for the full-refresh approach).

---

## 🥈 Silver Layer — Cleansing & Conformance (`03.silver/`)

Each Bronze dataset has a corresponding Silver transformation notebook that:
- Selects only analytics-relevant columns (drops `url` and other noise fields).
- Standardizes column names to **snake_case** and renames for clarity (`lat` → `latitude`, `date` → `race_date`, `grid` → `grid_position`, `laps` → `completed_laps`, `position` → `finish_position`, etc.).
- Applies **data quality checks** — filters out null business keys, drops duplicates via `.dropDuplicates()` on the correct primary key (`circuit_id`; composite `season + round` for races; `constructor_id`, `driver_id`, etc.).
- Applies `F.initcap()` for consistent Title Case on name/locality fields.
- **Drivers Silver** additionally derives a combined `driver_name` column via `F.concat_ws()` on given/family name.
- Writes conformed data to Silver Delta tables, preserving business keys across layers for downstream joins.

---

## 🥇 Gold Layer — Dimensional Model & Analytics (`04.gold/`)

A **Kimball-style star schema** was designed for reporting:

**Dimensions**
- `dim_races` — Races ⋈ Circuits (inner join) → season, round, race name/date, circuit, locality, country
- `dim_drivers` — Drivers ⋈ `ref_nationality_region` → driver identity + nationality region for geographic analysis
- `dim_constructors` — Constructors ⋈ `ref_nationality_region` → constructor identity + nationality region

**Fact**
- `fact_session_results` — Unified Race + Sprint results (same grain), combined via `unionByName(allowMissingColumns=True)`, with a `session_type` (`RACE`/`SPRINT`) discriminator column and derived analytical flags:
  - `is_win` (finished P1)
  - `is_podium` (finished P1–P3)
  - `has_points` (points > 0)

**Gold Views / Analytics (`05.analytics/`)**
- `v_driver_standing` — season-level driver standings using `RANK() OVER (PARTITION BY season ORDER BY total_points DESC, number_of_wins DESC)`.
- Dominant driver/constructor **"greatness score"** query — a weighted composite metric:
  `greatness_score = (championships × 100) + (wins × 10) + (podiums × 3)`, surfacing the most dominant drivers/teams across F1 history.
- All views are backed by Delta tables, optimized for repeated BI/reporting queries.

---

## ⚙️ Databricks Jobs Orchestration

- Orchestrated the full Bronze → Silver → Gold pipeline with **Databricks Workflows (Jobs)**, using a dedicated **job cluster**.
- Serverless compute used wherever possible to reduce idle cluster cost for scheduled/triggered runs.

---

## 🔁 Incremental Batch Pipeline (`f1_project_incremental_batch/`)

The second, more production-oriented approach reprocesses only **new/changed batches**, identified by a `batch_id` (e.g. `2025-01`) folder convention in the landing zone, and uses **MERGE (upsert)** semantics instead of full overwrites downstream.

**Key patterns implemented:**
- **`dbutils.widgets`** parameters (`p_batch_id`) passed at the job/task level so every notebook processes a single batch.
- **Bronze:** appends data **partitioned by `batch_id`**, using `.option("replaceWhere", f"batch_id = '{v_batch_id}'")` for safe, idempotent re-runs of a given batch without touching other partitions. Refactored into a reusable `write_to_bronze()` helper function.
- **Silver:** uses `DeltaTable.merge()` (CDC-style upsert) keyed on business keys, with a **`whenMatchedUpdate` guarded by `s.batch_id >= t.batch_id`** to prevent older/replayed batches from overwriting newer data, and separate `created_timestamp` (insert-only) vs `updated_timestamp` (every merge) audit columns. Refactored into a reusable `write_to_silver()` helper (create-if-not-exists, else merge).
- **Gold:** mirrors the Silver merge pattern for `dim_races`, `dim_drivers`, `dim_constructors`, and `fact_session_results`, keyed on their respective grain (e.g. `season + round`).
- **Batch Control Table** (`control.batch_control`) tracks `batch_id`, `status` (`in_progress` / `complete`), and timestamps — implementing a lightweight **batch orchestration / watermarking pattern**:
  1. **Identify Next Batch** — diffs landing-zone batch folders against already-tracked batches to compute the next `batch_id`, publishing it via `dbutils.jobs.taskValues`.
  2. **Create New Batch** — inserts an `in_progress` control row.
  3. *(Medallion pipeline runs against that `batch_id`.)*
  4. **Complete Batch** — merges the control row to `complete` once the pipeline finishes.
- A **Databricks Job with a conditional task** wires these together: only proceeds to run the medallion pipeline if a new batch is actually available, then marks it complete — a self-driving, idempotent incremental pipeline.

---

## 📊 Reporting & Dashboard

A **Databricks Lakeview dashboard** (`F1_Project_Analytics_Dashboard.lvdash.json`) was built directly on the Gold views, with four pages:

1. **Driver Standings** — championship standings table, championship-distribution pie chart, total points bar chart.
2. **Constructor Standings** — championship standings table, championship-distribution pie chart, total points bar chart.
3. **Dominant Drivers** — greatness-score table (points, podiums, championships), championship-share pie chart, greatness-score bar chart.
4. **Dominant Constructors** — same "greatness score" analysis at the constructor level.

---

## ✅ Non-Functional Requirements Addressed

- **Reliability & recovery:** idempotent re-runs via `replaceWhere` (Bronze) and `MERGE` with batch-aware guards (Silver/Gold); batch control table prevents duplicate/partial reprocessing.
- **ACID & data integrity:** Delta Lake transaction log guarantees; schema enforcement with `FAILFAST` for malformed source data.
- **Auditability & GDPR-readiness:** every record is timestamped with `ingestion_timestamp` / `created_timestamp` / `updated_timestamp` and traceable to its `source_file` and `batch_id`.
- **Time travel & rollback:** Delta Lake versioned reads (`spark.read.option("versionAsOf", 0)`) supported for historical/point-in-time analysis.
- **Cost optimization:** serverless triggers for most scheduled workloads; dedicated job clusters only where required.
- **Version control:** all notebooks developed on feature branches in **Databricks Repos**, committed and merged into `main`/`feature` via pull request.

---

## 🔗 Links

- **GitHub Repository:** https://github.com/mtj-08/f1_project_repo
- Full/Batch pipeline notebooks: `f1_project_batch/`
- Incremental pipeline notebooks: `f1_project_incremental_batch/`
- Dashboard definition: `F1_Project_Analytics_Dashboard.lvdash.json`
