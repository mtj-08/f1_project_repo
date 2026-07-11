# 🏎️ End-to-End Data Engineering & BI Pipeline for Formula 1 Analytics (Azure Databricks, Delta Lake, Unity Catalog)

**Repository:** [github.com/mtj-08/f1_project_repo](https://github.com/mtj-08/f1_project_repo) (`feature` branch)

An end-to-end **Lakehouse pipeline** on **Azure Databricks + ADLS Gen2 + Delta Lake + Unity Catalog**, implementing the **Medallion Architecture** (Landing → Bronze → Silver → Gold) for Formula 1 racing data, built twice over — once as a **Single Full-Refresh Batch** pipeline and once as a **Batch-ID-driven Incremental** pipeline with MERGE semantics.

> This page is generated directly from the notebooks in the repo (cloned and inspected folder-by-folder), cross-referenced with the project's design notes, so folder names, function names, and table/column names below match the actual code.

**Keywords:** `Azure Databricks` · `Delta Lake` · `PySpark` · `Spark SQL` · `ADLS Gen2` · `Unity Catalog` · `External Locations/Volumes` · `Medallion Architecture` · `Star Schema Dimensional Modeling` · `Delta MERGE / Upsert / CDC` · `Databricks Workflows & Jobs` · `Task Values & Conditional Tasks` · `Serverless Compute` · `Databricks Repos (Git integration)` · `Batch Watermarking / Control Table Pattern` · `Databricks Lakeview Dashboards`

---

## Table of Contents
1. [Repository Layout](#1-repository-layout)
2. [Architecture](#2-architecture)
3. [Environment Setup (`01.Setup_Notebook`)](#3-environment-setup)
4. [Common / Config & Helpers (`00.Common`)](#4-common--config--helpers)
5. [Bronze Layer (`02.Bronze_Ingestion_Notebooks`)](#5-bronze-layer)
6. [Silver Layer (`03.Silver_Transformation_Notebooks`)](#6-silver-layer)
7. [Gold Layer (`04.Gold_Dimensions_Notebooks`)](#7-gold-layer)
8. [Analytics Views (`05.Analytics`)](#8-analytics-views)
9. [Incremental-only: Orchestration (`06.Orchestration_Notebooks`)](#9-incremental-only-orchestration)
10. [Jobs / Workflow Orchestration](#10-jobs--workflow-orchestration)
11. [Dashboard](#11-dashboard)
12. [Batch vs Incremental — Key Differences](#12-batch-vs-incremental--key-differences)

---

## 1. Repository Layout

```
f1_project_repo/  (branch: feature)
│
├── f1_project_batch/                       # Approach 1 — Single Full-Refresh Batch
│   ├── 00.Common/
│   │   ├── 01.Environment-config.ipynb
│   │   └── 02.Helper_Notebook.ipynb
│   ├── 01.Setup_Notebook/
│   │   └── 01.Environment_Setup.ipynb
│   ├── 02.Bronze_Ingestion_Notebooks/
│   │   ├── 01.Circuits_ingestion_notebook.ipynb
│   │   ├── 02.Races_ingestion_notebook.ipynb
│   │   ├── 03.Constructors_ingestion_notebook.ipynb
│   │   ├── 04.Drivers_ingestion_notebook.ipynb
│   │   ├── 05.Results_ingestion_notebook.ipynb
│   │   └── 06.Sprints_ingestion_notebook.ipynb
│   ├── 03.Silver_Transformation_Notebooks/
│   │   ├── 01.Transforming_circuits_table.ipynb
│   │   ├── 02.Transforming_races_table.ipynb
│   │   ├── 03.Transforming_constructors_table.ipynb
│   │   ├── 04.Transforming_drivers_table.ipynb
│   │   ├── 05.Transforming_results_table.ipynb
│   │   └── 06.Transforming_sprints_table.ipynb
│   ├── 04.Gold_Dimensions_Notebooks/
│   │   ├── 01.Building_Nationality_Region_ref.ipynb
│   │   ├── 02.Building_Races_Dimesion_Table.ipynb
│   │   ├── 03.Building_Constructors_Dimesion_Table.ipynb
│   │   ├── 04.Building_Drivers_Dimesion_Table.ipynb
│   │   └── 05.Building_Results_Fact_Table.ipynb
│   └── 05.Analytics/
│       ├── 01.Driver_Standings_View.ipynb
│       ├── 02.Constructors_Standings_View.ipynb
│       ├── 03.Dominant_Driver_Query.dbquery.ipynb
│       └── 04.Dominant_Constructor_Query.dbquery.ipynb
│
├── f1_project_incremental_batch/           # Approach 2 — batch_id driven Incremental Load
│   ├── 00.Common/
│   │   ├── 01.Environment-config.ipynb
│   │   ├── 02.Helper_Notebook_Bronze.ipynb
│   │   ├── 03.Helper_Notebook_Silver.ipynb
│   │   └── 04.Helper_Notebook_Gold.ipynb
│   ├── 01.Setup_Notebook/01.Environment_Setup.ipynb
│   ├── 02.Bronze_Ingestion_Notebooks/       (same 6 datasets, batch_id-aware)
│   ├── 03.Silver_Transformation_Notebooks/  (same 6 datasets, MERGE-based)
│   ├── 04.Gold_Dimensions_Notebooks/        (same 5 tables, MERGE-based)
│   ├── 05.Analytics/
│   │   ├── 01.Driver_Standings_View.ipynb
│   │   └── 02.Constructors_Standings_View.ipynb
│   └── 06.Orchestration_Notebooks/
│       ├── 01.Creating_Control_Table.ipynb
│       ├── 02.Identify_Next_Batch.ipynb
│       ├── 03.Create_New_Batch.ipynb
│       └── 04.Complete_Batch.ipynb
│
├── F1_Project_Analytics_Dashboard.lvdash.json    # Databricks Lakeview dashboard
└── .gitattributes
```

Both pipelines were developed on the **`feature`** branch inside **Databricks Repos**, committed and pushed straight from the workspace, with merges going through pull requests.
<img width="477" height="302" alt="image" src="https://github.com/user-attachments/assets/57c0aac5-6387-469a-a726-280874720f25" />
<img width="940" height="471" alt="image" src="https://github.com/user-attachments/assets/aa8cb346-9191-4eee-9fb6-abf700737827" />
<img width="940" height="426" alt="image" src="https://github.com/user-attachments/assets/96432d39-c4b7-47b4-bf2c-e2c3e2fef5a0" />

---

## 2. Architecture

```
Landing (ADLS Gen2 external Volume, raw files)
        │  circuits.csv, races.csv, constructors.json, drivers.json,
        │  results/*.json, sprints/*.json  (multiLine)
        ▼
Bronze (managed Delta tables, schema-enforced, audit metadata)
        │  ingestion_timestamp, SourceFile, [batch_id for incremental]
        ▼
Silver (managed Delta tables, cleaned & conformed)
        │  snake_case renames, dedup on business keys, null filtering, initcap
        ▼
Gold (managed Delta tables, star schema)
        │  dim_races, dim_drivers, dim_constructors, ref_nationality_region,
        │  fact_session_results
        ▼
Gold Views (v_driver_standings, v_constructor_standings) + Lakeview Dashboard
```
<img width="940" height="425" alt="image" src="https://github.com/user-attachments/assets/6f4a343d-3f41-4a77-b135-38ab4e97b4a1" />


Two ADLS Gen2 **containers** on the same storage account (`databricksf1projectext`) back the two pipelines:
- `f1project` → full-refresh pipeline (`formula1` catalog)
- `f1project-incr` → incremental pipeline (`formula1_incr` catalog)

<img width="940" height="455" alt="image" src="https://github.com/user-attachments/assets/bfb201d0-fd38-481b-a3b8-709957849bfd" />

---

## 3. Environment Setup

Both `01.Setup_Notebook/01.Environment_Setup.ipynb` notebooks follow the same four steps, just against different containers/catalogs:

**Batch pipeline (`formula1` catalog / `f1project` container):**
```sql
CREATE EXTERNAL LOCATION IF NOT EXISTS databricksf1projectext_f1project
URL 'abfss://f1project@databricksf1projectext.dfs.core.windows.net/'
WITH (STORAGE CREDENTIAL `db-project-sc`)
COMMENT 'External ADLS location for source data';

CREATE CATALOG IF NOT EXISTS formula1
MANAGED LOCATION 'abfss://f1project@databricksf1projectext.dfs.core.windows.net/'
COMMENT 'Main catalog for the f1 project';

CREATE SCHEMA IF NOT EXISTS landing;
CREATE SCHEMA IF NOT EXISTS bronze MANAGED LOCATION 'abfss://f1project@databricksf1projectext.dfs.core.windows.net/';
CREATE SCHEMA IF NOT EXISTS silver MANAGED LOCATION 'abfss://f1project@databricksf1projectext.dfs.core.windows.net/';
CREATE SCHEMA IF NOT EXISTS gold   MANAGED LOCATION 'abfss://f1project@databricksf1projectext.dfs.core.windows.net/';

CREATE EXTERNAL VOLUME formula1.landing.files
LOCATION 'abfss://f1project@databricksf1projectext.dfs.core.windows.net/landing'
COMMENT 'landing files volume';
```
<img width="940" height="309" alt="image" src="https://github.com/user-attachments/assets/ea5cbb13-8f86-4fd2-b6f1-bdd919b99a91" />

<img width="769" height="289" alt="image" src="https://github.com/user-attachments/assets/31d54da4-9da5-4763-af34-5b04f34fe2e1" />

**Incremental pipeline** mirrors this exactly, swapping in `formula1_incr` / `f1project-incr` and its own external location (`databricksf1projectext_f1project_incr`).

Note: `landing` is intentionally **not** a managed schema — it only carries an **external Volume** so raw files are addressed with the friendlier `/Volumes/<catalog>/<schema>/<volume>/...` path instead of a raw `abfss://` path; `bronze/silver/gold` are managed Delta locations.

---

## 4. Common / Config & Helpers

### Batch pipeline — `00.Common/`
**`01.Environment-config.ipynb`** — centralizes every name used downstream so nothing is hardcoded:
```python
catalog_name = 'formula1'
landing_schema = 'landing'
silver_schema = 'silver'
bronze_schema = 'bronze'
gold_schema = 'gold'
landing_folder_path = '/Volumes/formula1/landing/files'
```

**`02.Helper_Notebook.ipynb`** — single reusable audit-metadata function used by every Bronze notebook:
```python
from pyspark.sql import functions as F

def add_timestamp_metadata(df):
    return (
        df.withColumn('ingestion_timestamp', F.current_timestamp())
          .withColumn('SourceFile', F.col('_metadata.file_path'))
    )
```

### Incremental pipeline — `00.Common/`
Config adds a `control_schema` and points at the `formula1_incr` catalog:
```python
catalog_name = 'formula1_incr'
landing_schema = 'landing'
silver_schema = 'silver'
bronze_schema = 'bronze'
gold_schema = 'gold'
control_schema = 'control'
landing_folder_path = '/Volumes/formula1_incr/landing/files'
```

Three dedicated helper notebooks (one per layer) replace the single batch helper:

**`02.Helper_Notebook_Bronze.ipynb`** — same `add_timestamp_metadata`, plus a batch-partitioned writer:
```python
def write_to_bronze(input_df, target_table, batch_id):
    final_df = input_df.withColumn("batch_id", F.lit(batch_id))
    final_df.write.format('delta').mode('overwrite') \
        .partitionBy('batch_id') \
        .option('replaceWhere', f"batch_id = '{batch_id}'") \
        .saveAsTable(target_table)
```
`replaceWhere` makes re-running a batch idempotent — it only overwrites the partition for that `batch_id`, leaving every other batch's data untouched.

**`03.Helper_Notebook_Silver.ipynb`** — create-or-MERGE upsert helper:
```python
def write_to_silver(input_df, target_table, merge_condition, columns_to_update):
    final_df = (input_df
        .withColumn("created_timestamp", F.current_timestamp())
        .withColumn("updated_timestamp", F.current_timestamp()))

    if not spark.catalog.tableExists(target_table):
        final_df.write.format('delta').mode('overwrite').saveAsTable(target_table)
    else:
        delta_table = DeltaTable.forName(spark, target_table)
        update_map = {c: f"s.{c}" for c in columns_to_update}
        update_map["updated_timestamp"] = "s.updated_timestamp"
        (delta_table.alias("t").merge(final_df.alias("s"), merge_condition)
            .whenMatchedUpdate(condition="s.batch_id>=t.batch_id", set=update_map)
            .whenNotMatchedInsertAll()
            .execute())
```
The `s.batch_id >= t.batch_id` guard on the match condition is what prevents an out-of-order or replayed batch from clobbering newer Silver data. `created_timestamp` is only ever set on insert (via `whenNotMatchedInsertAll`); `updated_timestamp` refreshes on every merge.

**`04.Helper_Notebook_Gold.ipynb`** — same create-or-merge pattern for Gold, but **without** the batch-id guard on the update condition (`whenMatchedUpdate(set=update_map)` — unconditional), since Gold dimension/fact rows are keyed by business keys rather than being watermarked the same way Silver is.

---

## 5. Bronze Layer

`02.Bronze_Ingestion_Notebooks/` — six notebooks, one per source file, each following: **`%run` config → `%run` helper → define path/table name → define schema → read → stamp metadata → write**.

| Notebook | Format | Schema handling |
|---|---|---|
| `01.Circuits_ingestion_notebook` | CSV | Explicit `StructType` (`circuitId`, `url`, `circuitname`, `lat`, `long`, `locality`, `country`) |
| `02.Races_ingestion_notebook` | CSV | `StructType` with `season`(Int), `round`(Int), `raceName`, `date`(Date), `circuitId` |
| `03.Constructors_ingestion_notebook` | Single-line JSON | DDL string schema `"constructorId STRING, name STRING, nationality STRING, url STRING"` + `.option("mode","FAILFAST")` |
| `04.Drivers_ingestion_notebook` | Nested JSON | Inner `name_schema` (`givenName`, `familyName`) nested inside `driver_schema` — preserved as a struct in Bronze |
| `05.Results_ingestion_notebook` | Folder of single-line JSON | Reads the whole `results/` folder path; explicit schema incl. `grid`, `laps`, `number`, `points`(Float), `position`, `positionText`, `status` |
| `06.Sprints_ingestion_notebook` | Folder of multi-line JSON | Same schema as Results + `.option("multiLine","true")` |

Full-refresh Bronze write (identical shape in every notebook):
```python
final_circuit = add_timestamp_metadata(circuits_df)
(final_circuit.write.format('delta').mode('overwrite').saveAsTable(table_name))
```

**Incremental Bronze** differs by adding a batch parameter and calling the partitioned writer instead:
```python
dbutils.widgets.text("p_batch_id", "")
v_batch_id = dbutils.widgets.get("p_batch_id")
...
source_file = f"{landing_folder_path}/{v_batch_id}/circuits.csv"
...
write_to_bronze(input_df=final_circuit, target_table=table_name, batch_id=v_batch_id)
```

---

## 6. Silver Layer

`03.Silver_Transformation_Notebooks/` — one notebook per dataset. Common shape: **read Bronze → select required columns → rename to snake_case → filter nulls / dropDuplicates on business key → initcap text fields → write**.

| Notebook | Renames | Dedup key | Value transforms |
|---|---|---|---|
| Circuits | `circuitId→circuits_id`, `circuitname→circuit_name`, `lat→latitude`, `long→longitude` | `circuits_id` | `initcap` on `circuit_name`, `locality` |
| Races | `raceName→race_name`, `circuitId→circuit_id`, `date→race_date` | `season, round` | `initcap` on `race_name` |
| Constructors | `constructorId→constructor_id`, `name→constructor_name` | `constructor_id` | `initcap` on `nationality` |
| Drivers | `driverId→driver_id`, `dateOfBirth→date_of_birth`; struct `name.givenName`+`name.familyName` concatenated into new `driver_name` via `F.concat_ws(" ", ...)` then dropped | `driver_id` | `initcap` on `driver_name`, `nationality` |
| Results | `raceName→race_name`, `constructorId→constructor_id`, `driverId→driver_id`, `date→race_date`, `grid→grid_position`, `laps→completed_laps`, `number→car_number`, `position→final_position`, `positionText→final_position_text` | `season, round, constructor_id, driver_id` (after filtering out nulls on all four) | `initcap` on `race_name` |
| Sprints | Same rename map as Results | Same composite key as Results | `initcap` on `race_name` |

Example (Circuits, batch pipeline):
```python
circuits_required_df = circuits_df.select(
    F.col("circuitId"), F.col("circuitname"), F.col("lat"), F.col("long"),
    F.col("locality"), F.col("country"), F.col("ingestion_timestamp"), F.col("SourceFile"))

circuits_renamed_df = circuits_required_df.withColumnsRenamed(
    {"circuitId":"circuits_id","circuitname":"circuit_name","lat":"latitude","long":"longitude","SourceFile":"source_file"})

circuits_clean_df = circuits_renamed_df.filter(F.col("circuits_id").isNotNull())
circuits_clean_df1 = circuits_clean_df.dropDuplicates(["circuits_id"])

circuits_final_df = (circuits_clean_df1
    .withColumn("circuit_name", F.initcap(F.col("circuit_name")))
    .withColumn("locality", F.initcap(F.col("locality"))))
```

**Incremental Silver** adds the batch filter and swaps the final `overwrite` write for the `write_to_silver()` MERGE helper:
```python
dbutils.widgets.text("p_batch_id", "")
v_batch_id = dbutils.widgets.get("p_batch_id")
circuits_df = spark.table(source_name).filter(F.col("batch_id") == v_batch_id)
...
write_to_silver(
    input_df=circuits_final_df,
    target_table=target_name,
    merge_condition="t.circuits_id=s.circuits_id",
    columns_to_update=['circuits_id','circuit_name','latitude','longitude','locality','country',
                        'ingestion_timestamp','source_file','batch_id']
)
```
Races Silver merges on the composite key: `merge_condition = "t.season=s.season AND t.round=s.round"`.

---

## 7. Gold Layer

`04.Gold_Dimensions_Notebooks/` — builds the star schema.

**`01.Building_Nationality_Region_ref.ipynb`** — a manually curated reference table (not sourced from Bronze/Silver) mapping ~40 nationalities to a `region` (Europe, North America, South America, Africa, Asia, Oceania), built via `spark.createDataFrame([Row(nationality=..., region=...), ...])` and written to `gold.ref_nationality_region`. Used to enrich both `dim_drivers` and `dim_constructors` with geography for regional analysis.

**`02.Building_Races_Dimesion_Table.ipynb`** — `dim_races`:
```python
dim_races_df = (
    races_df.join(circuits_df, races_df.circuit_id == circuits_df.circuits_id, "inner")
    .select(races_df.season, races_df.round, races_df.race_name, races_df.race_date,
            circuits_df.circuit_name, circuits_df.locality, circuits_df.country)
)
```
*(Note: the join key on the circuits side is `circuits_id`, matching the slightly non-standard column name carried over from the Silver Circuits table.)*

**`03.Building_Constructors_Dimesion_Table.ipynb`** — `dim_constructors`: left-joins Silver `constructors` to `ref_nationality_region` on `nationality`, aliasing `region` → `nationality_region`.

**`04.Building_Drivers_Dimesion_Table.ipynb`** — `dim_drivers`: same left-join pattern against `ref_nationality_region`, keeping `driver_id`, `driver_name`, `date_of_birth`, `nationality`, `nationality_region`.

**`05.Building_Results_Fact_Table.ipynb`** — `fact_session_results`: unifies Results and Sprints at the same grain.
```python
results_df = (spark.table(results_table)
    .withColumn("session_type", F.lit("RACE"))
    .drop("race_name","race_date","ingestion_timestamp","SourceFile"))

sprints_df = (spark.table(sprints_table)
    .withColumn("session_type", F.lit("SPRINT"))
    .drop("race_name","race_date","ingestion_timestamp","SourceFile"))

fact_session_df = results_df.unionByName(sprints_df)

fact_final_df = (fact_session_df
    .withColumn("is_win", F.col("final_position")==1)
    .withColumn("is_podium", F.col("final_position").between(1,3))
    .withColumn("has_points", F.col("points")>0))
```

**Incremental Gold** filters every Silver read on `batch_id` first, and writes via `write_to_gold(...)` MERGE instead of `overwrite` — e.g. `dim_races` merges on `"t.season=s.season AND t.round=s.round"`, `dim_constructors`/`dim_drivers` on their surrogate business key, and `fact_session_results` on the composite `season, round, constructor_id, driver_id, session_type`.

---

## 8. Analytics Views

`05.Analytics/`

**`v_driver_standings`** (season-level driver ranking):
```sql
CREATE OR REPLACE VIEW formula1.gold.v_driver_standings AS
WITH driver_session_summary AS (
  SELECT f.season, d.driver_id, d.driver_name, d.nationality,
         COUNT(*) AS race_starts,
         SUM(f.points) AS total_points,
         COUNT_IF(f.is_win) AS number_of_wins,
         COUNT_IF(f.is_podium) AS number_of_podiums
  FROM formula1.gold.fact_session_results f
  JOIN formula1.gold.dim_drivers d ON f.driver_id = d.driver_id
  GROUP BY f.season, d.driver_id, d.driver_name, d.nationality
)
SELECT season, driver_id, driver_name, nationality,
       RANK() OVER (PARTITION BY season ORDER BY total_points DESC, number_of_wins DESC) AS standings,
       race_starts, total_points, number_of_wins, number_of_podiums
FROM driver_session_summary
```

**`v_constructor_standings`** — identical pattern joined against `dim_constructors` on `constructor_id`.

**Dominant Driver / Constructor "greatness score"** (batch pipeline only — `03.Dominant_Driver_Query` / `04.Dominant_Constructor_Query`):
```sql
WITH driver_metrics AS (
  SELECT driver_name, SUM(race_starts) AS race_starts, SUM(number_of_wins) AS total_wins,
         SUM(number_of_podiums) AS total_podiums,
         SUM(CASE WHEN standings = 1 THEN 1 ELSE 0 END) AS total_championships
  FROM v_driver_standings
  GROUP BY driver_name
  HAVING total_championships >= 1
)
SELECT driver_name, race_starts, total_wins, total_podiums, total_championships,
       (total_championships*100) + (total_wins*10) + (total_podiums*3) AS greatness_score
FROM driver_metrics
ORDER BY greatness_score DESC;
```
The constructor version is the same query shape against `v_constructor_standings`. *(The incremental pipeline's `05.Analytics` folder currently only ships the two standings views — the dominance queries live in the batch pipeline.)*

---

## 9. Incremental-only: Orchestration

`06.Orchestration_Notebooks/` implements a lightweight **batch watermarking / control-table pattern**:

**`01.Creating_Control_Table.ipynb`** — creates the `control` schema and table:
```python
spark.sql(f"CREATE SCHEMA IF NOT EXISTS {catalog_name}.{control_schema}")
spark.sql(f"""CREATE TABLE IF NOT EXISTS {catalog_name}.{control_schema}.batch_control(
    batch_id STRING, status STRING, created_timestamp TIMESTAMP, updated_timestamp TIMESTAMP
)""")
```

**`02.Identify_Next_Batch.ipynb`** — diffs landing-zone batch folders against already-tracked batches:
```python
landing_batches = sorted([f.name.rstrip("/") for f in dbutils.fs.ls(landing_folder_path) if f.isDir()])

if spark.catalog.tableExists(control_table):
    tracked_batches = [r.batch_id for r in spark.table(control_table)
        .filter(F.col("status").isin("in_progress","completed"))
        .select("batch_id").distinct().collect()]
else:
    tracked_batches = []

new_batches = sorted(list(set(landing_batches) - set(tracked_batches)))
next_batch = new_batches[0] if new_batches else None

if next_batch is None:
    dbutils.jobs.taskValues.set(key="p_batch_id", value="")
    dbutils.jobs.taskValues.set(key="has_batch", value="false")
else:
    dbutils.jobs.taskValues.set(key="p_batch_id", value=next_batch)
    dbutils.jobs.taskValues.set(key="has_batch", value="true")
```
`dbutils.jobs.taskValues` publishes the discovered `batch_id` so downstream job tasks (and a **conditional task**) can consume it.

**`03.Create_New_Batch.ipynb`** — marks the batch `in_progress` (append-only insert):
```python
if v_batch_id:
    in_progress_df = (spark.createDataFrame([Row(batch_id=v_batch_id, status="in_progress")])
        .withColumn("created_timestamp", F.current_timestamp())
        .withColumn("updated_timestamp", F.current_timestamp()))
    in_progress_df.write.format("delta").mode("append").saveAsTable(control_table)
else:
    raise Exception("batch_id is missing")
```

**`04.Complete_Batch.ipynb`** — flips the batch to `completed` via a guarded MERGE:
```python
delta_table = DeltaTable.forName(spark, control_table)
source_df = (spark.createDataFrame([(v_batch_id,)], ["batch_id"])
    .withColumn("status", F.lit("completed"))
    .withColumn("updated_timestamp", F.current_timestamp()))

(delta_table.alias("t").merge(source_df.alias("s"), "t.batch_id=s.batch_id AND t.status='in_progress'")
    .whenMatchedUpdate(set={"status":"s.status","updated_timestamp":"s.updated_timestamp"})
    .execute())
```

**End-to-end flow:** *Identify Next Batch* → (conditional: `has_batch == true`) → *Create New Batch* → run the Bronze→Silver→Gold medallion tasks for that `batch_id` → *Complete Batch*.

---

## 10. Jobs / Workflow Orchestration

- Both pipelines are orchestrated with **Databricks Workflows (Jobs)**, on a dedicated job cluster to control cost.
- The incremental job passes `p_batch_id` as a **job/task-level parameter** into every notebook task, and uses a **conditional task** gated on the `has_batch` flag published by `Identify_Next_Batch`, so the medallion pipeline only runs when there's actually a new batch to process — a self-driving, idempotent design.

# Full refresh Job
<img width="940" height="534" alt="image" src="https://github.com/user-attachments/assets/1a0b4a5c-c429-4f2d-bdec-a19479f318a2" />

<img width="631" height="639" alt="image" src="https://github.com/user-attachments/assets/cad0c6a1-c69e-4921-960a-e3dd8b53ebff" />

# Orchestration Job for incremental processing
<img width="841" height="150" alt="image" src="https://github.com/user-attachments/assets/42c72be0-17a1-4349-8830-313e698149ae" />

---

## 11. Dashboard

`F1_Project_Analytics_Dashboard.lvdash.json` — a **Databricks Lakeview dashboard** built on the Gold views (`v_driver_standings`, `v_constructor_standings`) and the greatness-score queries, with four pages:
1. Driver standings — table + championship pie chart + total-points bar chart
2. Constructor standings — same layout at the constructor level
3. Dominant drivers — greatness-score table, championship-share pie chart, greatness-score bar chart
4. Dominant constructors — same at the constructor level

<img width="940" height="514" alt="image" src="https://github.com/user-attachments/assets/165c3057-77a8-45f9-a4dd-984a8f4bee59" />

<img width="940" height="465" alt="image" src="https://github.com/user-attachments/assets/81fefa35-0080-47c9-b805-844296bb79a3" />

<img width="940" height="497" alt="image" src="https://github.com/user-attachments/assets/e03bbe35-00cc-4502-9e19-0bed90d2b24d" />

<img width="940" height="478" alt="image" src="https://github.com/user-attachments/assets/264eb505-8df1-4692-a1cd-12d4f2da25c4" />

---

## 12. Batch vs Incremental — Key Differences

| Aspect | Full Batch (`f1_project_batch`) | Incremental Batch (`f1_project_incremental_batch`) |
|---|---|---|
| Catalog | `formula1` | `formula1_incr` |
| ADLS container | `f1project` | `f1project-incr` |
| Common helpers | 1 notebook (`add_timestamp_metadata`) | 3 notebooks (Bronze/Silver/Gold writers) |
| Bronze write | `mode('overwrite')` full table replace | `partitionBy('batch_id')` + `replaceWhere` on that batch only |
| Silver/Gold write | `mode('overwrite')` full table replace | `DeltaTable.merge()` upsert, batch-guarded on Silver |
| Batch parameter | None | `dbutils.widgets` → `p_batch_id` in every notebook |
| Orchestration extras | None | `control.batch_control` table + Identify/Create/Complete-batch notebooks |
| Analytics | Standings views **+** dominance ("greatness score") queries | Standings views only |
| Audit columns | `ingestion_timestamp`, `SourceFile` | Adds `batch_id`, `created_timestamp`, `updated_timestamp` |

---

## Links
- Repository: https://github.com/mtj-08/f1_project_repo
- Full batch pipeline: `f1_project_batch/`
- Incremental pipeline: `f1_project_incremental_batch/`
- Dashboard: `F1_Project_Analytics_Dashboard.lvdash.json`
