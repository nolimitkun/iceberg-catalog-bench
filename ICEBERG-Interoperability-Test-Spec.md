## Iceberg Cross-Engine Interoperability Test Specification

### 1. Purpose and Scope
- **Goal**: Validate functional correctness, interoperability, and performance when reading and writing Apache Iceberg v2 tables on ADLS Gen2 using multiple compute engines and catalogs.
- **Engines under test**: Apache Spark (OSS or Synapse/Spark runtime), Snowflake, Azure Databricks.
- **Catalogs under test**: Snowflake Polaris (Open Catalog) and Azure Databricks Unity Catalog (UC).
- **Storage**: Azure Data Lake Storage Gen2 (ADLS Gen2), hierarchical namespace enabled.
- **Out of scope**: Non-Iceberg table formats, GCS/S3 storage, on-premises HDFS.

### 2. High-Level Objectives
- **Interoperability**: Create/alter/write with Engine A, read/update with Engine B/C across catalogs.
- **Correctness**: Ensure results equivalence, snapshot lineage integrity, schema/partition evolution compatibility, ACID guarantees.
- **Performance**: Measure read/write throughput and latency across representative workloads and data sizes.
- **Operability**: Validate observability, access control, failure recovery, and maintenance tasks (compaction, snapshot expiration).

### 3. Architecture Under Test
- **Storage**: ADLS Gen2 account with containers for `iceberg-warehouse` and `staging`.
- **Catalogs**:
  - Snowflake Polaris (Open Catalog): catalog service hosting Iceberg table metadata; configured to point to ADLS external locations.
  - Azure Databricks Unity Catalog: metastore with external locations and storage credentials mapped to ADLS.
- **Engines**:
  - Apache Spark 3.4+ with Iceberg runtime 1.4+ (Spark SQL catalog configured to Polaris or UC via REST/URI).
  - Snowflake: Snowflake account with Iceberg external tables and/or Polaris Catalog integration to ADLS.
  - Azure Databricks: Runtime 14+ or DBR 13.3 LTS+ with UC-enabled workspace.

### 4. Environments and Versions Matrix
- **Iceberg**: Table format version v2, distribution mode default.
- **Spark**: 3.4.x/3.5.x; Iceberg runtime 1.4.x; Hadoop Azure 3.3.x.
- **Databricks**: 13.3 LTS/14.x (Photon on/off), UC GA; DB Connect for automated tests.
- **Snowflake**: Current GA supporting Polaris/Open Catalog integration and Iceberg external table capabilities.
- **ADLS**: Gen2, HNS enabled; encryption with Microsoft-managed keys (MMK) initially; test optional CMK scenario.

### 5. Security and Access Control
- **ADLS Access**: Use service principals (SPN) with RBAC: Storage Blob Data Contributor on the container.
- **Databricks UC**: Configure Storage Credential and External Location mapped to SPN; table ACLs managed via UC grants.
- **Snowflake**: Storage Integration to ADLS; External Volume mapping; roles with privileges on Iceberg tables and stages.
- **Network**: Public endpoints initially; optionally test Private Endpoints/VNet injection later.

### 6. Test Data Sets
- **Synthetic**:
  - Small: 10M rows, 10 columns (mix of int, bigint, string, timestamp, decimal). Skewed and uniform variants.
  - Medium: 100M rows, 25 columns. Partitionable dimensions (date, country, tenant_id).
- **TPC-DS subset**: Tables `store_sales`, `item`, `date_dim` scaled at 100GB for read benchmarks.
- **Data properties**: Include nulls, high-cardinality strings, nested structs (for Spark/Databricks), and arrays (optional interop coverage where supported).

### 7. Table Design and Properties
- **Namespace**: `interopspec`.
- **Baseline table**: `sales_events`
  - Columns: `event_id bigint`, `tenant_id int`, `event_ts timestamp`, `sku string`, `qty int`, `price decimal(18,2)`, `country string`, `ds date`.
  - Partition spec v2: `days(event_ts)`, `bucket(tenant_id, 16)`.
  - Sort order (where supported): `event_ts`, `tenant_id`.
  - Table properties: `write.distribution-mode=hash`, `commit.manifest.min-count-to-merge=100`, `format-version=2`.

### 8. Test Matrix (Interoperability)
- **Create with Spark → Read with Snowflake / Databricks**
- **Create with Databricks (UC) → Read with Snowflake / Spark**
- **Create with Snowflake (Polaris) → Read with Spark / Databricks**
- **Write/Update/Delete alternation across engines**:
  1. Engine A: initial create and bulk insert
  2. Engine B: schema add column, append
  3. Engine C: update/delete/merge
  4. Engine A: time-travel read; validate snapshot history

### 9. Functional Test Cases
- **Creation**:
  - Create database/namespace; create Iceberg v2 table with partition + sort.
  - Validate table metadata existence in chosen catalog and manifests in ADLS.
- **Ingestion**:
  - Bulk append (COPY/INSERT) of 10M/100M rows; verify snapshot count and data files.
  - Idempotent re-runs should not corrupt metadata.
- **DML**:
  - `MERGE INTO` upsert path (20% updates, 5% deletes, 75% inserts).
  - `UPDATE` set scalar column; `DELETE` with equality predicates.
  - Row-level deletes (equality and position deletes) where supported.
- **Schema Evolution**:
  - Add column with default; backfill optional.
  - Rename column; drop column; change column type widening (int→bigint).
- **Partition Evolution**:
  - Add `truncate(sku, 8)`; remove `bucket(tenant_id,16)`; verify engine compatibility.
- **Snapshot and Time Travel**:
  - List snapshots; query by `as of timestamp` and by snapshot-id across engines.
- **Concurrency & Isolation**:
  - Concurrent writers (2-4 jobs) append; verify no commit conflicts or data loss.
- **Maintenance**:
  - Optimize/compact (rewrite data files); expire snapshots; remove orphan files; validate post-maintenance readability across engines.
- **Permissions**:
  - Revoke read/write then restore; ensure correct error surfaces and recovery.

### 10. Performance Benchmarks
- **KPIs**:
  - Bulk write throughput (MB/s, rows/s) for initial load and merge.
  - Read latency P50/P95 for selective and full scans.
  - Cost per TB processed (Databricks DBUs, Snowflake credits, Spark vCores).
- **Workloads**:
  - Scan with partition pruning (`ds=YYYY-MM-DD`).
  - Aggregations (group by `country, ds`).
  - Point lookups by `event_id` and `tenant_id`.
- **Run modes**: Warm cache vs cold cache; Photon on/off; Snowflake warehouse sizes S/M/L.

### 11. Validation and Correctness
- **Row-count parity** across engines at each snapshot.
- **Checksum** of stable columns (e.g., `sum(qty)`, `sum(price*qty)`).
- **Result equivalence**: Canonical query suite with ordered comparisons.
- **Metadata checks**: Partition spec, sort order, snapshot lineage length, orphan files = 0 after maintenance.
- **Schema registry**: Verify column types and nullability after evolution steps in all catalogs.

### 12. Failure Injection
- **Network**: Temporary ADLS permission removal mid-commit.
- **Concurrency**: Force commit conflict by overlapping writers.
- **Resource**: Reduce cluster/warehouse size during merge to trigger retries.
- **Catalog**: Kill session/token expiration during write; ensure retries or expected failure.

### 13. Test Automation Strategy
- **Harness**: Python (pytest) orchestrator with a config-driven matrix.
- **Connectors**:
  - Spark: `pyspark` with Iceberg session extensions and Hadoop Azure connectors.
  - Databricks: REST Jobs API (or `databricks-sdk-python`) to submit notebooks, UC grants bootstrap.
  - Snowflake: `snowflake-connector-python` and `snowflake-snowpark-python` for SQL/DML, and Polaris catalog operations where applicable.
- **Data generation**: `faker`/`mimesis` for synthetic; optional TPC-DS dataset pre-generated and uploaded to ADLS staging.
- **Configuration**: YAML describing engines, catalogs, credentials, tables, and workloads.
- **Idempotency**: Unique namespace per test run (timestamp suffix); cleanup on success/failure.

### 14. Configuration Examples
- **Spark (Iceberg on ADLS via UC or Polaris)**
```bash
spark-shell \
  --packages org.apache.iceberg:iceberg-spark-runtime-3.4_2.12:1.4.3,org.apache.hadoop:hadoop-azure:3.3.4,com.azure:azure-identity:1.11.1 \
  --conf spark.sql.extensions=org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions \
  --conf spark.sql.catalog.interop=org.apache.iceberg.spark.SparkCatalog \
  --conf spark.sql.catalog.interop.catalog-impl=org.apache.iceberg.rest.RESTCatalog \
  --conf spark.sql.catalog.interop.uri=$CATALOG_URI \# Polaris or UC REST endpoint \
  --conf spark.sql.catalog.interop.warehouse=abfss://iceberg-warehouse@$ACCOUNT.dfs.core.windows.net/ \
  --conf spark.hadoop.fs.azure.account.auth.type=$AUTH_TYPE \
  --conf spark.hadoop.fs.azure.account.oauth2.client.id=$CLIENT_ID \
  --conf spark.hadoop.fs.azure.account.oauth2.client.secret=$CLIENT_SECRET \
  --conf spark.hadoop.fs.azure.account.oauth2.client.endpoint=$TENANT_OAUTH_TOKEN_ENDPOINT
```

- **Databricks (Unity Catalog external location)**
```sql
CREATE STORAGE CREDENTIAL spn_iceberg
WITH AZURE_SERVICE_PRINCIPAL (CLIENT_ID '$CLIENT_ID', CLIENT_SECRET '$CLIENT_SECRET')
COMMENT 'SPN for ADLS access';

CREATE EXTERNAL LOCATION iceberg_wh
URL 'abfss://iceberg-warehouse@$ACCOUNT.dfs.core.windows.net/'
WITH (STORAGE CREDENTIAL spn_iceberg);

CREATE CATALOG interop_uc MANAGED LOCATION 'abfss://iceberg-warehouse@$ACCOUNT.dfs.core.windows.net/interop_uc/';
GRANT USAGE ON CATALOG interop_uc TO `account users`;
```

- **Snowflake (Storage Integration + Iceberg external volume)**
```sql
CREATE STORAGE INTEGRATION adls_iceberg
  TYPE = EXTERNAL_STAGE
  STORAGE_PROVIDER = AZURE
  ENABLED = TRUE
  AZURE_TENANT_ID = '<tenant-id>'
  STORAGE_ALLOWED_LOCATIONS = ('abfss://iceberg-warehouse@$ACCOUNT.dfs.core.windows.net/');

CREATE EXTERNAL VOLUME adls_ev
  STORAGE_INTEGRATION = adls_iceberg
  ENABLED = TRUE
  STORAGE_LOCATIONS = ('abfss://iceberg-warehouse@$ACCOUNT.dfs.core.windows.net/');

-- Using Polaris/Open Catalog where available to register the Iceberg table metadata
-- Alternative: create Iceberg external table referencing existing metadata location
```

### 15. Test Procedures (Representative)
1. **Bootstrap**
   - Provision or reference ADLS containers.
   - Configure UC external location and Snowflake storage integration.
   - Create catalogs/namespaces: `interop_uc`, `interop_polaris`.
2. **Create Table (Engine A)**
   - Create `interopspec.sales_events` with partition + sort; insert 10M rows.
   - Record snapshot-id `S1` and manifest counts.
3. **Read Validate (Engine B)**
   - Read snapshot `S1`, verify row count and checksums.
4. **Schema Evolution (Engine B)**
   - Add column `channel string default 'web'`; rename `sku` → `product_sku`.
   - Append 5M rows; record snapshot `S2`.
5. **DML (Engine C)**
   - `MERGE INTO` with 1M updates, 250k deletes; record snapshot `S3`.
6. **Time Travel (Engine A)**
   - Query `AS OF` `S1` and `S2`; validate counts and checksums.
7. **Maintenance**
   - Compact small files; expire snapshots older than `S2`; remove orphan files.
8. **Interleave**
   - Swap engine roles and repeat with medium dataset.

### 16. Benchmark Execution
- Warm-up one run; measure next three; report median and P95.
- Capture engine-specific metrics: Spark (Stage metrics), Databricks (Ganglia/metrics, query profile), Snowflake (QUERY_HISTORY, CREDITS_USED).
- Store results in a simple table (`results.run_metrics`).

### 17. Reporting and Acceptance Criteria
- **Pass** when:
  - All cross-engine reads succeed after each write/evolution step.
  - Checksums/row counts match expected at each snapshot.
  - No orphan files remain after maintenance.
  - Performance within ±20% across engines for equivalent configurations, or documented rationale.
- **Deliverables**:
  - Test report with functional results and performance graphs.
  - Catalog metadata snapshots and ADLS manifest listings at key steps.
  - Automated test harness code and runbook.

### 18. Risks and Mitigations
- **Feature parity gaps** (e.g., specific partition transforms, sort orders): prefer widely supported transforms; document deviations.
- **Catalog API differences**: separate adapters; use RESTCatalog where possible.
- **Auth differences**: standardize on SPN/OAuth; abstract credentials in config.
- **Large file counts**: schedule compaction; tune write target file size.

### 19. Directory Layout (Proposed)
```
/iceberg-tests/
  config/
    engines.yaml
    datasets.yaml
  scripts/
    spark_jobs/
    databricks/
    snowflake/
  results/
    metrics/
    logs/
  docs/
    runbook.md
```

### 20. Next Steps
- Finalize target versions and feature flags per environment.
- Stand up minimal ADLS + UC + Polaris connectivity.
- Implement automation harness and seed datasets.
- Execute small dataset interop run, iterate on gaps, then scale to medium.
