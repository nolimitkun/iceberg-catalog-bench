# Blob DFS Benchmark

This directory contains a standalone PySpark benchmark (`blob-dfs_bench.py`) that exercises an Apache Iceberg table through a Polaris REST catalog. It is intended to compare storage backends (e.g., cloud blob vs distributed file system) by replaying the same workload and capturing timings for writes, reads, and maintenance procedures.

## Prerequisites
- Python environment with `pyspark` available on the path.
- Spark runtime able to reach your Iceberg catalog and storage accounts. Update the `SparkSession.builder` block in the script with the appropriate `spark.sql.catalog` properties, credentials, and package versions for your environment.
- Sufficient cluster resources; the default scale writes ~100M synthetic rows.

## Configuration knobs
`blob-dfs_bench.py` exposes a handful of constants near the top of the file:
- `TARGET`: label that identifies the storage backend under test (`"blob"` or `"dfs"`). It is only used in the metrics output path and JSON payload.
- `CATALOG`, `DB`, `TABLE`: name the Iceberg catalog, namespace, and table that will be created/dropped by the benchmark.
- `SCALE_ROWS`: environment variable (default `100_000_000`) controlling how many synthetic rows are generated.
- `PARTITION_BY_DAYS`: toggle daily partitioning by `ts` in addition to bucketing.
- `BUCKETS`: number of buckets for `user_id` partitioning.
- `REPETITIONS`: how many times each query is executed when calculating the median latency.

You can override `SCALE_ROWS` at runtime, for example:

```bash
SCALE_ROWS=5000000 python blob-dfs_bench.py
```

Adjust `repartition(200)` in the write section if your cluster requires a different number of shuffle partitions.

## What the script does
1. Creates the target namespace and recreates the benchmark table with an Iceberg v2 layout.
2. Synthesises a deterministic transaction-like dataset with timestamps, amounts, cities, and categories.
3. Times a bulk append write followed by three representative read patterns:
   - Partition-pruned aggregate over a narrow date window.
   - Wide aggregation querying the full table.
   - Point-lookups on a handful of `user_id` values.
4. Runs Iceberg maintenance procedures (`rewrite_data_files`, `rewrite_manifests`, `expire_snapshots`).
5. Stores the timing summaries in `/tmp/iceberg_ab_<target>_results.json` and prints the same JSON to stdout.

## Running the benchmark
1. Ensure your catalog configuration in the script is valid, including any delegated credentials or scopes.
2. (Optional) Set `SCALE_ROWS` or tweak other knobs to fit your cluster size.
3. Execute the script with Python:
   ```bash
   python blob-dfs_bench.py
   ```
4. Inspect the console output or the JSON file under `/tmp` for timing comparisons.

## Operational notes
- The job drops and recreates the table each run; do not point it at a production table.
- Maintenance calls assume the catalog is registered as `opencatalog`. Rename the `CALL` statements if you adjust the builder configuration.
- Replace the placeholder credential string in the builder with a secure secret management solution before sharing or checking the script into a broader repository.
