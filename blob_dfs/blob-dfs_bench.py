import pyspark
from pyspark.sql import SparkSession

spark = SparkSession.builder.appName('iceberg_lab') \
.config("spark.executor.memory", "1g") \
.config("spark.driver.memory", "8g") \
.config('spark.jars.packages', 'org.apache.iceberg:iceberg-spark-runtime-3.5_2.12:1.4.1,org.apache.iceberg:iceberg-azure-bundle:1.4.1') \
.config('spark.sql.extensions', 'org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions') \
.config('spark.sql.defaultCatalog', 'opencatalog') \
.config('spark.sql.catalog.opencatalog', 'org.apache.iceberg.spark.SparkCatalog') \
.config('spark.sql.catalog.opencatalog.type', 'rest') \
.config('spark.sql.catalog.opencatalog.header.X-Iceberg-Access-Delegation','vended-credentials') \
.config('spark.sql.catalog.opencatalog.uri','https://pxlrpte-vs41448open.snowflakecomputing.com/polaris/api/catalog') \
.config('spark.sql.catalog.opencatalog.credential','4Qdi+HwwOAIBmt2zDe2s8LktPCc=:yoursecret') \
.config('spark.sql.catalog.opencatalog.warehouse','open_spark_dfs') \
.config('spark.sql.catalog.opencatalog.scope','PRINCIPAL_ROLE:spark') \
.getOrCreate()

import time, json, os, random
from pyspark.sql import SparkSession, functions as F, types as T

# ======= USER KNOBS =======
TARGET = "blob"  # "dfs" or "blob"
CATALOG = "open_spark"
#TARGET = "dfs"
#CATALOG = "open_spark_dfs"                      # matches spark.sql.catalog.<name>
DB = "bench"
TABLE = "tx_events"

SCALE_ROWS = int(os.getenv("SCALE_ROWS", 100_000_000))  # adjust for cluster; e.g., 100M ~ ~100GB depending on schema
PARTITION_BY_DAYS = True
BUCKETS = 16

REPETITIONS = 3  # do each query 3x, use median

# ==========================

#spark = SparkSession.builder.getOrCreate()
spark.sql(f"CREATE NAMESPACE IF NOT EXISTS {CATALOG}.{DB}")

schema = T.StructType() \
    .add("user_id", T.LongType()) \
    .add("ts", T.TimestampType()) \
    .add("amount", T.DoubleType()) \
    .add("city", T.StringType()) \
    .add("category", T.StringType())

def synthesise(n):

    # Deterministic synthetic generator
    df = spark.range(n).withColumnRenamed("id", "user_id")
    
    # 2025-01-01 00:00:00 UTC → epoch seconds
    EPOCH_BASE = 1735689600
    offset = (F.col("user_id") % (60*60*24*30)).cast("long")
    df = df.withColumn("ts",
        F.to_timestamp(F.from_unixtime(F.lit(EPOCH_BASE) + offset))
    )
    
    df = df.withColumn("amount", (F.rand(seed=42)*1000.0).cast("double"))
    cities = F.array([F.lit(c) for c in ["Paris","Seoul","Tokyo","Lyon","Lille","Marseille","Nantes","Bordeaux"]])
    cats = F.array([F.lit(c) for c in ["A","B","C","D","E"]])
    df = df.withColumn("city", cities[(F.col("user_id") % F.size(cities)).cast("int")]) \
           .withColumn("category", cats[(F.col("user_id") % F.size(cats)).cast("int")])
    return df.select("user_id","ts","amount","city","category")

table_ident = f"{CATALOG}.{DB}.{TABLE}"

# Drop & (re)create table with same partition spec for both targets
spark.sql(f"DROP TABLE IF EXISTS {table_ident}")

partition_spec = "PARTITIONED BY (days(ts), bucket({b}, user_id))".format(b=BUCKETS) if PARTITION_BY_DAYS else "PARTITIONED BY (bucket({b}, user_id))".format(b=BUCKETS)

spark.sql(f"""
  CREATE TABLE {table_ident} (
    user_id BIGINT,
    ts TIMESTAMP,
    amount DOUBLE,
    city STRING,
    category STRING
  )
  USING iceberg
  {partition_spec}
  TBLPROPERTIES (
    'write.target-file-size-bytes'='134217728',
    'format-version'='2'
  )
""")

def timer(fn):
    t0 = time.perf_counter()
    res = fn()
    t1 = time.perf_counter()
    return res, t1 - t0

def median(xs):
    s = sorted(xs)
    n = len(s)
    return 0 if n==0 else (s[n//2] if n%2==1 else 0.5*(s[n//2-1]+s[n//2]))

results = []

# 1) WRITE (bulk append)
df = synthesise(SCALE_ROWS).repartition(200)  # adjust to your cluster
_, dur = timer(lambda: df.writeTo(table_ident).append())
results.append({"phase":"write_append","target":TARGET,"seconds":dur})

# 2) READS (repeat 3x each, record median)
def bench_sql(name, sql):
    times = []
    for _ in range(REPETITIONS):
        _, d = timer(lambda: spark.sql(sql).agg(F.sum("cnt")).collect() if "cnt" in sql else spark.sql(sql).collect())
        times.append(d)
    results.append({"phase":name,"target":TARGET,"seconds":median(times)})

# Q1: Partition pruned (restrict to a couple of days)
bench_sql("read_pruned", f"""
  SELECT city, count(*) as cnt
  FROM {table_ident}
  WHERE ts >= '2025-01-05' AND ts < '2025-01-07'
  GROUP BY city
""")

# Q2: Wide aggregation
bench_sql("read_agg", f"""
  SELECT category, approx_percentile(amount, 0.95) as p95, count(*) as cnt
  FROM {table_ident}
  GROUP BY category
""")

# Q3: High selectivity lookup
bench_sql("read_lookup", f"""
  SELECT *
  FROM {table_ident}
  WHERE user_id IN (123, 456789, 987654321)
""")

# 3) MAINTENANCE
# Compaction (data files)
_, dur = timer(lambda: spark.sql(f"""
  CALL opencatalog.system.rewrite_data_files(table => '{table_ident}', options => map('min-input-files','50','max-file-size-bytes','536870912'))
""").collect())
results.append({"phase":"rewrite_data_files","target":TARGET,"seconds":dur})

# Rewrite manifests
_, dur = timer(lambda: spark.sql(f"""
  CALL opencatalog.system.rewrite_manifests('{table_ident}')
""").collect())
results.append({"phase":"rewrite_manifests","target":TARGET,"seconds":dur})

# Expire snapshots (keep last 2)
_, dur = timer(lambda: spark.sql(f"""
  CALL opencatalog.system.expire_snapshots(table => '{table_ident}', retain_last => 2)
""").collect())
results.append({"phase":"expire_snapshots","target":TARGET,"seconds":dur})

# Save results (CSV)
out_path = f"/tmp/iceberg_ab_{TARGET}_results.json"
with open(out_path, "w") as f:
    json.dump(results, f, indent=2)

print(json.dumps(results, indent=2))