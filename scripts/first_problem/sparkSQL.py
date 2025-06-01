from pyspark.sql import SparkSession
from pyspark.sql.functions import col, count, min, max, avg, collect_set, collect_list, struct, lower, regexp_replace, col, regexp_extract, to_date
from pyspark.sql import functions as F
from pyspark.sql.types import *
import time


spark = SparkSession.builder \
    .appName("CarBrandStats") \
    .config("spark.executor.memory", "4g") \
    .config("spark.sql.parquet.filterPushdown", "true") \
    .config("spark.sql.parquet.mergeSchema", "false") \
    .config("spark.sql.shuffle.partitions", "4") \
    .getOrCreate()


fractions = [
    (1.0, "100% (1 GB completo)"),
    (0.5, "50% (~0.5 GB)"),
    (0.25, "25% (~0.25 GB)")
]

df = spark.read \
    .option("header", "true") \
    .option("delimiter", ",") \
    .option("quote", '"') \
    .option("escape", '\\') \
    .option("multiLine", "true") \
    .option("inferSchema", "true") \
    .parquet("hdfs://localhost:9000/user/spark/output2/used_cars_data_cleaned")


regex_make = r"^[a-z]+(?:[ -][a-z]+)*$"


df_clean = df.filter(
    F.col("make_name").isNotNull() &
    F.col("model_name").isNotNull() &
    F.col("price").isNotNull() &
    F.col("year").isNotNull()
).select(
    "make_name",
    "model_name",
    F.col("price").cast("float").alias("price_float"),
    F.col("year").cast("int").alias("year_int")
)


benchmark_results = {}

for frac, label in fractions:
    df_subset = df_clean.sample(withReplacement=False, fraction=frac, seed=42)
    count_subset = df_subset.count()
    
    t0 = time.time()

    df_brands_distinct = df_clean.select("make_name") \
        .distinct() \
        .filter(F.col("make_name").isNotNull())

    df_brands_distinct.show(truncate=False)

    model_stats = df_clean.groupBy("make_name", "model_name").agg(
        F.count("*").alias("count"),
        F.min("price_float").alias("min_price"),
        F.max("price_float").alias("max_price"),
        F.avg("price_float").alias("avg_price"),
        F.countDistinct("year_int").alias("total_years"),
        #F.min("year_int").alias("min_year"),
        F.sort_array(F.collect_set("year_int"), asc=True).alias("years_available")
    )


    valid_stats = model_stats.join(df_brands_distinct, on="make_name", how="inner")

    valid_stats = valid_stats.filter(
        (F.col("min_price").isNotNull()) &                        
        (F.col("max_price").isNotNull()) &                        
        (~F.col("model_name").isin("true", "false"))               
    )
    num_valid_groups = valid_stats.count()

    elapsed = time.time() - t0
    
    benchmark_results[label] = (elapsed, num_valid_groups, count_subset)

    
    print(f"\n--- Muestra de resultados para {label} ---")
    valid_stats.show(10, truncate=False, vertical=True)
    #valid_stats.show(20, truncate=False, vertical=True)


print("\n=== Comparativa de tiempos (dataset 1 GB vs subsets) ===")
print(f"{'Dataset':<20} {'Filas':>10} {'Grupos válidos':>15} {'Tiempo [s]':>12}")
print("-" * 60)
for lbl, (secs, num_groups, num_rows) in benchmark_results.items():
    print(f"{lbl:<20} {num_rows:10d} {num_groups:15d} {secs:12.2f}")



# valid_stats.coalesce(1) \
#       .write \
#       .mode("overwrite") \
#       .json("hdfs://localhost:9000/user/spark/output/analysis1_sparksql")


spark.stop()