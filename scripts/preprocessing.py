from pyspark.sql import SparkSession
from pyspark.sql.types import IntegerType, FloatType
from pyspark.sql.functions import lower, regexp_replace, col, regexp_extract
import time

spark = SparkSession.builder \
    .appName("Used Cars Preprocessing - Limited Memory") \
    .master("local[2]") \
    .config("spark.driver.memory", "4g") \
    .config("spark.executor.memory", "4g") \
    .config("spark.driver.maxResultSize", "1g") \
    .config("spark.sql.shuffle.partitions", "4000") \
    .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer") \
    .config("spark.sql.adaptive.enabled", "true") \
    .config("spark.sql.adaptive.coalescePartitions.enabled", "true") \
    .getOrCreate()

def process_data_chunk(year_filter=None, make_filter=None):
    df_builder = spark.read \
        .option("header", "true") \
        .option("delimiter", ",") \
        .option("quote", '"') \
        .option("escape", '\\') \
        .option("multiLine", "true") \
        .option("inferSchema", "true") \
        .csv("hdfs://localhost:9000/user/spark/input/used_cars_data.csv") 

    if year_filter:
        df_builder = df_builder.filter(col("year") == year_filter)
    if make_filter:
        df_builder = df_builder.filter(col("make_name").like(f"%{make_filter}%"))

    df = df_builder.repartition(4)
    record_count = df.count()
    if record_count == 0:
        return spark.createDataFrame([], df.schema)

    df_transformed = df.select(
        "vin", "daysonmarket", "price", "year", "city",
        "make_name", "model_name", "description",
        "engine_displacement", "engine_type", "engine_cylinders", "power", "torque",
        lower(regexp_replace(col("city"), "[^a-zA-Z0-9\\s]", "")).alias("city_clean"),
        lower(regexp_replace(col("make_name"), "[^a-zA-Z0-9\\s]", "")).alias("make_name_clean"),
        lower(regexp_replace(col("model_name"), "[^a-zA-Z0-9\\s]", "")).alias("model_name_clean"),
        regexp_extract(col("engine_cylinders"), "(\\d+)", 1).cast(IntegerType()).alias("engine_cylinders_num"),
        regexp_extract(col("engine_displacement"), "(\\d+)", 1).cast(FloatType()).alias("engine_displacement_num"),
        regexp_extract(col("power"), "(\\d+)(\\s*[Hh][Pp]|$)", 1).cast(FloatType()).alias("power_clean"),
        regexp_extract(col("torque"), "(\\d+)(\\s*[Ll]b[-]?[Ff]t|$)", 1).cast(FloatType()).alias("torque_clean"),
        lower(regexp_replace(col("description"), "[^\\w\\s]", "")).alias("description_clean"),
    )

    df_cleaned = df_transformed \
        .drop("city", "make_name", "model_name", 
              "engine_cylinders", "engine_displacement", 
              "description", "power", "torque") \
        .withColumnRenamed("city_clean", "city") \
        .withColumnRenamed("make_name_clean", "make_name") \
        .withColumnRenamed("model_name_clean", "model_name") \
        .withColumnRenamed("engine_displacement_num", "engine_displacement") \
        .withColumnRenamed("engine_cylinders_num", "engine_cylinders") \
        .withColumnRenamed("description_clean", "description") \
        .withColumnRenamed("power_clean", "power") \
        .withColumnRenamed("torque_clean", "torque")

    return df_cleaned.dropDuplicates(["vin"])

def run_full_processing():
    df_sample = spark.read \
        .option("header", "true") \
        .option("delimiter", ",") \
        .option("quote", '"') \
        .option("escape", '"') \
        .option("multiLine", "true") \
        .option("inferSchema", "true") \
        .csv("hdfs://localhost:9000/user/spark/input/used_cars_data.csv")

    available_years = [row["year"] for row in df_sample.select("year").distinct().collect() if row["year"] is not None]
    available_years.sort()

    total_processed_records = 0
    successful_years = 0

    for i, year in enumerate(available_years, 1):
        try:
            df_processed = process_data_chunk(year_filter=year)
            count = df_processed.count()

            if count > 0:
                df_processed.write \
                    .mode("append") \
                    .parquet("/user/spark/output2/used_cars_data_cleaned")

                total_processed_records += count
                successful_years += 1

            df_processed.unpersist()

        except Exception:
            continue

        time.sleep(3)

    print(f"Successfully processed years: {successful_years}/{len(available_years)}")
    print(f"Total processed records: {total_processed_records}")
    print("Output saved at: /user/spark/output2/used_cars_data_cleaned")

if __name__ == "__main__":
    try:
        run_full_processing()
    except KeyboardInterrupt:
        pass
    except Exception:
        pass
    finally:
        spark.stop()
