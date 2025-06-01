from pyspark.sql import SparkSession
import math
import time

# Although this is not Spark Core, reading Parquet is faster this way. Spark Core is used for processing.
spark = SparkSession.builder \
    .appName("GroupCarsSparkCore") \
    .config("spark.driver.memory", "4g") \
    .config("spark.executor.memory", "4g") \
    .config("spark.sql.parquet.filterPushdown", "true") \
    .config("spark.sql.parquet.mergeSchema", "false") \
    .config("spark.sql.shuffle.partitions", "4") \
    .getOrCreate()

selected_columns = ["power", "engine_displacement", "model_name", "price"]
df = spark.read.parquet("hdfs://localhost:9000/user/spark/output2/used_cars_data_cleaned")
#df.printSchema()
df = df.select(*selected_columns)



start = time.time()
rdd = df.rdd
row_sample = rdd.first()

column_names = list(row_sample.__fields__)
print(column_names)

filtered_rdd = rdd.filter(lambda row:
    row.power is not None and row.power > 0 and
    row.engine_displacement is not None and row.engine_displacement > 0 and
    row.model_name is not None and
    row.price is not None
)

def assign_bins(row):
    hp = float(row.power)
    displacement = float(row.engine_displacement)
    bin_hp = int(math.floor(math.log(hp) / math.log(1.1)))
    bin_disp = int(math.floor(math.log(displacement) / math.log(1.1)))
    return (
        (bin_hp, bin_disp),
        {
            'model_name': row.model_name,
            'price': float(row.price),
            'horsepower': hp
        }
    )

binned_rdd = filtered_rdd.map(assign_bins)

def create_combiner(value):
    return (
        value['price'],         # total_price
        1,                      # count
        value['horsepower'],    # max_horsepower
        value['model_name']     # top_model
    )

def merge_value(accumulator, value):
    total_price, count, max_hp, top_model = accumulator
    new_price = value['price']
    new_hp = value['horsepower']
    new_model = value['model_name']

    total_price += new_price
    count += 1

    if new_hp > max_hp or (new_hp == max_hp and new_model < top_model):
        max_hp = new_hp
        top_model = new_model

    return (total_price, count, max_hp, top_model)

def merge_combiners(a, b):
    total_price_a, count_a, max_hp_a, model_a = a
    total_price_b, count_b, max_hp_b, model_b = b

    total_price = total_price_a + total_price_b
    count = count_a + count_b

    if max_hp_a > max_hp_b or (max_hp_a == max_hp_b and model_a < model_b):
        max_hp = max_hp_a
        top_model = model_a
    else:
        max_hp = max_hp_b
        top_model = model_b

    return (total_price, count, max_hp, top_model)

grouped_rdd = binned_rdd.combineByKey(
    create_combiner,
    merge_value,
    merge_combiners
)

def compute_result(record):
    (bin_hp, bin_disp), (total_price, count, max_hp, top_model) = record
    avg_price = total_price / count
    return {
        'bin_hp': bin_hp,
        'bin_disp': bin_disp,
        'avg_price': avg_price,
        'max_horsepower': max_hp,
        'top_model': top_model
    }

result_rdd = grouped_rdd.map(compute_result)

finish = time.time()



result_sample = result_rdd.take(10)

print(f"Total time: {finish - start:.2f} seconds")
for record in result_sample:
    print(record)

spark.stop()
