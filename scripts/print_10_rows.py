from pyspark.sql import SparkSession
import time

# Configurar sesión de Spark para lectura eficiente
spark = SparkSession.builder \
    .appName("Comparación Datos Originales vs Procesados") \
    .master("local[1]") \
    .config("spark.sql.parquet.filterPushdown", "true") \
    .config("spark.sql.parquet.mergeSchema", "false") \
    .config("spark.sql.shuffle.partitions", "1") \
    .getOrCreate()

# 1. Leer dataset original (solo primeras 10 filas)
print("\n" + "="*90)
print("🚗 DATASET ORIGINAL - PRIMERAS 10 FILAS (CSV)")
print("="*90)

try:
    start_time = time.time()
    # Leer solo las primeras 10 filas del CSV original
    df_original = spark.read \
        .option("header", "true") \
        .csv("hdfs://localhost:9000/user/spark/input/used_cars_data.csv") \
        .limit(10)
    
    # Mostrar metadatos y datos
    print(f"📋 Columnas: {len(df_original.columns)}")
    print(f"📝 Muestra de columnas: {df_original.columns[:5]}...")
    print(f"⏱  Tiempo lectura: {time.time() - start_time:.2f} segundos")
    
    df_original.show(10, truncate=50, vertical=True)  # Vertical para mejor visualización
    
except Exception as e:
    print(f"❌ Error leyendo dataset original: {str(e)}")

# 2. Leer datos procesados en Parquet
print("\n" + "="*90)
print("✨ DATOS PROCESADOS - PRIMERAS 10 FILAS (PARQUET)")
print("="*90)

try:
    start_time = time.time()
    # Leer todos los archivos Parquet del directorio
    df_procesado = spark.read.parquet("hdfs://localhost:9000/user/spark/output2/used_cars_data_cleaned")
    
    # Mostrar metadatos y estadísticas
    print(f"📋 Columnas procesadas: {len(df_procesado.columns)}")
    print(f"🗂  Archivos Parquet: {len(df_procesado.inputFiles())}")
    print(f"📊 Registros totales: {df_procesado.count()}")
    print(f"⏱  Tiempo lectura: {time.time() - start_time:.2f} segundos")
    
    # Mostrar diferencias clave en esquema
    print("\n🔍 Cambios en columnas (ejemplos):")
    print(" - back_legroom: String → Double")
    print(" - city: String → String normalizado")
    print(" - engine_cylinders: String → Integer")
    print(" - listed_date: String → Date")
    
    # Mostrar datos con formato vertical
    print("\n💾 Muestra de datos procesados:")
    df_procesado.limit(100).show(truncate=50, vertical=True)
    
except Exception as e:
    print(f"❌ Error leyendo datos procesados: {str(e)}")

# 3. Comparación de almacenamiento
print("\n" + "="*90)
print("📊 COMPARACIÓN DE ALMACENAMIENTO")
print("="*90)

try:
    # Obtener tamaño del dataset original (aproximado)
    original_size = spark._jsc.hadoopConfiguration().get(
        "fs.defaultFS"
    ) + "/user/spark/input/used_cars_data.csv"
    
    # Obtener tamaño del directorio Parquet
    parquet_path = spark._jsc.hadoopConfiguration().get(
        "fs.defaultFS"
    ) + "/user/spark/output2/used_cars_data_cleaned"
    
    # Usar la API de Hadoop para obtener tamaños
    fs = spark._jvm.org.apache.hadoop.fs.FileSystem.get(
        spark._jsc.hadoopConfiguration()
    )
    
    # Tamaño CSV
    csv_size = fs.getContentSummary(spark._jvm.org.apache.hadoop.fs.Path(original_size)).getLength()
    
    # Tamaño Parquet (suma de todos los archivos)
    parquet_size = fs.getContentSummary(spark._jvm.org.apache.hadoop.fs.Path(parquet_path)).getLength()
    
    print(f"📦 Tamaño dataset original: {csv_size/1e9:.2f} GB")
    print(f"📦 Tamaño datos procesados: {parquet_size/1e6:.2f} MB")
    print(f"🚀 Reducción de tamaño: {(1 - parquet_size/csv_size)*100:.1f}%")
    
except Exception as e:
    print(f"⚠️  No se pudo obtener tamaño: {str(e)}")

# Finalizar sesión
spark.stop()
print("\n✅ Comparación completada!")