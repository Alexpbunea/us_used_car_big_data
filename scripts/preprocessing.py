from pyspark.sql import SparkSession
from pyspark.sql.types import *
from pyspark.sql.functions import lower, regexp_replace, col, regexp_extract, to_date
import time
#import psutil

# 1. Iniciar SparkSession - Configuración optimizada para 16GB RAM
spark = SparkSession.builder \
    .appName("Preprocesado Usados - Memoria Limitada") \
    .master("local[2]") \
    .config("spark.driver.memory", "4g") \
    .config("spark.executor.memory", "4g") \
    .config("spark.driver.maxResultSize", "1g") \
    .config("spark.sql.shuffle.partitions", "4000") \
    .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer") \
    .config("spark.sql.adaptive.enabled", "true") \
    .config("spark.sql.adaptive.coalescePartitions.enabled", "true") \
    .getOrCreate()

# 2. Definir esquema - Ajustado para evitar conflictos
schema = StructType([
    StructField("vin", StringType(), False),
    StructField("back_legroom", StringType(), True),
    StructField("bed", StringType(), True),
    StructField("bed_height", StringType(), True),
    StructField("bed_length", StringType(), True),
    StructField("body_type", StringType(), True),
    StructField("cabin", StringType(), True),
    StructField("city", StringType(), False),
    StructField("city_fuel_economy", FloatType(), True),
    StructField("combine_fuel_economy", FloatType(), True),
    StructField("daysonmarket", IntegerType(), False),
    StructField("dealer_zip", IntegerType(), True),
    StructField("engine_cylinders", StringType(), True),
    StructField("engine_displacement", FloatType(), True),
    StructField("engine_type", StringType(), True),
    StructField("exterior_color", StringType(), True),
    StructField("fleet", BooleanType(), True),
    StructField("frame_damaged", BooleanType(), True),
    StructField("franchise_dealer", BooleanType(), True),
    StructField("franchise_make", StringType(), True),
    StructField("front_legroom", StringType(), True),
    StructField("fuel_tank_volume", StringType(), True),
    StructField("fuel_type", StringType(), True),
    StructField("has_accidents", BooleanType(), True),
    StructField("height", StringType(), True),
    StructField("highway_fuel_economy", FloatType(), True),
    StructField("horsepower", FloatType(), True),  # Mantenemos el original
    StructField("interior_color", StringType(), True),
    StructField("isCab", BooleanType(), True),
    StructField("is_certified", BooleanType(), True),
    StructField("is_cpo", BooleanType(), True),
    StructField("is_new", BooleanType(), True),
    StructField("is_oemcpo", BooleanType(), True),
    StructField("latitude", FloatType(), True),
    StructField("length", StringType(), True),
    StructField("listed_date", StringType(), True),
    StructField("listing_color", StringType(), True),
    StructField("listing_id", IntegerType(), False),
    StructField("longitude", FloatType(), True),
    StructField("main_picture_url", StringType(), True),
    StructField("major_options", StringType(), True),
    StructField("make_name", StringType(), False),
    StructField("maximum_seating", IntegerType(), True),
    StructField("mileage", IntegerType(), True),
    StructField("model_name", StringType(), False),
    StructField("owner_count", IntegerType(), True),
    StructField("power", StringType(), True),  # Campo original para extraer horsepower
    StructField("price", DoubleType(), False),
    StructField("salvage", BooleanType(), True),
    StructField("savings_amount", DoubleType(), True),
    StructField("seller_rating", FloatType(), True),
    StructField("sp_id", StringType(), True),
    StructField("sp_name", StringType(), True),
    StructField("theft_title", BooleanType(), True),
    StructField("torque", StringType(), True),
    StructField("transmission", StringType(), True),
    StructField("transmission_display", StringType(), True),
    StructField("trimId", IntegerType(), True),
    StructField("trim_name", StringType(), True),
    StructField("vehicle_damage_category", StringType(), True),
    StructField("wheel_system", StringType(), True),
    StructField("wheel_system_display", StringType(), True),
    StructField("wheelbase", StringType(), True),
    StructField("width", StringType(), True),
    StructField("year", IntegerType(), False),
    StructField("description", StringType(), True),
])

# def verificar_memoria():
#     """
#     Monitorea el uso de memoria del sistema para prevenir sobrecarga.
#     Es como tener un medidor de combustible que te avisa cuando necesitas parar.
#     """
#     memoria = psutil.virtual_memory()
#     print(f"💻 Memoria RAM: {memoria.percent}% utilizada ({memoria.used // (1024**3):.1f}GB / {memoria.total // (1024**3):.1f}GB)")
    
#     if memoria.percent > 85:
#         print("⚠️  ADVERTENCIA: Memoria alta. Pausando para permitir limpieza...")
#         time.sleep(10)
#         return False
#     return True

def procesar_chunk_datos(year_filter=None, make_filter=None):
    """
    Procesa un subconjunto específico de los datos aplicando todas las transformaciones
    de limpieza en una sola pasada optimizada para memoria limitada.
    
    Esta función es el corazón del procesamiento: toma datos sucios y los devuelve limpios,
    pero trabajando solo con la porción que puede manejar cómodamente en memoria.
    """
    
    print(f"🔍 Cargando datos para filtros: año={year_filter}, marca={make_filter}")
    
    # Leer datos con el esquema predefinido
    df_builder = spark.read \
        .option("header", "true") \
        .option("delimiter", ",") \
        .option("quote", '"') \
        .option("escape", '\\') \
        .option("multiLine", "true") \
        .option("inferSchema", "true") \
        .csv("hdfs://localhost:9000/user/spark/input/used_cars_data.csv") 
    
    # Aplicar filtros inmediatamente para reducir el volumen de datos
    if year_filter:
        df_builder = df_builder.filter(col("year") == year_filter)
    if make_filter:
        df_builder = df_builder.filter(col("make_name").like(f"%{make_filter}%"))
    
    # Reparticionar para optimizar el procesamiento en memoria limitada
    df = df_builder.repartition(4)
    
    # Contar registros para monitoreo
    count_inicial = df.count()
    print(f"📊 Procesando chunk con {count_inicial} registros")
    
    if count_inicial == 0:
        print("⚠️  No hay datos para procesar con estos filtros")
        return spark.createDataFrame([], schema)
    
    # Transformación principal: combinar todas las operaciones en una sola pasada
    # Esto es crítico para eficiencia de memoria - una sola lectura, múltiples transformaciones
    
    df_transformado = df.select(
        # === COLUMNAS ORIGINALES ===
        # Incluir TODAS las columnas que necesitas mantener
        "vin", 
        "daysonmarket", 
        "price", 
        "year",
        "city",
        "make_name","model_name",
        "description",
        "engine_displacement", "engine_type", "engine_cylinders", "power", "torque",
        
        
        # === TRANSFORMACIONES DE TEXTO (limpieza y normalización) ===
        lower(regexp_replace(col("city"), "[^a-zA-Z0-9\\s]", "")).alias("city_clean"),
        lower(regexp_replace(col("make_name"), "[^a-zA-Z0-9\\s]", "")).alias("make_name_clean"),
        lower(regexp_replace(col("model_name"), "[^a-zA-Z0-9\\s]", "")).alias("model_name_clean"),
        #lower(regexp_replace(col("exterior_color"), "[^a-zA-Z0-9\\s]", "")).alias("exterior_color_clean"),
        #lower(regexp_replace(col("fuel_type"), "[^a-zA-Z0-9\\s]", "")).alias("fuel_type_clean"),
        
        # === EXTRACCIÓN DE VALORES NUMÉRICOS DE CAMPOS DE MEDICIÓN ===
        #regexp_extract(col("back_legroom"), "(\\d+\\.?\\d*)", 1).cast(DoubleType()).alias("back_legroom_num"),
        #regexp_extract(col("bed_height"), "(\\d+\\.?\\d*)", 1).cast(DoubleType()).alias("bed_height_num"),
        #regexp_extract(col("bed_length"), "(\\d+\\.?\\d*)", 1).cast(DoubleType()).alias("bed_length_num"),
        #regexp_extract(col("front_legroom"), "(\\d+\\.?\\d*)", 1).cast(DoubleType()).alias("front_legroom_num"),
        #regexp_extract(col("fuel_tank_volume"), "(\\d+\\.?\\d*)", 1).cast(DoubleType()).alias("fuel_tank_volume_num"),
        #regexp_extract(col("height"), "(\\d+\\.?\\d*)", 1).cast(DoubleType()).alias("height_num"),
        #regexp_extract(col("length"), "(\\d+\\.?\\d*)", 1).cast(DoubleType()).alias("length_num"),
        #regexp_extract(col("wheelbase"), "(\\d+\\.?\\d*)", 1).cast(DoubleType()).alias("wheelbase_num"),
        #regexp_extract(col("width"), "(\\d+\\.?\\d*)", 1).cast(DoubleType()).alias("width_num"),
        
        # === PROCESAMIENTO ESPECIALIZADO ===
        regexp_extract(col("engine_cylinders"), "(\\d+)", 1).cast(IntegerType()).alias("engine_cylinders_num"),
        regexp_extract(col("engine_displacement"), "(\\d+)", 1).cast(FloatType()).alias("engine_displacement_num"),
        #to_date(col("listed_date"), "MM/dd/yyyy").alias("listed_date_parsed"),
        
        # === EXTRACCIÓN DE POTENCIA Y TORQUE ===
        # Usamos nombres diferentes para evitar conflictos con campos existentes
        
        regexp_extract(col("power"), "(\\d+)(\\s*[Hh][Pp]|$)", 1).cast(FloatType()).alias("power_clean"),
        regexp_extract(col("torque"), "(\\d+)(\\s*[Ll]b[-]?[Ff]t|$)", 1).cast(FloatType()).alias("torque_clean"),
        
        # === LIMPIEZA DE DESCRIPCIÓN ===
        lower(regexp_replace(col("description"), "[^\\w\\s]", "")).alias("description_clean"),
        
        # === CAMPOS BOOLEANOS (casting para asegurar tipo correcto) ===
        #col("fleet").cast(BooleanType()).alias("fleet"),
        #col("frame_damaged").cast(BooleanType()).alias("frame_damaged"),
        #col("franchise_dealer").cast(BooleanType()).alias("franchise_dealer"),
        #col("has_accidents").cast(BooleanType()).alias("has_accidents"),
        #col("isCab").cast(BooleanType()).alias("isCab"),
        #col("is_certified").cast(BooleanType()).alias("is_certified"),
        #col("is_cpo").cast(BooleanType()).alias("is_cpo"),
        #col("is_new").cast(BooleanType()).alias("is_new"),
        #col("is_oemcpo").cast(BooleanType()).alias("is_oemcpo"),
        #col("salvage").cast(BooleanType()).alias("salvage"),
        #col("theft_title").cast(BooleanType()).alias("theft_title")
    )
    
    # Ahora renombramos las columnas limpias para reemplazar las originales
    # Esto es como cambiar las etiquetas en los cajones después de organizar el contenido
    df_renombrado = df_transformado \
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
    
    # Eliminar duplicados basados en identificadores únicos
    df_final = df_renombrado.dropDuplicates(["vin"])
    
    count_final = df_final.count()
    print(f"✅ Transformación completada: {count_inicial} → {count_final} registros (duplicados eliminados: {count_inicial - count_final})")
    
    return df_final

def ejecutar_procesamiento_completo():
    """
    Orquesta todo el procesamiento dividiendo los datos en chunks manejables por año.
    
    Esta función actúa como el director de orquesta, coordinando que cada sección
    (año) sea procesada en el momento correcto sin sobrecargar la memoria.
    """
    
    print("🚀 Iniciando procesamiento por chunks para dataset de 10GB...")
    #verificar_memoria()
    
    # Exploración inicial para identificar años disponibles
    print("🔍 Explorando estructura del dataset...")
    df_muestra = spark.read \
        .option("header", "true") \
        .option("delimiter", ",") \
        .option("quote", '"') \
        .option("escape", '"') \
        .option("multiLine", "true") \
        .option("inferSchema", "true") \
        .csv("hdfs://localhost:9000/user/spark/input/used_cars_data.csv")
    
    # Obtener años únicos disponibles
    years_disponibles = [row["year"] for row in df_muestra.select("year").distinct().collect() if row["year"] is not None]
    #print(years_disponibles)
    years_disponibles.sort()
    #print(years_disponibles)
    
    print(f"📅 Años encontrados en el dataset: {years_disponibles}")
    print(f"📈 Se procesarán {len(years_disponibles)} chunks por separado")
    
    # Procesar cada año individualmente
    total_registros_procesados = 0
    años_exitosos = 0
    
    for i, year in enumerate(years_disponibles, 1):
        print(f"\n{'='*50}")
        print(f"🔄 Procesando chunk {i}/{len(years_disponibles)}: Año {year}")
        print(f"{'='*50}")
        
        # Verificar memoria antes de cada procesamiento
        # if not verificar_memoria():
        #     print("⚡ Forzando limpieza de memoria...")
        #     spark.catalog.clearCache()
        #     time.sleep(5)
        
        try:
            # Procesar este año específico
            start_time = time.time()
            df_year_procesado = procesar_chunk_datos(year_filter=year)
            
            # Verificar resultados
            count_registros = df_year_procesado.count()
            
            if count_registros > 0:
                # Guardar chunk procesado
                df_year_procesado.write \
                    .mode("append") \
                    .parquet("/user/spark/output2/used_cars_data_cleaned")
                
                total_registros_procesados += count_registros
                años_exitosos += 1
                
                elapsed_time = time.time() - start_time
                print(f"💾 Año {year} guardado exitosamente")
                print(f"⏱️  Tiempo de procesamiento: {elapsed_time:.2f} segundos")
                print(f"📊 Registros acumulados: {total_registros_procesados}")
            else:
                print(f"⚠️  Año {year}: Sin datos para procesar")
            
            # Limpiar memoria explícitamente después de cada chunk
            df_year_procesado.unpersist()
            
        except Exception as e:
            print(f"❌ Error procesando año {year}: {str(e)}")
            print("🔄 Continuando con el siguiente año...")
            continue
        
        # Pausa entre chunks para permitir garbage collection
        print("🧹 Limpiando memoria...")
        time.sleep(3)
    
    # Resumen final
    print(f"\n{'='*60}")
    print(f"🎉 ¡PROCESAMIENTO COMPLETADO!")
    print(f"{'='*60}")
    print(f"✅ Años procesados exitosamente: {años_exitosos}/{len(years_disponibles)}")
    print(f"📊 Total de registros procesados: {total_registros_procesados}")
    print(f"💾 Datos guardados en: /user/spark/output2/used_cars_data_cleaned")
    print(f"{'='*60}")

# ===============================================
# EJECUCIÓN PRINCIPAL
# ===============================================
if __name__ == "__main__":
    try:
        ejecutar_procesamiento_completo()
    except KeyboardInterrupt:
        print("\n⏹️  Procesamiento interrumpido por el usuario")
    except Exception as e:
        print(f"\n💥 Error fatal en el procesamiento: {str(e)}")
    finally:
        print("🔚 Cerrando sesión de Spark...")
        spark.stop()
        print("✅ Sesión cerrada correctamente")