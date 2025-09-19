# Notebook principal

# Databricks notebook source
# ETL de dataset público de aeropuertos (OurAirports)
# Fuente: https://raw.githubusercontent.com/jpatokal/openflights/master/data/airports.dat

from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, IntegerType

# 1. Definir esquema
schema = StructType([
    StructField("id", IntegerType(), True),
    StructField("name", StringType(), True),
    StructField("city", StringType(), True),
    StructField("country", StringType(), True),
    StructField("iata", StringType(), True),
    StructField("icao", StringType(), True),
    StructField("latitude", DoubleType(), True),
    StructField("longitude", DoubleType(), True),
    StructField("altitude", IntegerType(), True),
    StructField("timezone", DoubleType(), True),
    StructField("dst", StringType(), True),
    StructField("tz_database_time_zone", StringType(), True),
    StructField("type", StringType(), True),
    StructField("source", StringType(), True),
])

# 2. Leer dataset público (CSV desde GitHub)
url = "https://raw.githubusercontent.com/jpatokal/openflights/master/data/airports.dat"
df_raw = spark.read.csv(url, schema=schema)

print(f"Cantidad de registros: {df_raw.count()}")
df_raw.show(5, truncate=False)

# 3. Transformaciones (ejemplo: limpiar nulos y normalizar mayúsculas en país)
df_clean = (
    df_raw
    .filter(df_raw.country.isNotNull())
    .withColumn("country", df_raw.country.upper())
)

# 4. Guardar en formato Parquet en el almacenamiento de Databricks
output_path = "/mnt/airports/airports_parquet"
df_clean.write.mode("overwrite").parquet(output_path)

print("✅ Data guardada en formato Parquet en:", output_path)

# 5. Registrar tabla en el catálogo para consultas SQL
spark.sql("CREATE DATABASE IF NOT EXISTS airports_db")
spark.sql("DROP TABLE IF EXISTS airports_db.airports")
spark.sql(f"""
    CREATE TABLE airports_db.airports
    USING PARQUET
    LOCATION '{output_path}'
""")

print("✅ Tabla registrada en catalogo: airports_db.airports")

