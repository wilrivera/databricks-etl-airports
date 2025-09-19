# Databricks notebook source
# =============================================================
# ETL de dataset público de aeropuertos (OurAirports) - Opción 1
# Lectura usando SparkFiles
# =============================================================

from pyspark.sql.types import StructType, StructField, StringType, DoubleType, IntegerType
from pyspark import SparkFiles

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

url = "https://raw.githubusercontent.com/jpatokal/openflights/master/data/airports.dat"

# 2. Descargar archivo temporal
spark.sparkContext.addFile(url)
local_path = SparkFiles.get("airports.dat")

# 3. Leer archivo con Spark
df_raw = spark.read.csv(local_path, schema=schema)

print(f"Cantidad de registros: {df_raw.count()}")
df_raw.show(5, truncate=False)

# 4. Transformación
df_clean = df_raw.filter(df_raw.country.isNotNull()).withColumn("country", df_raw.country.upper())

# 5. Guardar en Parquet
output_path = "/mnt/airports/airports_parquet_opt1"
df_clean.write.mode("overwrite").parquet(output_path)

# 6. Registrar tabla en catálogo
spark.sql("CREATE DATABASE IF NOT EXISTS airports_db")
spark.sql("DROP TABLE IF EXISTS airports_db.airports_opt1")
spark.sql(f"""
    CREATE TABLE airports_db.airports_opt1
    USING PARQUET
    LOCATION '{output_path}'
""")

# 7. Consulta de prueba
result = spark.sql("""
    SELECT country, COUNT(*) AS num_airports
    FROM airports_db.airports_opt1
    GROUP BY country
    ORDER BY num_airports DESC
    LIMIT 10
""")
result.show()
