# ===================================================
# ETL Dataset público de Aeropuertos - Versión Delta
# Compatible con Databricks Community Edition
# ===================================================

import pandas as pd
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, IntegerType
from pyspark.sql.functions import upper, col

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

# 2. Descargar dataset con Pandas (conversión de "\N" a NaN)
url = "https://raw.githubusercontent.com/jpatokal/openflights/master/data/airports.dat"
pdf = pd.read_csv(
    url,
    header=None,
    names=[
        "id","name","city","country","iata","icao","latitude","longitude",
        "altitude","timezone","dst","tz_database_time_zone","type","source"
    ],
    na_values="\\N"
)

# 3. Convertir a Spark DataFrame
df_raw = spark.createDataFrame(pdf, schema=schema)
print(f"Cantidad de registros: {df_raw.count()}")
df_raw.show(5, truncate=False)

# 4. Transformación (ejemplo)
df_clean = (
    df_raw
    .filter(df_raw.country.isNotNull())
    .withColumn("country", upper(col("country")))
)

# 5. Guardar directamente como tabla Delta en catálogo
spark.sql("CREATE DATABASE IF NOT EXISTS airports_db")
df_clean.write.format("delta").mode("overwrite").saveAsTable("airports_db.airports_delta")

# 6. Consulta de prueba
result = spark.sql("""
    SELECT country, COUNT(*) AS num_airports
    FROM airports_db.airports_delta
    GROUP BY country
    ORDER BY num_airports DESC
    LIMIT 10
""")
result.show()