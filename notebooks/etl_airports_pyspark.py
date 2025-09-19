# Notebook principal

# 1) VARIABLES
csv_url = "https://ourairports.com/countries/US/airports.csv"   # fuente pública (US)
dbfs_csv_path = "/dbfs/tmp/airports_us.csv"                     # ruta temporal en driver/DBFS
parquet_dir = "dbfs:/FileStore/airports/us_airports_parquet"    # donde guardaremos Parquet
database_name = "demo_etl"
table_name = "us_airports"

# 2) Intentar lectura directa (rápida) -- algunos workspaces permiten leer desde HTTP
from pyspark.sql import SparkSession
spark = SparkSession.builder.getOrCreate()
try:
    df = spark.read.option("header", True).option("inferSchema", True).csv(csv_url)
    print("Lectura directa desde URL: OK")
except Exception as e:
    print("Lectura directa falló:", e)
    # 3) Fallback: descargar al driver y guardar en /dbfs, luego leer desde DBFS
    import urllib.request
    print("Descargando CSV al driver y guardando en /dbfs/tmp ...")
    urllib.request.urlretrieve(csv_url, dbfs_csv_path)
    df = spark.read.option("header", True).option("inferSchema", True).csv("dbfs:/tmp/airports_us.csv")

# 4) Inspeccionar
print("Schema:")
df.printSchema()
display(df.limit(10))

# 5) Selección / limpieza básica
from pyspark.sql.functions import col, expr
df2 = df.select(
    col("id").cast("long"),
    col("ident"),
    col("type"),
    col("name"),
    col("latitude_deg").cast("double"),
    col("longitude_deg").cast("double"),
    col("elevation_ft").cast("double"),
    col("iso_country"),
    col("iso_region"),
    col("municipality"),
    col("iata_code"),
    col("gps_code")
).na.drop(subset=["latitude_deg", "longitude_deg"])  # eliminar filas sin coordenadas

# Derivada: elevación en metros
df2 = df2.withColumn("elevation_m", expr("round(elevation_ft * 0.3048, 2)"))

display(df2.limit(10))

# 6) Guardar en Parquet (partition opcional por iso_region, por ejemplo)
df2.write.mode("overwrite").partitionBy("iso_region").parquet(parquet_dir)
print("Parquet guardado en:", parquet_dir)

# 7) Registrar en catálogo/metastore para consultas SQL
# Opción A: Crear base si no existe
spark.sql(f"CREATE DATABASE IF NOT EXISTS {database_name}")
# Opción B: Crear tabla externa apuntando al parquet
spark.sql(f"""
CREATE TABLE IF NOT EXISTS {database_name}.{table_name}
USING PARQUET
LOCATION '{parquet_dir}'
""")
print("Tabla registrada como:", f"{database_name}.{table_name}")
