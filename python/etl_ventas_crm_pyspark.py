from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, trim, lower, regexp_replace, when,
    avg, countDistinct, sum as spark_sum
)

# Crear sesión Spark
spark = SparkSession.builder \
    .appName("ETL_Ventas_CRM") \
    .config("spark.jars", "/ruta/sqljdbc42.jar") \
    .getOrCreate()

# Cargar datasets
ventas = spark.read.csv("ventas_raw.csv", header=True, inferSchema=True)
crm = spark.read.csv("crm_clientes.csv", header=True, inferSchema=True)

# Normalización columnas
ventas = ventas.select([trim(col(c)).alias(c.lower()) for c in ventas.columns])
crm = crm.select([trim(col(c)).alias(c.lower()) for c in crm.columns])

# Conversión de tipos
ventas = ventas.withColumn("monto", col("monto").cast("double"))
ventas = ventas.withColumn("fecha", col("fecha").cast("date"))

# Filtrar registros inválidos
ventas = ventas.dropna(subset=["venta_id", "cliente_id", "fecha", "monto"])

# Eliminar montos negativos
ventas = ventas.filter(col("monto") > 0)

# Estandarizar sucursales
ventas = ventas.withColumn(
    "sucursal",
    lower(regexp_replace(col("sucursal"), "[^A-Za-z]", ""))
)

mapa_suc = {
    "santiago": "Santiago",
    "stgo": "Santiago",
    "antofagasta": "Antofagasta",
    "antofa": "Antofagasta"
}

for k, v in mapa_suc.items():
    ventas = ventas.withColumn(
        "sucursal",
        when(col("sucursal") == k, v).otherwise(col("sucursal"))
    )

# Eliminar duplicados reales
ventas = ventas.dropDuplicates(["venta_id"])

# Mezcla con CRM
ventas_crm = ventas.join(crm, on="cliente_id", how="left")

# Segmentación simple RFM
rfm = ventas_crm.groupBy("cliente_id").agg(
    spark_sum("monto").alias("monto_total"),
    countDistinct("fecha").alias("frecuencia")
)

ventas_crm = ventas_crm.join(rfm, on="cliente_id", how="left")

# Escribir en SQL Server
url = "jdbc:sqlserver://SERVIDOR:1433;databaseName=DB"
usuario = "user"
password = "password"
tabla = "dbo.ventas_crm_limpias"

ventas_crm.write \
    .format("jdbc") \
    .option("url", url) \
    .option("dbtable", tabla) \
    .option("user", usuario) \
    .option("password", password) \
    .option("driver", "com.microsoft.sqlserver.jdbc.SQLServerDriver") \
    .mode("overwrite") \
    .save()

spark.stop()
