# Databricks notebook source
CATALOG = "workspace"
SCHEMA = "default"
BATCH_ID = "batch_20260112"

print(f"Catalogo: {CATALOG}.{SCHEMA}")
print(f"Batch: {BATCH_ID}")

# COMMAND ----------

df_bronze = spark.table(f"{CATALOG}.{SCHEMA}.bronze_estornos")
print(f"Registros na Bronze: {df_bronze.count()}")
display(df_bronze.limit(5))

# COMMAND ----------

from pyspark.sql.window import Window
from pyspark.sql.functions import row_number, col

window_spec = Window.partitionBy("id_estorno").orderBy(col("data_ingestao").desc())
df_dedup = df_bronze.withColumn("rn", row_number().over(window_spec)).filter("rn = 1").drop("rn")
print(f"Registros únicos: {df_dedup.count()}")
display(df_dedup.limit(5))

# COMMAND ----------

from pyspark.sql.functions import col

df_validos = df_dedup.filter(
    (col("id_transacao").isNotNull()) & 
    (col("data_estorno").isNotNull()) & 
    (col("motivo").isNotNull())
)

df_invalidos = df_dedup.filter(
    (col("id_transacao").isNull()) | 
    (col("data_estorno").isNull()) | 
    (col("motivo").isNull())
)

print(f"Válidos: {df_validos.count()}")
print(f"Quarentena: {df_invalidos.count()}")

if df_invalidos.count() > 0:
    df_invalidos.write \
        .format("delta") \
        .mode("overwrite") \
        .option("overwriteSchema", "true") \
        .saveAsTable(f"{CATALOG}.{SCHEMA}.quarantine_estornos")
    print("Registros inválidos salvos em quarantine_estornos")
else:
    print("Nenhum registro inválido encontrado")

# COMMAND ----------

from pyspark.sql.functions import current_timestamp

df_prata = df_validos \
    .withColumn("data_processamento", current_timestamp())

df_prata.write \
    .format("delta") \
    .mode("overwrite") \
    .option("overwriteSchema", "true") \
    .saveAsTable(f"{CATALOG}.{SCHEMA}.silver_estornos")

print(f"Tabela silver_estornos criada com {df_prata.count()} registros")

# COMMAND ----------

df_verifica = spark.table(f"{CATALOG}.{SCHEMA}.silver_estornos")
print(f"Total na silver_estornos: {df_verifica.count()}")
display(df_verifica.limit(10))