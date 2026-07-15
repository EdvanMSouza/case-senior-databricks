# Databricks notebook source
CATALOG = "workspace"
SCHEMA = "default"
VOLUME = "lake_edvan"
BATCH_ID = "batch_20260112"

print(f"Catalogo: {CATALOG}.{SCHEMA}")
print(f"Volume: {VOLUME}")

# COMMAND ----------

caminho = f"/Volumes/{CATALOG}/{SCHEMA}/{VOLUME}/eventos_risco.csv"
print(f"Lendo arquivo: {caminho}")

df = spark.read.option("header", "true").option("inferSchema", "true").csv(caminho)
print(f"Registros lidos: {df.count()}")
display(df.limit(5))

# COMMAND ----------

from pyspark.sql.functions import current_timestamp, lit, md5, concat, col

df_metadata = df \
    .withColumn("arquivo_origem", lit(caminho)) \
    .withColumn("data_ingestao", current_timestamp()) \
    .withColumn("batch_id", lit(BATCH_ID)) \
    .withColumn("hash_linha", md5(concat(*[col(c).cast("string") for c in df.columns])))

print(f"Metadados adicionados: {df_metadata.count()} registros")
display(df_metadata.limit(5))

# COMMAND ----------

df_metadata.write \
    .format("delta") \
    .mode("overwrite") \
    .option("overwriteSchema", "true") \
    .saveAsTable(f"{CATALOG}.{SCHEMA}.bronze_eventos_risco")

print("Tabela bronze_eventos_risco criada com sucesso!")

# COMMAND ----------

df_verifica = spark.table(f"{CATALOG}.{SCHEMA}.bronze_eventos_risco")
print(f"Total na bronze_eventos_risco: {df_verifica.count()}")
display(df_verifica.limit(5))