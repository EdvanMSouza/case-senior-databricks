# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "5"
# ///
# Lendo o CSV do Volume que você criou
df_clientes = spark.read \
    .option("header", "true") \
    .option("inferSchema", "true") \
    .csv("/Volumes/workspace/default/lake_edvan/clientes_cdc.csv")

# Mostra quantos registros tem
print(f"Total de registros: {df_clientes.count()}")

# Mostra uma prévia
display(df_clientes.limit(10))

# COMMAND ----------

from pyspark.sql.functions import input_file_name, current_timestamp, lit, md5, concat, col

# Adiciona metadados
df_bronze = df_clientes \
    .withColumn("arquivo_origem", lit("/Volumes/workspace/default/lake_edvan/clientes_cdc.csv")) \
    .withColumn("data_ingestao", current_timestamp()) \
    .withColumn("batch_id", lit("batch_20260112")) \
    .withColumn("hash_linha", md5(concat(*[col(c).cast("string") for c in df_clientes.columns])))

# Cria a tabela Bronze em Delta
df_bronze.write \
    .format("delta") \
    .mode("overwrite") \
    .option("overwriteSchema", "true") \
    .saveAsTable("workspace.default.bronze_clientes")

print("Tabela bronze_clientes criada com sucesso!")
print(f"Registros na Bronze: {spark.sql('SELECT COUNT(*) FROM workspace.default.bronze_clientes').collect()[0][0]}")