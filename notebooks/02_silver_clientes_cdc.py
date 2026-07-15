# Databricks notebook source
# Configuração do ambiente
CATALOG = "workspace"
SCHEMA = "default"
BATCH_ID = "batch_20260112"

print(f"Processando lote: {BATCH_ID}")
print(f"Catalogo: {CATALOG}.{SCHEMA}")

# COMMAND ----------

# Lê dados da camada Bronze
df_bronze = spark.table(f"{CATALOG}.{SCHEMA}.bronze_clientes")
print(f"Registros na Bronze: {df_bronze.count()}")
display(df_bronze.limit(5))

# COMMAND ----------

from pyspark.sql.window import Window
from pyspark.sql.functions import row_number, col

# Remove duplicados mantendo o mais recente
window_spec = Window.partitionBy("id_cliente").orderBy(col("data_ingestao").desc())
df_dedup = df_bronze.withColumn("rn", row_number().over(window_spec)).filter("rn = 1").drop("rn")
print(f"Registros únicos: {df_dedup.count()}")
display(df_dedup.limit(5))

# COMMAND ----------

from pyspark.sql.functions import col

# Separa válidos e inválidos - com parênteses explícitos
df_validos = df_dedup.filter(
    (col("cpf").isNotNull()) & 
    (col("nome").isNotNull()) & 
    (col("renda") > 0)
)

df_invalidos = df_dedup.filter(
    (col("cpf").isNull()) | 
    (col("nome").isNull()) | 
    (col("renda") <= 0)
)

print(f"Válidos: {df_validos.count()}")
print(f"Quarentena: {df_invalidos.count()}")

# Salva quarentena (se houver)
if df_invalidos.count() > 0:
    df_invalidos.write \
        .format("delta") \
        .mode("overwrite") \
        .option("overwriteSchema", "true") \
        .saveAsTable(f"{CATALOG}.{SCHEMA}.quarantine_clientes")
    print("Registros inválidos salvos em quarantine_clientes")
else:
    print("Nenhum registro inválido encontrado")

# COMMAND ----------

# Recria a tabela Silver com a estrutura correta
print("Criando tabela silver_clientes")

# Prepara os dados com as colunas extras
df_silver = df_validos \
    .withColumn("data_processamento", current_timestamp()) \
    .withColumn("batch_id", lit(BATCH_ID))

# Sobrescreve a tabela
df_silver.write \
    .format("delta") \
    .mode("overwrite") \
    .option("overwriteSchema", "true") \
    .saveAsTable(f"{CATALOG}.{SCHEMA}.silver_clientes")

print("Tabela silver_clientes recriada com sucesso!")
print(f"Registros: {spark.table(f'{CATALOG}.{SCHEMA}.silver_clientes').count()}")

# COMMAND ----------

# Confere o resultado
df_resultado = spark.table(f"{CATALOG}.{SCHEMA}.silver_clientes")
print(f"Total na Silver: {df_resultado.count()}")
display(df_resultado.limit(10))

# COMMAND ----------

# Contagem exata na Silver
count_silver = spark.sql("SELECT COUNT(*) FROM workspace.default.silver_clientes").collect()[0][0]
print(f"Total de registros na Silver: {count_silver}")

# Mostra as colunas
print("\nEstrutura da tabela:")
spark.sql("DESCRIBE workspace.default.silver_clientes").show()

# COMMAND ----------

# MAGIC %md
# MAGIC