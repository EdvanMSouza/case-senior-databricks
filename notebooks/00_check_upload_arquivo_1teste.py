# Databricks notebook source
# LER O ARQUIVO CSV QUE VOCÊ CARREGOU
df = spark.read.format("csv") \
  .option("header", "true") \
  .option("inferSchema", "true") \
  .load("/Volumes/workspace/default/lake_edvan/clientes_cdc.csv")


# COMMAND ----------

# MOSTRAR OS DADOS (tabela bonita)
display(df)


# COMMAND ----------


# MOSTRAR AS 5 PRIMEIRAS LINHAS
df.show(5)


# COMMAND ----------

# VER A ESTRUTURA (tipos das colunas)
df.printSchema()

# COMMAND ----------

# VER QUANTAS LINHAS TEM
print(f"Total de registros: {df.count()}")