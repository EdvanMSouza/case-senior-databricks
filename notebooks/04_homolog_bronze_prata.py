# Databricks notebook source
CATALOG = "workspace"
SCHEMA = "default"

print("=" * 60)
print("RESUMO GERAL - TABELAS BRONZE E PRATA")
print("=" * 60)

# Lista todas as tabelas
tabelas = spark.sql(f"SHOW TABLES IN {CATALOG}.{SCHEMA}").collect()

bronze = []
prata = []
quarantine = []

for t in tabelas:
    nome = t.tableName
    if nome.startswith("bronze_"):
        bronze.append(nome)
    elif nome.startswith("silver_"):
        prata.append(nome)
    elif nome.startswith("quarantine_"):
        quarantine.append(nome)

print(f"\nBRONZE ({len(bronze)} tabelas):")
for tb in sorted(bronze):
    count = spark.table(f"{CATALOG}.{SCHEMA}.{tb}").count()
    print(f"  - {tb}: {count} registros")

print(f"\nPRATA ({len(prata)} tabelas):")
for tb in sorted(prata):
    count = spark.table(f"{CATALOG}.{SCHEMA}.{tb}").count()
    print(f"  - {tb}: {count} registros")

print(f"\nQUARENTENA ({len(quarantine)} tabelas):")
for tb in sorted(quarantine):
    count = spark.table(f"{CATALOG}.{SCHEMA}.{tb}").count()
    print(f"  - {tb}: {count} registros")

print("\n" + "=" * 60)
print("FIM DO RESUMO")
print("=" * 60)