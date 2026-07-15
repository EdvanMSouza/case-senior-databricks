## Como Executar

1. **Configurar ambiente:**

   * Criar Volume no Unity Catalog: `workspace.default.lake\_edvan`
   * Fazer upload dos CSVs para o Volume
2. **Executar notebooks em ordem:**

   * Bronze: `bronze\_\*.py` (6 notebooks)
   * Prata: `prata\_\*.py` (6 notebooks)
   * Ouro: `ouro\_views\_sql.py`
   * Homologação: `homologacao\_gold.py`

## Premissas e Decisões Técnicas

### SCD Tipo 2

Optei por SCD Tipo 2 para clientes, contas e cartões para preservar histórico de mudanças, permitindo análises temporais e auditoria.

### Auto Loader

Embora não implementado na versão atual, em produção utilizaria Auto Loader para ingestão incremental com schema evolution e suporte a dados atrasados.

### Particionamento

Em produção, aplicaria particionamento por data em transações (data\_transacao) e ZORDER por id\_cliente nas dimensões para otimizar consultas.

### Qualidade de Dados

Regras implementadas:

* Campos obrigatórios não nulos
* Valores positivos (renda, limite, valor)
* Separação de inválidos em quarentena

### Idempotência

Todos os pipelines são idempotentes via MERGE ou OVERWRITE, garantindo reprocessamento seguro.

### Tratamento de Estornos

Transações estornadas são identificadas e seu valor é negativado no fato, não compondo métricas líquidas.

### Dados Atrasados

A ingestão preserva data\_ingestao e data\_atualizacao para tratamento de dados fora de ordem.

## Métricas e Volumetria

|Camada|Tabelas/Views|Registros|
|-|-|-|
|Bronze|6 tabelas|5.800|
|Prata|6 tabelas + 1 quarentena|5.699|
|Ouro|7 views|6.496|

## Evidências de Execução

Prints disponíveis em `./evidence/evidencias\_execucao.docx`

## Próximos Passos (Produção)

* \[ ] Implementar Auto Loader para ingestão incremental
* \[ ] Adicionar particionamento e ZORDER
* \[ ] Configurar Databricks Workflows para agendamento
* \[ ] Adicionar testes unitários completos
* \[ ] Implementar monitoramento com Databricks Lakehouse Monitoring
* \[ ] Configurar Unity Catalog com permissões granulares

## Autor

Desafio técnico - Edvan - Engenheiro de Dados Sênior

