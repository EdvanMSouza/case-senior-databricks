-- Databricks notebook source
-- MAGIC %python
-- MAGIC CATALOG = "workspace"
-- MAGIC SCHEMA = "default"
-- MAGIC
-- MAGIC print(f"Catalogo: {CATALOG}.{SCHEMA}")
-- MAGIC print("Criando views da Camada Ouro...")

-- COMMAND ----------

CREATE OR REPLACE VIEW workspace.default.gold_fato_transacao AS
WITH transacoes_validas AS (
  SELECT 
    t.id_transacao,
    t.id_cartao,
    t.data_transacao,
    t.valor,
    t.mcc,
    t.estabelecimento,
    t.canal,
    t.pais,
    t.moeda,
    c.id_conta,
    c.id_cliente,
    -- Estorno: se existir, valor negativo
    CASE WHEN e.id_estorno IS NOT NULL THEN t.valor * -1 ELSE t.valor END AS valor_liquido,
    e.id_estorno,
    e.data_estorno,
    e.motivo AS motivo_estorno,
    -- Status do cartão na data da transação
    ca.status_cartao,
    ca.tipo_cartao,
    ca.limite
  FROM workspace.default.silver_transacoes t
  LEFT JOIN workspace.default.silver_cartoes ca ON t.id_cartao = ca.id_cartao
  LEFT JOIN workspace.default.silver_contas c ON ca.id_conta = c.id_conta
  LEFT JOIN workspace.default.silver_estornos e ON t.id_transacao = e.id_transacao
  WHERE ca.status_cartao = 'ATIVO'  -- Cartões ativos
    AND t.valor > 0                  -- Transações válidas
)

SELECT * FROM transacoes_validas;

-- COMMAND ----------

CREATE OR REPLACE VIEW workspace.default.gold_dim_cliente AS
SELECT 
  id_cliente,
  cpf,
  nome,
  cidade,
  estado,
  renda,
  segmento,
  data_atualizacao,
  data_processamento,
  -- Contagem de contas por cliente
  (SELECT COUNT(*) FROM workspace.default.silver_contas c WHERE c.id_cliente = cl.id_cliente) AS total_contas,
  -- Contagem de cartões por cliente
  (SELECT COUNT(*) 
   FROM workspace.default.silver_cartoes ca 
   JOIN workspace.default.silver_contas c ON ca.id_conta = c.id_conta 
   WHERE c.id_cliente = cl.id_cliente) AS total_cartoes
FROM workspace.default.silver_clientes cl;

-- COMMAND ----------

CREATE OR REPLACE VIEW workspace.default.gold_cliente_mes AS
WITH base_mensal AS (
  SELECT 
    c.id_cliente,
    DATE_TRUNC('MONTH', t.data_transacao) AS mes_referencia,
    COUNT(t.id_transacao) AS qtde_transacoes,
    SUM(t.valor) AS valor_total,
    AVG(t.valor) AS valor_medio,
    SUM(CASE WHEN t.canal = 'ONLINE' THEN 1 ELSE 0 END) AS qtde_online,
    SUM(CASE WHEN t.canal = 'PRESENCIAL' THEN 1 ELSE 0 END) AS qtde_presencial,
    COUNT(DISTINCT t.estabelecimento) AS qtde_estabelecimentos
  FROM workspace.default.silver_transacoes t
  JOIN workspace.default.silver_cartoes ca ON t.id_cartao = ca.id_cartao
  JOIN workspace.default.silver_contas c ON ca.id_conta = c.id_conta
  WHERE t.valor > 0
  GROUP BY c.id_cliente, DATE_TRUNC('MONTH', t.data_transacao)
)

SELECT 
  id_cliente,
  mes_referencia,
  qtde_transacoes,
  valor_total,
  valor_medio,
  qtde_online,
  qtde_presencial,
  qtde_estabelecimentos,
  -- Percentual de transações online
  ROUND(qtde_online / NULLIF(qtde_transacoes, 0) * 100, 2) AS pct_online
FROM base_mensal;

-- COMMAND ----------

CREATE OR REPLACE VIEW workspace.default.gold_indicadores_risco AS
SELECT 
  r.id_evento,
  r.id_transacao,
  r.tipo_evento,
  r.severidade,
  r.data_evento,
  t.id_cartao,
  t.valor AS valor_transacao,
  t.estabelecimento,
  t.data_transacao,
  c.id_cliente,
  -- Dias entre transação e evento
  DATEDIFF(r.data_evento, t.data_transacao) AS dias_para_evento,
  -- Classificação de severidade
  CASE 
    WHEN r.severidade = 'ALTA' THEN 3
    WHEN r.severidade = 'MEDIA' THEN 2
    WHEN r.severidade = 'BAIXA' THEN 1
    ELSE 0
  END AS score_severidade
FROM workspace.default.silver_eventos_risco r
LEFT JOIN workspace.default.silver_transacoes t ON r.id_transacao = t.id_transacao
LEFT JOIN workspace.default.silver_cartoes ca ON t.id_cartao = ca.id_cartao
LEFT JOIN workspace.default.silver_contas c ON ca.id_conta = c.id_conta;

-- COMMAND ----------

CREATE OR REPLACE VIEW workspace.default.gold_features_cliente AS
WITH transacoes_agregadas AS (
  SELECT 
    c.id_cliente,
    COUNT(t.id_transacao) AS total_transacoes,
    SUM(t.valor) AS total_gasto,
    AVG(t.valor) AS ticket_medio,
    MAX(t.valor) AS max_transacao,
    MIN(t.valor) AS min_transacao,
    COUNT(DISTINCT t.estabelecimento) AS diversidade_estabelecimentos,
    COUNT(DISTINCT t.mcc) AS diversidade_mcc,
    SUM(CASE WHEN t.canal = 'ONLINE' THEN 1 ELSE 0 END) AS transacoes_online,
    SUM(CASE WHEN t.canal = 'PRESENCIAL' THEN 1 ELSE 0 END) AS transacoes_presencial,
    -- Última transação
    MAX(t.data_transacao) AS ultima_transacao,
    DATEDIFF(CURRENT_DATE(), MAX(t.data_transacao)) AS dias_sem_transacao
  FROM workspace.default.silver_transacoes t
  JOIN workspace.default.silver_cartoes ca ON t.id_cartao = ca.id_cartao
  JOIN workspace.default.silver_contas c ON ca.id_conta = c.id_conta
  WHERE t.valor > 0
  GROUP BY c.id_cliente
),

risco_agregado AS (
  SELECT 
    c.id_cliente,
    COUNT(r.id_evento) AS total_eventos_risco,
    SUM(CASE WHEN r.severidade = 'ALTA' THEN 1 ELSE 0 END) AS eventos_alta,
    SUM(CASE WHEN r.severidade = 'MEDIA' THEN 1 ELSE 0 END) AS eventos_media,
    SUM(CASE WHEN r.severidade = 'BAIXA' THEN 1 ELSE 0 END) AS eventos_baixa
  FROM workspace.default.silver_eventos_risco r
  JOIN workspace.default.silver_transacoes t ON r.id_transacao = t.id_transacao
  JOIN workspace.default.silver_cartoes ca ON t.id_cartao = ca.id_cartao
  JOIN workspace.default.silver_contas c ON ca.id_conta = c.id_conta
  GROUP BY c.id_cliente
)

SELECT 
  cl.id_cliente,
  cl.nome,
  cl.cidade,
  cl.estado,
  cl.renda,
  cl.segmento,
  COALESCE(ta.total_transacoes, 0) AS total_transacoes,
  COALESCE(ta.total_gasto, 0) AS total_gasto,
  COALESCE(ta.ticket_medio, 0) AS ticket_medio,
  COALESCE(ta.max_transacao, 0) AS max_transacao,
  COALESCE(ta.diversidade_estabelecimentos, 0) AS diversidade_estabelecimentos,
  COALESCE(ta.transacoes_online, 0) AS transacoes_online,
  COALESCE(ta.dias_sem_transacao, 999) AS dias_sem_transacao,
  COALESCE(ra.total_eventos_risco, 0) AS total_eventos_risco,
  COALESCE(ra.eventos_alta, 0) AS eventos_risco_alta,
  -- Score de risco (quanto maior, mais arriscado)
  COALESCE(ra.eventos_alta * 3 + ra.eventos_media * 2 + ra.eventos_baixa * 1, 0) AS score_risco
FROM workspace.default.silver_clientes cl
LEFT JOIN transacoes_agregadas ta ON cl.id_cliente = ta.id_cliente
LEFT JOIN risco_agregado ra ON cl.id_cliente = ra.id_cliente;

-- COMMAND ----------

CREATE OR REPLACE VIEW workspace.default.gold_features_cliente AS
WITH transacoes_agregadas AS (
  SELECT 
    c.id_cliente,
    COUNT(t.id_transacao) AS total_transacoes,
    SUM(t.valor) AS total_gasto,
    AVG(t.valor) AS ticket_medio,
    MAX(t.valor) AS max_transacao,
    MIN(t.valor) AS min_transacao,
    COUNT(DISTINCT t.estabelecimento) AS diversidade_estabelecimentos,
    COUNT(DISTINCT t.mcc) AS diversidade_mcc,
    SUM(CASE WHEN t.canal = 'ONLINE' THEN 1 ELSE 0 END) AS transacoes_online,
    SUM(CASE WHEN t.canal = 'PRESENCIAL' THEN 1 ELSE 0 END) AS transacoes_presencial,
    -- Última transação
    MAX(t.data_transacao) AS ultima_transacao,
    DATEDIFF(CURRENT_DATE(), MAX(t.data_transacao)) AS dias_sem_transacao
  FROM workspace.default.silver_transacoes t
  JOIN workspace.default.silver_cartoes ca ON t.id_cartao = ca.id_cartao
  JOIN workspace.default.silver_contas c ON ca.id_conta = c.id_conta
  WHERE t.valor > 0
  GROUP BY c.id_cliente
),

risco_agregado AS (
  SELECT 
    c.id_cliente,
    COUNT(r.id_evento) AS total_eventos_risco,
    SUM(CASE WHEN r.severidade = 'ALTA' THEN 1 ELSE 0 END) AS eventos_alta,
    SUM(CASE WHEN r.severidade = 'MEDIA' THEN 1 ELSE 0 END) AS eventos_media,
    SUM(CASE WHEN r.severidade = 'BAIXA' THEN 1 ELSE 0 END) AS eventos_baixa
  FROM workspace.default.silver_eventos_risco r
  JOIN workspace.default.silver_transacoes t ON r.id_transacao = t.id_transacao
  JOIN workspace.default.silver_cartoes ca ON t.id_cartao = ca.id_cartao
  JOIN workspace.default.silver_contas c ON ca.id_conta = c.id_conta
  GROUP BY c.id_cliente
)

SELECT 
  cl.id_cliente,
  cl.nome,
  cl.cidade,
  cl.estado,
  cl.renda,
  cl.segmento,
  COALESCE(ta.total_transacoes, 0) AS total_transacoes,
  COALESCE(ta.total_gasto, 0) AS total_gasto,
  COALESCE(ta.ticket_medio, 0) AS ticket_medio,
  COALESCE(ta.max_transacao, 0) AS max_transacao,
  COALESCE(ta.diversidade_estabelecimentos, 0) AS diversidade_estabelecimentos,
  COALESCE(ta.transacoes_online, 0) AS transacoes_online,
  COALESCE(ta.dias_sem_transacao, 999) AS dias_sem_transacao,
  COALESCE(ra.total_eventos_risco, 0) AS total_eventos_risco,
  COALESCE(ra.eventos_alta, 0) AS eventos_risco_alta,
  -- Score de risco (quanto maior, mais arriscado)
  COALESCE(ra.eventos_alta * 3 + ra.eventos_media * 2 + ra.eventos_baixa * 1, 0) AS score_risco
FROM workspace.default.silver_clientes cl
LEFT JOIN transacoes_agregadas ta ON cl.id_cliente = ta.id_cliente
LEFT JOIN risco_agregado ra ON cl.id_cliente = ra.id_cliente;

-- COMMAND ----------

CREATE OR REPLACE VIEW workspace.default.gold_analise_comportamento AS
WITH transacoes_mensais AS (
  SELECT 
    c.id_cliente,
    DATE_TRUNC('MONTH', t.data_transacao) AS mes,
    SUM(t.valor) AS gasto_mensal,
    COUNT(t.id_transacao) AS qtde_mensal
  FROM workspace.default.silver_transacoes t
  JOIN workspace.default.silver_cartoes ca ON t.id_cartao = ca.id_cartao
  JOIN workspace.default.silver_contas c ON ca.id_conta = c.id_conta
  WHERE t.valor > 0
  GROUP BY c.id_cliente, DATE_TRUNC('MONTH', t.data_transacao)
),

comportamento AS (
  SELECT 
    id_cliente,
    mes,
    gasto_mensal,
    qtde_mensal,
    -- LAG: mês anterior
    LAG(gasto_mensal, 1) OVER (PARTITION BY id_cliente ORDER BY mes) AS gasto_mes_anterior,
    -- LEAD: próximo mês
    LEAD(gasto_mensal, 1) OVER (PARTITION BY id_cliente ORDER BY mes) AS gasto_proximo_mes,
    -- FIRST_VALUE: primeiro mês
    FIRST_VALUE(gasto_mensal) OVER (PARTITION BY id_cliente ORDER BY mes) AS gasto_primeiro_mes,
    -- LAST_VALUE: último mês
    LAST_VALUE(gasto_mensal) OVER (PARTITION BY id_cliente ORDER BY mes 
      ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS gasto_ultimo_mes,
    -- Variação percentual
    ROUND((gasto_mensal - LAG(gasto_mensal, 1) OVER (PARTITION BY id_cliente ORDER BY mes)) / 
      NULLIF(LAG(gasto_mensal, 1) OVER (PARTITION BY id_cliente ORDER BY mes), 0) * 100, 2) AS variacao_pct
  FROM transacoes_mensais
)

SELECT 
  id_cliente,
  mes,
  gasto_mensal,
  qtde_mensal,
  gasto_mes_anterior,
  gasto_proximo_mes,
  gasto_primeiro_mes,
  gasto_ultimo_mes,
  variacao_pct,
  -- NTILE: segmentação em quartis por gasto
  NTILE(4) OVER (ORDER BY gasto_mensal) AS quartil_gasto,
  -- PERCENT_RANK
  ROUND(PERCENT_RANK() OVER (ORDER BY gasto_mensal) * 100, 2) AS percentil_gasto
FROM comportamento
ORDER BY id_cliente, mes;

-- COMMAND ----------

CREATE OR REPLACE VIEW workspace.default.gold_anomalias_transacionais AS
WITH stats_cliente AS (
  SELECT 
    c.id_cliente,
    AVG(t.valor) AS avg_valor,
    STDDEV(t.valor) AS std_valor
  FROM workspace.default.silver_transacoes t
  JOIN workspace.default.silver_cartoes ca ON t.id_cartao = ca.id_cartao
  JOIN workspace.default.silver_contas c ON ca.id_conta = c.id_conta
  WHERE t.valor > 0
  GROUP BY c.id_cliente
)

SELECT 
  t.id_transacao,
  t.id_cartao,
  c.id_cliente,
  t.data_transacao,
  t.valor,
  t.estabelecimento,
  t.canal,
  s.avg_valor,
  s.std_valor,
  -- Anomalia: valor acima de 3 desvios padrão
  CASE 
    WHEN s.std_valor > 0 AND t.valor > (s.avg_valor + 3 * s.std_valor) THEN 'VALOR_EXTREMO'
    WHEN t.valor > 5000 THEN 'VALOR_ALTO'
    ELSE 'NORMAL'
  END AS tipo_anomalia,
  -- Comparação contra média do cliente
  ROUND((t.valor - s.avg_valor) / NULLIF(s.std_valor, 0), 2) AS zscore,
  -- Comparação contra média do segmento
  ROUND(t.valor / NULLIF(
    (SELECT AVG(t2.valor) 
     FROM workspace.default.silver_transacoes t2
     JOIN workspace.default.silver_cartoes ca2 ON t2.id_cartao = ca2.id_cartao
     JOIN workspace.default.silver_contas c2 ON ca2.id_conta = c2.id_conta
     JOIN workspace.default.silver_clientes cl2 ON c2.id_cliente = cl2.id_cliente
     WHERE cl2.segmento = cl.segmento
    ), 0), 2) AS ratio_vs_segmento
FROM workspace.default.silver_transacoes t
JOIN workspace.default.silver_cartoes ca ON t.id_cartao = ca.id_cartao
JOIN workspace.default.silver_contas c ON ca.id_conta = c.id_conta
JOIN workspace.default.silver_clientes cl ON c.id_cliente = cl.id_cliente
LEFT JOIN stats_cliente s ON c.id_cliente = s.id_cliente
WHERE t.valor > 0;