-- Databricks notebook source
-- COMMAND ----------
-- DBTITLE 1,Métricas da tabela claims_enriched
-- Task: Gerar métricas da tabela claims_enriched
-- Variáveis esperadas: ${catalog}, ${schema_silver}, ${schema_gold}

-- COMMAND ----------
-- DBTITLE 1,Configuração do catálogo e schema
USE CATALOG ${catalog};
USE SCHEMA ${schema_gold};

-- COMMAND ----------
-- DBTITLE 1,Remove view se existir
DROP VIEW IF EXISTS ${catalog}.${schema_gold}.claims_metrics;

-- COMMAND ----------
-- DBTITLE 1,Cria view com métricas
-- Cria view na camada gold com métricas agregadas da tabela claims_enriched
-- A view sempre calcula as métricas dinamicamente quando consultada,
-- garantindo que os dados estejam sempre atualizados
CREATE VIEW ${catalog}.${schema_gold}.claims_metrics
AS
SELECT
  COUNT(*) AS total_enriched_records,
  COUNT(DISTINCT claim_no) AS unique_claims,
  COUNT(DISTINCT policy_no) AS unique_policies,
  COUNT(DISTINCT customer_id) AS unique_customers,
  CONCAT('Join concluido. Total de registros enriquecidos: ', COUNT(*)) AS result_message,
  current_timestamp() AS calculated_at
FROM ${catalog}.${schema_silver}.claims_enriched;

-- COMMAND ----------
-- DBTITLE 1,Exibe métricas calculadas
-- A view calcula as métricas dinamicamente sempre que consultada
SELECT * FROM ${catalog}.${schema_gold}.claims_metrics;

