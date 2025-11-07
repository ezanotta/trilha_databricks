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
-- DBTITLE 1,Exibe métricas
SELECT
  COUNT(*) AS total_enriched_records,
  COUNT(DISTINCT claim_no) AS unique_claims,
  COUNT(DISTINCT policy_no) AS unique_policies,
  COUNT(DISTINCT customer_id) AS unique_customers,
  CONCAT('Join concluido. Total de registros enriquecidos: ', COUNT(*)) AS result_message
FROM ${catalog}.${schema_silver}.claims_enriched;

