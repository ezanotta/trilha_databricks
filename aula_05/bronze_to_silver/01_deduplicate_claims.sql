-- Databricks notebook source
-- COMMAND ----------
-- DBTITLE 1,Deduplicação de Claims
-- Deduplicação de claims da camada bronze para silver
-- Remove duplicatas mantendo apenas o registro mais recente por claim_no
-- Variáveis esperadas: ${catalog}, ${schema_bronze}, ${schema_silver}

-- COMMAND ----------
-- DBTITLE 1,Configuração do catálogo e schema
USE CATALOG ${catalog};
USE SCHEMA ${schema_silver};

-- COMMAND ----------
-- DBTITLE 1,Estatísticas antes da deduplicação
-- Calcula métricas antes do processo de deduplicação
SELECT
  COUNT(*) AS total_rows_before,
  COUNT(DISTINCT claim_no) AS unique_claims_before,
  COUNT(*) - COUNT(DISTINCT claim_no) AS duplicate_rows
FROM ${catalog}.${schema_bronze}.claims;

-- COMMAND ----------
-- DBTITLE 1,Remove tabela se existir
DROP TABLE IF EXISTS ${catalog}.${schema_silver}.claims_dedup;

-- COMMAND ----------
-- DBTITLE 1,Cria tabela deduplicada
-- Deduplica mantendo apenas o registro mais recente por claim_no
-- Ordena por claim_date DESC NULLS LAST para manter a data mais recente
CREATE TABLE ${catalog}.${schema_silver}.claims_dedup
AS
SELECT *
FROM (
  SELECT 
    *,
    ROW_NUMBER() OVER (
      PARTITION BY claim_no 
      ORDER BY claim_date DESC NULLS LAST
    ) AS rn
  FROM ${catalog}.${schema_bronze}.claims
)
WHERE rn = 1;

-- COMMAND ----------
-- DBTITLE 1,Estatísticas depois da deduplicação
-- Verifica métricas após o processo de deduplicação
SELECT
  COUNT(*) AS total_rows_after,
  COUNT(DISTINCT claim_no) AS unique_claims_after
FROM ${catalog}.${schema_silver}.claims_dedup;

