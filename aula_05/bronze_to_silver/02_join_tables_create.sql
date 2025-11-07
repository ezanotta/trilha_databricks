-- Databricks notebook source
-- COMMAND ----------
-- DBTITLE 1,Join de Tabelas - Criar claims_enriched
-- Task: Criar tabela claims_enriched combinando claims, policies e customers
-- Variáveis esperadas: ${catalog}, ${schema_bronze}, ${schema_silver}

-- COMMAND ----------
-- DBTITLE 1,Configuração do catálogo e schema
USE CATALOG ${catalog};
USE SCHEMA ${schema_silver};

-- COMMAND ----------
-- DBTITLE 1,Remove tabela se existir
DROP TABLE IF EXISTS ${catalog}.${schema_silver}.claims_enriched;

-- COMMAND ----------
-- DBTITLE 1,Cria tabela claims_enriched com joins
-- Combina claims deduplicados com policies e customers
CREATE TABLE ${catalog}.${schema_silver}.claims_enriched
AS
SELECT
  c.claim_no,
  c.policy_no,
  c.claim_date,
  c.months_as_customer,
  c.injury,
  c.property,
  c.vehicle,
  c.total AS claim_total,
  c.collision_type,
  c.number_of_vehicles_involved,
  c.age,
  c.insured_relationship,
  c.license_issue_date,
  c.date,
  c.hour,
  c.type,
  c.severity,
  c.number_of_witnesses,
  c.suspicious_activity,
  p.CUST_ID,
  p.POLICYTYPE,
  p.POL_ISSUE_DATE,
  p.POL_EFF_DATE,
  p.POL_EXPIRY_DATE,
  p.MAKE,
  p.MODEL,
  p.MODEL_YEAR,
  p.CHASSIS_NO,
  p.USE_OF_VEHICLE,
  p.PRODUCT,
  p.SUM_INSURED,
  p.PREMIUM,
  p.DEDUCTABLE,
  cust.customer_id,
  cust.date_of_birth,
  cust.borough,
  cust.neighborhood,
  cust.zip_code,
  cust.name AS customer_name,
  current_timestamp() AS processed_at
FROM ${catalog}.${schema_silver}.claims_dedup c
INNER JOIN ${catalog}.${schema_bronze}.policies p
  ON CAST(c.policy_no AS STRING) = CAST(p.POLICY_NO AS STRING)
INNER JOIN ${catalog}.${schema_bronze}.customers cust
  ON CAST(p.CUST_ID AS DOUBLE) = CAST(cust.customer_id AS DOUBLE);

