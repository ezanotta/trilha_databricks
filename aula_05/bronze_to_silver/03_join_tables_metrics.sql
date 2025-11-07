-- Task: Gerar metricas da tabela claims_enriched
-- Variaveis esperadas: ${catalog}, ${schema_silver}

USE CATALOG ${catalog};
USE SCHEMA ${schema_silver};

SELECT
  COUNT(*) AS total_enriched_records,
  COUNT(DISTINCT claim_no) AS unique_claims,
  COUNT(DISTINCT policy_no) AS unique_policies,
  COUNT(DISTINCT customer_id) AS unique_customers,
  CONCAT('Join concluido. Total de registros enriquecidos: ', COUNT(*)) AS result_message
FROM ${catalog}.${schema_silver}.claims_enriched;

