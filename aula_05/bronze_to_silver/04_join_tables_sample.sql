-- Task: Apresentar amostra da tabela claims_enriched
-- Variaveis esperadas: ${catalog}, ${schema_silver}

USE CATALOG ${catalog};
USE SCHEMA ${schema_silver};

SELECT
  claim_no,
  customer_name,
  borough,
  MAKE,
  MODEL,
  claim_total,
  PREMIUM,
  severity,
  processed_at
FROM ${catalog}.${schema_silver}.claims_enriched
LIMIT 10;

