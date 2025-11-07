-- Ingestão batch de dados de policies para a camada bronze
-- Este script assume que o arquivo policies.csv já está disponível no volume
-- Volume: /Volumes/smart_claims_dev/00_landing/sql_server/policies.csv
-- Destino: smart_claims_dev.01_bronze.policies

USE CATALOG smart_claims_dev;
USE SCHEMA 01_bronze;

-- Remove a tabela se existir (para re-ingestão)
DROP TABLE IF EXISTS smart_claims_dev.01_bronze.policies;

-- Cria a tabela bronze usando CREATE TABLE AS
CREATE TABLE smart_claims_dev.01_bronze.policies
AS SELECT *
FROM read_files(
  '/Volumes/smart_claims_dev/00_landing/sql_server/policies.csv',
  format => 'csv'
);

