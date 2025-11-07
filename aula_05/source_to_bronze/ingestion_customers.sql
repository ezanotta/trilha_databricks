-- Ingestão batch de dados de customers para a camada bronze
-- Este script assume que o arquivo customers.csv já está disponível no volume
-- Volume: /Volumes/smart_claims_dev/00_landing/sql_server/customers.csv
-- Destino: smart_claims_dev.01_bronze.customers

USE CATALOG smart_claims_dev;
USE SCHEMA 01_bronze;

-- Remove a tabela se existir (para re-ingestão)
DROP TABLE IF EXISTS smart_claims_dev.01_bronze.customers;

-- Cria a tabela bronze usando CREATE TABLE AS
CREATE TABLE smart_claims_dev.01_bronze.customers
AS SELECT *
FROM read_files(
  '/Volumes/smart_claims_dev/00_landing/sql_server/customers.csv',
  format => 'csv'
);

