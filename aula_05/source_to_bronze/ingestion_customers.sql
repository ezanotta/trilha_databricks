-- Databricks notebook source
-- COMMAND ----------
-- Ingestão batch de dados de customers para a camada bronze
-- Este script assume que o arquivo customers.csv já está disponível no volume
-- Volume: /Volumes/${catalog}/00_landing/sql_server/customers.csv
-- Destino: ${catalog}.${schema_bronze}.customers
--
-- VARIÁVEIS DE TASK (definidas no Lakeflow Job):
--   ${catalog} - Nome do catálogo (ex: smart_claims_dev)
--   ${schema_bronze}  - Nome do schema bronze (ex: 01_bronze)

-- COMMAND ----------
-- DBTITLE 1,Configuração do catálogo e schema
USE CATALOG ${catalog};
USE SCHEMA ${schema_bronze};

-- COMMAND ----------
-- Remove a tabela se existir (para re-ingestão)
DROP TABLE IF EXISTS ${catalog}.${schema_bronze}.customers;

-- COMMAND ----------
-- Cria a tabela bronze usando CREATE TABLE AS
CREATE TABLE ${catalog}.${schema_bronze}.customers
AS SELECT *
FROM read_files(
  '/Volumes/${catalog}/00_landing/sql_server/customers.csv',
  format => 'csv'
);

