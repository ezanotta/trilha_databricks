-- Databricks notebook source
-- COMMAND ----------
-- Ingestão batch de dados de claims para a camada bronze
-- Este script processa TODOS os arquivos claims*.csv do volume automaticamente
-- Volume: /Volumes/${catalog}/00_landing/sql_server/
-- Arquivos processados: claims.csv, claims_02.csv, claims_03.csv, etc. (todos automaticamente)
-- Destino: ${catalog}.${schema}.claims
--
-- VARIÁVEIS DE TASK (definidas no Lakeflow Job):
--   ${catalog} - Nome do catálogo (ex: smart_claims_dev)
--   ${schema}  - Nome do schema (ex: 01_bronze)
--
-- IMPORTANTE: Este script reprocessa TODOS os arquivos claims*.csv sempre que executado
-- Ideal para reprocessar dados quando novos arquivos chegam com o mesmo schema
-- O padrão glob claims*.csv processa automaticamente todos os arquivos que correspondem

-- COMMAND ----------
-- DBTITLE 1,Configuração do catálogo e schema
USE CATALOG ${catalog};
USE SCHEMA ${schema};

-- COMMAND ----------
-- Remove a tabela se existir (para re-ingestão completa)
DROP TABLE IF EXISTS ${catalog}.${schema}.claims;

-- Cria a tabela bronze processando TODOS os arquivos claims*.csv automaticamente
-- O padrão glob 'claims*.csv' processa todos os arquivos que começam com 'claims' e terminam com '.csv'
-- Isso inclui: claims.csv, claims_02.csv, claims_03.csv, etc. sem necessidade de adicionar manualmente
--
-- NOTA: Se o padrão glob não funcionar no read_files(), você pode usar a abordagem alternativa abaixo
-- (descomente e ajuste conforme necessário):
--
-- CREATE TABLE ${catalog}.${schema}.claims
-- AS 
-- SELECT * FROM read_files('/Volumes/${catalog}/00_landing/sql_server/claims.csv', format => 'csv')
-- UNION ALL
-- SELECT * FROM read_files('/Volumes/${catalog}/00_landing/sql_server/claims_02.csv', format => 'csv');

-- COMMAND ----------
-- Cria a tabela bronze processando TODOS os arquivos claims*.csv automaticamente
CREATE TABLE ${catalog}.${schema}.claims
AS SELECT *
FROM read_files(
  '/Volumes/${catalog}/00_landing/sql_server/claims*.csv',
  format => 'csv'
);

