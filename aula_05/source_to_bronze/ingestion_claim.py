"""
Ingestão batch de dados de claims para a camada bronze
Este script processa TODOS os arquivos claims*.csv do volume automaticamente
Volume: /Volumes/${catalog}/00_landing/sql_server/
Arquivos processados: claims.csv, claims_02.csv, claims_03.csv, etc. (todos automaticamente)
Destino: ${catalog}.${schema}.claims

VARIÁVEIS DE TASK (definidas no Lakeflow Job):
  catalog - Nome do catálogo (ex: smart_claims_dev)
  schema  - Nome do schema (ex: 01_bronze)

IMPORTANTE: Este script reprocessa TODOS os arquivos claims*.csv sempre que executado
Ideal para reprocessar dados quando novos arquivos chegam com o mesmo schema
O padrão glob claims*.csv processa automaticamente todos os arquivos que correspondem
"""


def _get_widget_or_env(name: str, env_var: str) -> str:
    value = None
    if "dbutils" in globals():
        try:
            value = dbutils.widgets.get(name).strip()
        except Exception:
            value = None
    if not value:
        import os

        value = os.getenv(env_var, "").strip()
    if not value:
        raise ValueError(f"Missing configuration for {name} / {env_var}")
    return value


def main() -> None:
    catalog = _get_widget_or_env("catalog", "CATALOG")
    schema = _get_widget_or_env("schema", "SCHEMA")

    spark.sql(f"USE CATALOG {catalog}")
    spark.sql(f"USE SCHEMA {schema}")

    volume_path = f"/Volumes/{catalog}/00_landing/sql_server/claims*.csv"
    target_table = f"{catalog}.{schema}.claims"

    spark.sql(f"DROP TABLE IF EXISTS {target_table}")

    claims_df = (
        spark.read.format("csv")
        .option("header", "true")
        .option("inferSchema", "true")
        .load(volume_path)
        .cache()
    )

    row_count = claims_df.count()

    (
        claims_df.write.mode("overwrite")
        .option("overwriteSchema", "true")
        .saveAsTable(target_table)
    )

    print(f"Ingestão concluída. Total de registros processados: {row_count}")
    print(f"Tabela criada: {target_table}")

    claims_df.unpersist()


if __name__ == "__main__":
    main()

