import pandas as pd
from pathlib import Path

# === Diretório de saída ===
output_dir = Path("dados_exemplo")
output_dir.mkdir(exist_ok=True)

# === Dados base ===
data = [
    {"id": 1, "nome": "Luciano", "idade": 34, "cidade": "São Paulo", "salario": 8500.50},
    {"id": 2, "nome": "Fernanda", "idade": 29, "cidade": "Rio de Janeiro", "salario": 9200.00},
    {"id": 3, "nome": "Marcos", "idade": 41, "cidade": "Belo Horizonte", "salario": 10500.75},
    {"id": 4, "nome": "Ana", "idade": 25, "cidade": "Curitiba", "salario": 7000.00},
    {"id": 5, "nome": "Pedro", "idade": 38, "cidade": "Salvador", "salario": 9700.25},
]

# === Criar DataFrame ===
df = pd.DataFrame(data)

print("Schema:")
print(df.dtypes)
print("\nPrévia:")
print(df.head())

# === 1) Gerar arquivo Parquet ===
parquet_path = output_dir / "dados.parquet"
df.to_parquet(parquet_path, index=False)
print(f"\n✅ Arquivo Parquet salvo em: {parquet_path.resolve()}")

# === 2) Gerar arquivo CSV ===
csv_path = output_dir / "dados.csv"
df.to_csv(csv_path, index=False, encoding="utf-8")
print(f"✅ Arquivo CSV salvo em: {csv_path.resolve()}")

# === 3) Gerar arquivo JSON ===
json_path = output_dir / "dados.json"
df.to_json(json_path, orient="records", lines=True, force_ascii=False)
print(f"✅ Arquivo JSON salvo em: {json_path.resolve()}")
