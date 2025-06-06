import logging
import os

import duckdb

# Base path (resolve onde o script está localizado)
BASE_DIR = os.path.dirname(os.path.abspath(__file__))
logger = logging.getLogger(__name__)

# Caminhos absolutos baseados no diretório do script
sql_path = os.path.join(BASE_DIR, "materialize_primary")
output_path = os.path.join(BASE_DIR, "../../data/03_primary")

# Lista de tabelas a serem processadas
tables = ["vagas", "prospects"]

# Garante que o diretório de saída existe
os.makedirs(output_path, exist_ok=True)

# Processar cada .sql
for table in tables:
    sql_file = os.path.join(sql_path, f"{table}.sql")
    output_file = os.path.join(output_path, f"{table}.parquet")

    if not os.path.exists(sql_file):
        raise FileNotFoundError(f"Arquivo SQL não encontrado: {sql_file}")

    with open(sql_file, encoding="utf-8") as f:
        query = f.read()

    # Executa a query e salva o resultado em Parquet
    df = duckdb.sql(query).df()
    df.to_parquet(output_file, index=False)

    logger.info(f"Processado {table}: {len(df)} linhas")
