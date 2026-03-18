import pandas as pd
import psycopg2
from pathlib import Path
import json

DB_NAME = "educacao"
DB_HOST = "localhost"
DB_PORT = "5432"
DB_USER = "mairasoaressales"  # ajuste se precisar

DATA_DIR = Path("data")

FILES = [
    DATA_DIR / "INSE_2021_escolas.xlsx",
    DATA_DIR / "INSE_2023_escolas.xlsx",
]

def main():
    conn = psycopg2.connect(
        dbname=DB_NAME,
        user=DB_USER,
        host=DB_HOST,
        port=DB_PORT
    )

    cur = conn.cursor()

    cur.execute("""
    CREATE TABLE IF NOT EXISTS inse_escola (
        ano INTEGER,
        co_entidade BIGINT,
        dados_json JSONB,
        PRIMARY KEY (ano, co_entidade)
    );
    """)
    conn.commit()

    for file_path in FILES:
        if not file_path.exists():
            print("Arquivo não encontrado:", file_path)
            continue

        print("\nLendo:", file_path)
        df = pd.read_excel(file_path)
        print("Colunas encontradas:", list(df.columns))

        if "CO_ENTIDADE" not in df.columns and "ID_ESCOLA" in df.columns:
            df["CO_ENTIDADE"] = df["ID_ESCOLA"]

        if "CO_ENTIDADE" not in df.columns:
            print("CO_ENTIDADE não encontrado. Pulando arquivo.")
            continue

        for _, row in df.iterrows():
            if pd.isna(row["CO_ENTIDADE"]) or pd.isna(row["NU_ANO_SAEB"]):
                continue

            co_entidade = int(row["CO_ENTIDADE"])
            ano = int(row["NU_ANO_SAEB"])

            row_dict = row.where(pd.notna(row), None).to_dict()
            dados_json = json.dumps(row_dict, ensure_ascii=False)

            cur.execute("""
            INSERT INTO inse_escola (ano, co_entidade, dados_json)
            VALUES (%s, %s, %s::jsonb)
            ON CONFLICT (ano, co_entidade)
            DO UPDATE SET dados_json = EXCLUDED.dados_json;
            """, (ano, co_entidade, dados_json))

        conn.commit()
        print("Importado:", file_path.name)

    cur.close()
    conn.close()
    print("\nImportação finalizada.")

if __name__ == "__main__":
    main()
