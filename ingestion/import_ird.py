import pandas as pd
import psycopg2
from pathlib import Path
import json
import re

DB_NAME = "educacao"
DB_HOST = "localhost"
DB_PORT = "5432"
DB_USER = "mairasoaressales"

BASE_DIR = Path(__file__).resolve().parent.parent
DATA_DIR = BASE_DIR / "extracted" / "ird"

def extrair_ano(texto):
    m = re.search(r"20\d{2}", str(texto))
    return int(m.group()) if m else None

def encontrar_linha_cabecalho(file_path):
    previa = pd.read_excel(file_path, header=None, nrows=20)
    for i in range(len(previa)):
        linha = [str(x).strip().upper() for x in previa.iloc[i].tolist()]
        if "CO_ENTIDADE" in linha or "ID_ESCOLA" in linha:
            return i
    return None

def main():
    conn = psycopg2.connect(
        dbname=DB_NAME,
        user=DB_USER,
        host=DB_HOST,
        port=DB_PORT
    )
    cur = conn.cursor()

    arquivos = sorted(DATA_DIR.rglob("*.xlsx"))

    print("Arquivos encontrados:")
    for arq in arquivos:
        print("-", arq)

    for file_path in arquivos:
        ano = extrair_ano(file_path.name) or extrair_ano(file_path)

        if not ano:
            print("Ano não identificado:", file_path)
            continue

        print(f"\nLendo: {file_path}")

        linha_cabecalho = encontrar_linha_cabecalho(file_path)
        print("Linha de cabeçalho encontrada:", linha_cabecalho)

        if linha_cabecalho is None:
            print("Cabeçalho real não encontrado. Pulando arquivo.")
            continue

        df = pd.read_excel(file_path, header=linha_cabecalho)
        df.columns = [str(c).strip() for c in df.columns]

        print("Colunas encontradas:", list(df.columns))

        if "CO_ENTIDADE" not in df.columns and "ID_ESCOLA" in df.columns:
            df["CO_ENTIDADE"] = df["ID_ESCOLA"]

        if "CO_ENTIDADE" not in df.columns:
            print("CO_ENTIDADE não encontrado. Pulando arquivo.")
            continue

        for _, row in df.iterrows():
            if pd.isna(row["CO_ENTIDADE"]):
                continue

            try:
                co_entidade = int(float(row["CO_ENTIDADE"]))
            except Exception:
                continue

            row_dict = row.where(pd.notna(row), None).to_dict()
            dados_json = json.dumps(row_dict, ensure_ascii=False)

            cur.execute("""
            INSERT INTO ird_escola (ano, co_entidade, dados_json)
            VALUES (%s, %s, %s::jsonb)
            ON CONFLICT (ano, co_entidade)
            DO UPDATE SET dados_json = EXCLUDED.dados_json;
            """, (ano, co_entidade, dados_json))

        conn.commit()
        print(f"Importado: {file_path.name}")

    cur.close()
    conn.close()
    print("\nImportação do IRD finalizada.")

if __name__ == "__main__":
    main()
