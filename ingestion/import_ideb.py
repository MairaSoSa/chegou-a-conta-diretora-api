import pandas as pd
from pathlib import Path
import json
import re
import unicodedata

from db_config import get_psycopg2_connection

BASE_DIR = Path(__file__).resolve().parent.parent
DATA_DIR = BASE_DIR / "extracted"

def extrair_ano(texto):
    m = re.search(r"20\d{2}", str(texto))
    return int(m.group()) if m else None

def normalizar(txt):
    txt = str(txt).strip()
    txt = unicodedata.normalize("NFKD", txt).encode("ASCII", "ignore").decode("ASCII")
    return txt.upper()

def detectar_etapa(nome):
    n = normalizar(nome)

    if "ANOS_INICIAIS" in n or "INICIAIS" in n:
        return "anos_iniciais"

    if "ANOS_FINAIS" in n or "FINAIS" in n:
        return "anos_finais"

    if "ENSINO_MEDIO" in n or "MEDIO" in n:
        return "ensino_medio"

    return "geral"

def encontrar_cabecalho(file):

    preview = pd.read_excel(file, header=None, nrows=25)

    for i in range(len(preview)):
        linha = [normalizar(x) for x in preview.iloc[i].tolist()]

        if (
            "CODIGO DA ESCOLA" in linha
            or "CO_ENTIDADE" in linha
            or "ID_ESCOLA" in linha
            or "CODIGO" in linha
        ):
            return i

    return None

def ajustar_coluna_escola(df):

    mapa = {normalizar(c): c for c in df.columns}

    candidatos = [
        "CO_ENTIDADE",
        "CODIGO DA ESCOLA",
        "CODIGO",
        "ID_ESCOLA"
    ]

    for c in candidatos:
        if c in mapa:
            df["CO_ENTIDADE"] = df[mapa[c]]
            return df

    return df

def achar_coluna_ano(df):

    mapa = {normalizar(c): c for c in df.columns}

    candidatos = [
        "ANO",
        "NU_ANO",
        "ANO IDEB",
        "ANO SAEB"
    ]

    for c in candidatos:
        if c in mapa:
            return mapa[c]

    return None

def main():
    conn = get_psycopg2_connection()

    cur = conn.cursor()

    arquivos = sorted(DATA_DIR.rglob("divulgacao_*_escolas_*.xlsx"))

    print("Arquivos encontrados:")
    for a in arquivos:
        print("-", a)

    if not arquivos:
        print("Nenhum arquivo IDEB encontrado.")
        cur.close()
        conn.close()
        return

    for file in arquivos:

        ano_arquivo = extrair_ano(file.name) or extrair_ano(file)

        etapa = detectar_etapa(file.name)

        print("\nProcessando:", file)
        print("Ano do arquivo:", ano_arquivo, "| Etapa:", etapa)

        header = encontrar_cabecalho(file)

        if header is None:
            print("Cabeçalho não encontrado.")
            continue

        print("Linha de cabeçalho:", header)

        df = pd.read_excel(file, header=header)

        df.columns = [str(c).strip() for c in df.columns]

        print("Colunas encontradas:", list(df.columns))

        df = ajustar_coluna_escola(df)

        if "CO_ENTIDADE" not in df.columns:
            print("Código da escola não encontrado.")
            continue

        col_ano = achar_coluna_ano(df)

        print("Coluna de ano identificada:", col_ano)

        total = 0

        for _, row in df.iterrows():

            if pd.isna(row["CO_ENTIDADE"]):
                continue

            try:
                escola = int(float(row["CO_ENTIDADE"]))
            except Exception:
                continue

            ano = ano_arquivo

            if col_ano and col_ano in df.columns:

                valor_ano = row[col_ano]

                if pd.notna(valor_ano):

                    try:
                        ano = int(float(valor_ano))
                    except Exception:
                        ano = ano_arquivo

            dados = row.where(pd.notna(row), None).to_dict()

            cur.execute("""
            INSERT INTO ideb_escola
            (ano, co_entidade, etapa, dados_json)
            VALUES (%s, %s, %s, %s::jsonb)
            ON CONFLICT (ano, co_entidade, etapa)
            DO UPDATE SET dados_json = EXCLUDED.dados_json;
            """, (
                ano,
                escola,
                etapa,
                json.dumps(dados, ensure_ascii=False)
            ))

            total += 1

        conn.commit()

        print("Importado:", total)

    cur.close()
    conn.close()

    print("\nImportação IDEB finalizada")

if __name__ == "__main__":
    main()
