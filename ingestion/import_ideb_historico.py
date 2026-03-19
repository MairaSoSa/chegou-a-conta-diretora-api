import pandas as pd
from pathlib import Path
import json
import re
import unicodedata

from db_config import get_psycopg2_connection

BASE_DIR = Path(__file__).resolve().parent.parent
DATA_DIR = BASE_DIR / "extracted"

ANOS_VALIDOS = [2005, 2007, 2009, 2011, 2013, 2015, 2017, 2019, 2021, 2023]

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
        if "CODIGO DA ESCOLA" in linha or "CO_ENTIDADE" in linha:
            return i

    return None

def ajustar_coluna_escola(df):
    mapa = {normalizar(c): c for c in df.columns}

    for cand in ["CO_ENTIDADE", "CODIGO DA ESCOLA", "CODIGO", "ID_ESCOLA"]:
        if cand in mapa:
            df["CO_ENTIDADE"] = df[mapa[cand]]
            return df

    return df

def limpar_numero(v):
    if pd.isna(v):
        return None
    if isinstance(v, str):
        v = v.strip()
        if v in ["--", "-", ""]:
            return None
        v = v.replace(",", ".")
    try:
        return float(v)
    except Exception:
        return None

def achar_coluna_por_ano(df, prefixo, ano):
    alvo1 = normalizar(f"{prefixo} - {ano}")
    alvo2 = normalizar(f"{prefixo} {ano}")
    alvo3 = normalizar(f"{prefixo}\n{ano}")
    alvo4 = normalizar(f"{prefixo} - {str(ano).replace('2021','20211')}")

    for c in df.columns:
        n = normalizar(c)
        if n in [alvo1, alvo2, alvo3, alvo4]:
            return c
    return None

def main():
    conn = get_psycopg2_connection()
    cur = conn.cursor()

    arquivos = sorted(DATA_DIR.rglob("divulgacao_*_escolas_*.xlsx"))

    print("Arquivos encontrados:")
    for a in arquivos:
        print("-", a)

    for file in arquivos:
        etapa = detectar_etapa(file.name)

        print("\nProcessando:", file)
        print("Etapa:", etapa)

        header = encontrar_cabecalho(file)
        if header is None:
            print("Cabeçalho não encontrado.")
            continue

        print("Linha de cabeçalho:", header)

        df = pd.read_excel(file, header=header)
        df.columns = [str(c).strip() for c in df.columns]
        df = ajustar_coluna_escola(df)

        if "CO_ENTIDADE" not in df.columns:
            print("Código da escola não encontrado.")
            continue

        total = 0

        for _, row in df.iterrows():
            if pd.isna(row["CO_ENTIDADE"]):
                continue

            try:
                escola = int(float(row["CO_ENTIDADE"]))
            except Exception:
                continue

            for ano in ANOS_VALIDOS:
                col_aprov = achar_coluna_por_ano(df, "Taxa de Aprovação", ano)
                col_saeb = achar_coluna_por_ano(df, "Nota SAEB", ano)

                col_ideb = None
                for c in df.columns:
                    nc = normalizar(c)
                    if "IDEB" in nc and str(ano) in nc:
                        col_ideb = c
                        break

                taxa_aprovacao = limpar_numero(row[col_aprov]) if col_aprov else None
                nota_saeb = limpar_numero(row[col_saeb]) if col_saeb else None
                ideb = limpar_numero(row[col_ideb]) if col_ideb else None

                if taxa_aprovacao is None and nota_saeb is None and ideb is None:
                    continue

                dados = {
                    "taxa_aprovacao_coluna": col_aprov,
                    "nota_saeb_coluna": col_saeb,
                    "ideb_coluna": col_ideb,
                    "taxa_aprovacao": taxa_aprovacao,
                    "nota_saeb": nota_saeb,
                    "ideb": ideb,
                }

                cur.execute("""
                INSERT INTO ideb_escola_historico
                (ano, co_entidade, etapa, ideb, nota_saeb, taxa_aprovacao, dados_json)
                VALUES (%s, %s, %s, %s, %s, %s, %s::jsonb)
                ON CONFLICT (ano, co_entidade, etapa)
                DO UPDATE SET
                    ideb = EXCLUDED.ideb,
                    nota_saeb = EXCLUDED.nota_saeb,
                    taxa_aprovacao = EXCLUDED.taxa_aprovacao,
                    dados_json = EXCLUDED.dados_json;
                """, (
                    ano,
                    escola,
                    etapa,
                    ideb,
                    nota_saeb,
                    taxa_aprovacao,
                    json.dumps(dados, ensure_ascii=False)
                ))

                total += 1

        conn.commit()
        print("Registros importados:", total)

    cur.close()
    conn.close()
    print("\nImportação do IDEB histórico finalizada.")

if __name__ == "__main__":
    main()
