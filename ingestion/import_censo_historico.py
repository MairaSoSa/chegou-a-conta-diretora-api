"""
Importação do Censo Escolar (histórico completo) para censo_escola_historico.

Compatível com arquivos do MEC de QUALQUER ano (2007 em diante).
- Salva TODAS as colunas do censo no campo dados_json.
- Mantém colunas estruturadas (co_entidade, sg_uf, etc.) para filtros rápidos.
- Detecta o ano pelo conteúdo (NU_ANO_CENSO / AN_CENSO) ou pelo nome do arquivo.
- Suporta variações de nomes de colunas entre anos.
- Usa inserção em lote — muito mais rápido que linha a linha.

Como usar:
  Coloque os CSVs do censo na pasta extracted/ (qualquer subpasta).
  Execute: python ingestion/import_censo_historico.py
"""

import json
import re
import sys
from pathlib import Path

import numpy as np
import pandas as pd
from sqlalchemy import text

from db_config import get_sqlalchemy_engine

engine = get_sqlalchemy_engine()

BASE_DIR = Path(__file__).resolve().parent.parent
PASTA = BASE_DIR / "extracted"

COLUNAS_ANO = ["NU_ANO_CENSO", "AN_CENSO", "NU_ANO", "ANO_CENSO"]

MAPA_COLUNAS = {
    "co_entidade":               ["CO_ENTIDADE", "CO_ESCOLA", "ID_ESCOLA"],
    "no_entidade":               ["NO_ENTIDADE", "NO_ESCOLA"],
    "co_municipio":              ["CO_MUNICIPIO", "CO_MUN", "CO_MUNICIPIO_ESC"],
    "no_municipio":              ["NO_MUNICIPIO", "NO_MUN", "NO_MUNICIPIO_ESC"],
    "sg_uf":                     ["SG_UF", "SG_UF_ESC"],
    "tp_dependencia":            ["TP_DEPENDENCIA", "TP_DEP_ADM_ESCOLA", "CO_DEPENDENCIA_ADM"],
    "tp_localizacao":            ["TP_LOCALIZACAO", "TP_LOC_ESCOLA", "CO_LOCALIZACAO"],
    "tp_situacao_funcionamento": ["TP_SITUACAO_FUNCIONAMENTO", "IN_SITUACAO_FUNC", "CO_SITUACAO_FUNCIONAMENTO"],
}

TAMANHO_LOTE = 2000


def extrair_ano_do_nome(caminho: Path) -> int | None:
    for parte in [caminho.name] + list(caminho.parts):
        m = re.search(r"(20\d{2}|199\d|200[0-9])", parte)
        if m:
            return int(m.group())
    return None


def resolver_coluna(cols_upper: list[str], candidatos: list[str]) -> str | None:
    for c in candidatos:
        if c.upper() in cols_upper:
            return c.upper()
    return None


def limpar_valor(v):
    if v is None:
        return None
    if isinstance(v, float) and (np.isnan(v) or np.isinf(v)):
        return None
    if isinstance(v, (np.integer,)):
        return int(v)
    if isinstance(v, (np.floating,)):
        return float(v)
    return v


def row_para_json(row: pd.Series) -> str:
    d = {k: limpar_valor(v) for k, v in row.items()}
    return json.dumps(d, ensure_ascii=False, default=str)


def processar_arquivo(caminho: Path) -> int:
    print(f"\n→ Lendo: {caminho.name}")

    try:
        df = pd.read_csv(caminho, sep=";", encoding="latin1", low_memory=False, dtype=str)
    except UnicodeDecodeError:
        df = pd.read_csv(caminho, sep=";", encoding="utf-8", low_memory=False, dtype=str)

    df.columns = [c.strip().upper() for c in df.columns]
    cols_upper = list(df.columns)

    col_entidade = resolver_coluna(cols_upper, MAPA_COLUNAS["co_entidade"])
    if not col_entidade:
        print("  Pulando — não é arquivo de escola (sem CO_ENTIDADE).")
        return 0

    col_ano = resolver_coluna(cols_upper, COLUNAS_ANO)
    ano_arquivo = extrair_ano_do_nome(caminho)

    if not col_ano and not ano_arquivo:
        print("  Pulando — não foi possível determinar o ano.")
        return 0

    df = df[df[col_entidade].notna() & (df[col_entidade].str.strip() != "")].copy()
    df[col_entidade] = pd.to_numeric(df[col_entidade], errors="coerce")
    df = df.dropna(subset=[col_entidade])
    df[col_entidade] = df[col_entidade].astype("Int64")

    if col_ano and col_ano in df.columns:
        df["_ano"] = pd.to_numeric(df[col_ano], errors="coerce").fillna(ano_arquivo).astype("Int64")
    else:
        df["_ano"] = ano_arquivo

    df = df.dropna(subset=["_ano"])
    df = df.drop_duplicates(subset=["_ano", col_entidade])

    total = len(df)
    if total == 0:
        print("  Nenhum registro válido.")
        return 0

    anos = sorted(df["_ano"].dropna().unique().tolist())
    print(f"  Anos: {anos} | Colunas: {len(cols_upper)} | Registros: {total:,}")

    def get_col(campo):
        col = resolver_coluna(cols_upper, MAPA_COLUNAS[campo])
        return col

    col_no_entidade               = get_col("no_entidade")
    col_co_municipio              = get_col("co_municipio")
    col_no_municipio              = get_col("no_municipio")
    col_sg_uf                     = get_col("sg_uf")
    col_tp_dependencia            = get_col("tp_dependencia")
    col_tp_localizacao            = get_col("tp_localizacao")
    col_tp_situacao               = get_col("tp_situacao_funcionamento")

    inseridos = 0
    with engine.begin() as conn:
        for i in range(0, total, TAMANHO_LOTE):
            lote = df.iloc[i: i + TAMANHO_LOTE]
            registros = []

            for _, row in lote.iterrows():
                dados = {k: v for k, v in row.items() if k != "_ano"}
                dados_json_str = row_para_json(pd.Series(dados))

                def val_int(col):
                    if col and col in row and pd.notna(row[col]):
                        try:
                            return int(float(row[col]))
                        except Exception:
                            return None
                    return None

                def val_str(col):
                    if col and col in row and pd.notna(row[col]):
                        v = str(row[col]).strip()
                        return v if v else None
                    return None

                registros.append({
                    "ano":                        int(row["_ano"]),
                    "co_entidade":                int(row[col_entidade]),
                    "no_entidade":                val_str(col_no_entidade),
                    "co_municipio":               val_int(col_co_municipio),
                    "no_municipio":               val_str(col_no_municipio),
                    "sg_uf":                      val_str(col_sg_uf),
                    "tp_dependencia":             val_int(col_tp_dependencia),
                    "tp_localizacao":             val_int(col_tp_localizacao),
                    "tp_situacao_funcionamento":  val_int(col_tp_situacao),
                    "dados_json":                 dados_json_str,
                })

            conn.execute(
                text("""
                    INSERT INTO censo_escola_historico (
                        ano, co_entidade, no_entidade, co_municipio, no_municipio,
                        sg_uf, tp_dependencia, tp_localizacao, tp_situacao_funcionamento,
                        dados_json
                    ) VALUES (
                        :ano, :co_entidade, :no_entidade, :co_municipio, :no_municipio,
                        :sg_uf, :tp_dependencia, :tp_localizacao, :tp_situacao_funcionamento,
                        :dados_json::jsonb
                    )
                    ON CONFLICT (ano, co_entidade) DO UPDATE SET
                        no_entidade               = EXCLUDED.no_entidade,
                        co_municipio              = EXCLUDED.co_municipio,
                        no_municipio              = EXCLUDED.no_municipio,
                        sg_uf                     = EXCLUDED.sg_uf,
                        tp_dependencia            = EXCLUDED.tp_dependencia,
                        tp_localizacao            = EXCLUDED.tp_localizacao,
                        tp_situacao_funcionamento = EXCLUDED.tp_situacao_funcionamento,
                        dados_json                = EXCLUDED.dados_json
                """),
                registros,
            )

            inseridos += len(registros)
            print(f"  Progresso: {inseridos:,}/{total:,}", end="\r")

    print(f"\n  ✓ {inseridos:,} registros salvos de {caminho.name}")
    return inseridos


def main():
    if not PASTA.exists():
        print(f"Pasta não encontrada: {PASTA}")
        print("Crie a pasta 'extracted/' e coloque os CSVs do censo lá.")
        sys.exit(1)

    arquivos_csv = sorted(list(PASTA.rglob("*.csv")) + list(PASTA.rglob("*.CSV")))
    print(f"CSVs encontrados em '{PASTA}': {len(arquivos_csv)}")

    if not arquivos_csv:
        print("\nNenhum CSV encontrado.")
        print("Baixe os microdados do censo em: https://www.gov.br/inep/pt-br/acesso-a-informacao/dados-abertos/microdados/censo-escolar")
        print("Extraia os ZIPs e coloque os CSVs dentro de 'extracted/'")
        sys.exit(0)

    total_geral = 0
    for caminho in arquivos_csv:
        total_geral += processar_arquivo(caminho)

    print(f"\n{'='*60}")
    print(f"Importação finalizada. Total: {total_geral:,} registros processados.")
    print(f"{'='*60}")


if __name__ == "__main__":
    main()
