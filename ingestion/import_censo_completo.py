"""
Importação completa do Censo Escolar 2020+ (todos os arquivos do MEC).

Detecta automaticamente o tipo de cada arquivo pelo nome e importa
para a tabela correta no banco de dados:

  Tabela_Escola_*.csv         → censo_escola_historico
  microdados_ed_basica_*.csv  → censo_escola_historico
  Tabela_Matricula_*.csv      → matricula_escola
  Tabela_Docente_*.csv        → docente_escola
  Tabela_Turma_*.csv          → turma_escola
  Tabela_Gestor_Escolar_*.csv → gestor_escolar

Como usar:
  Coloque todos os arquivos CSV dentro de extracted/ (qualquer subpasta).
  Execute:
    python ingestion/import_censo_completo.py
  
  Ou apontando para o banco externo:
    DATABASE_URL="postgresql://..." python ingestion/import_censo_completo.py
"""

import json
import re
import sys
from pathlib import Path

import numpy as np
import pandas as pd
from psycopg2.extras import execute_values
from sqlalchemy import create_engine, text

# Tenta usar db_config se disponível; caso contrário usa DATABASE_URL diretamente
try:
    from db_config import get_sqlalchemy_engine
    engine = get_sqlalchemy_engine()
except ModuleNotFoundError:
    import os
    _url = os.environ.get("DATABASE_URL") or os.environ.get("DATABASE_PUBLIC_URL")
    if not _url:
        print("ERRO: defina DATABASE_URL antes de rodar o script.")
        sys.exit(1)
    if _url.startswith("postgres://"):
        _url = _url.replace("postgres://", "postgresql://", 1)
    engine = create_engine(_url, pool_pre_ping=True)

BASE_DIR = Path(__file__).resolve().parent.parent
PASTA = BASE_DIR / "extracted"

TAMANHO_LOTE = 10_000

# Mapeamentos de colunas estruturadas (usadas em censo_escola_historico)
MAPA_COLUNAS_ESCOLA = {
    "co_entidade":               ["CO_ENTIDADE", "CO_ESCOLA", "ID_ESCOLA"],
    "no_entidade":               ["NO_ENTIDADE", "NO_ESCOLA"],
    "co_municipio":              ["CO_MUNICIPIO", "CO_MUN"],
    "no_municipio":              ["NO_MUNICIPIO", "NO_MUN"],
    "sg_uf":                     ["SG_UF", "SG_UF_ESC"],
    "tp_dependencia":            ["TP_DEPENDENCIA", "TP_DEP_ADM_ESCOLA"],
    "tp_localizacao":            ["TP_LOCALIZACAO", "TP_LOC_ESCOLA"],
    "tp_situacao_funcionamento": ["TP_SITUACAO_FUNCIONAMENTO"],
    # Endereço
    "ds_endereco":               ["DS_ENDERECO", "DS_END_ESCOLA", "LOGRADOURO"],
    "no_bairro":                 ["NO_BAIRRO", "DS_BAIRRO", "BAIRRO"],
    "co_cep":                    ["CO_CEP", "NU_CEP", "CEP"],
    "nu_ddd":                    ["NU_DDD", "DDD"],
    "nu_telefone":               ["NU_TELEFONE", "TELEFONE"],
}

COLUNAS_ANO = ["NU_ANO_CENSO", "AN_CENSO", "NU_ANO", "ANO_CENSO"]


# ---------------------------------------------------------------------------
# Utilitários
# ---------------------------------------------------------------------------

def extrair_ano_do_nome(caminho: Path) -> int | None:
    for parte in [caminho.name] + list(caminho.parts):
        m = re.search(r"(20\d{2}|199\d|200[0-9])", parte)
        if m:
            return int(m.group())
    return None


def resolver_coluna(cols: list[str], candidatos: list[str]) -> str | None:
    cols_up = [c.upper() for c in cols]
    for c in candidatos:
        if c.upper() in cols_up:
            return c.upper()
    return None


def limpar_valor(v):
    if v is None:
        return None
    if isinstance(v, float) and (np.isnan(v) or np.isinf(v)):
        return None
    if isinstance(v, np.integer):
        return int(v)
    if isinstance(v, np.floating):
        return float(v)
    return v


def row_para_json(row: pd.Series) -> str:
    return json.dumps(
        {k: limpar_valor(v) for k, v in row.items()},
        ensure_ascii=False,
        default=str,
    )


def df_para_json_bulk(df: pd.DataFrame, excluir: list) -> list:
    """Converte todas as linhas do df para lista de JSON strings de forma vetorizada."""
    cols_drop = [c for c in excluir if c in df.columns]
    records = df.drop(columns=cols_drop).to_dict(orient="records")
    return [
        json.dumps({k: limpar_valor(v) for k, v in r.items()}, ensure_ascii=False, default=str)
        for r in records
    ]


def _raw_cursor():
    """Retorna (raw_conn, cursor) psycopg2 a partir do engine SQLAlchemy."""
    raw = engine.raw_connection()
    return raw, raw.cursor()


def val_int(row, col):
    if col and col in row and pd.notna(row[col]):
        try:
            return int(float(str(row[col])))
        except Exception:
            return None
    return None


def val_str(row, col):
    if col and col in row and pd.notna(row[col]):
        v = str(row[col]).strip()
        return v or None
    return None


def ler_csv(caminho: Path) -> pd.DataFrame | None:
    try:
        df = pd.read_csv(caminho, sep=";", encoding="latin1", low_memory=False, dtype=str)
    except UnicodeDecodeError:
        try:
            df = pd.read_csv(caminho, sep=";", encoding="utf-8", low_memory=False, dtype=str)
        except Exception as e:
            print(f"  Erro ao ler {caminho.name}: {e}")
            return None
    except Exception as e:
        print(f"  Erro ao ler {caminho.name}: {e}")
        return None

    df.columns = [c.strip().upper() for c in df.columns]
    return df


def detectar_tipo(caminho: Path) -> str:
    """Retorna o tipo de arquivo: escola | matricula | docente | turma | gestor | desconhecido"""
    nome = caminho.name.lower()
    if "tabela_escola" in nome or "microdados_ed_basica" in nome:
        return "escola"
    if "matricula" in nome:
        return "matricula"
    if "docente" in nome:
        return "docente"
    if "turma" in nome:
        return "turma"
    if "gestor" in nome:
        return "gestor"
    return "desconhecido"


def preparar_df(df: pd.DataFrame, caminho: Path):
    """Filtra, detecta ano, e adiciona coluna _ano. Retorna (df, col_entidade) ou (None, None)."""
    cols = list(df.columns)
    col_entidade = resolver_coluna(cols, ["CO_ENTIDADE", "CO_ESCOLA", "ID_ESCOLA"])
    if not col_entidade:
        print("  Pulando — sem coluna CO_ENTIDADE.")
        return None, None

    col_ano = resolver_coluna(cols, COLUNAS_ANO)
    ano_arquivo = extrair_ano_do_nome(caminho)

    if not col_ano and not ano_arquivo:
        print("  Pulando — não foi possível determinar o ano.")
        return None, None

    df = df[df[col_entidade].notna() & (df[col_entidade].astype(str).str.strip() != "")].copy()
    df[col_entidade] = pd.to_numeric(df[col_entidade], errors="coerce")
    df = df.dropna(subset=[col_entidade])
    df[col_entidade] = df[col_entidade].astype("Int64")

    if col_ano and col_ano in df.columns:
        df["_ano"] = pd.to_numeric(df[col_ano], errors="coerce").fillna(ano_arquivo).astype("Int64")
    else:
        df["_ano"] = ano_arquivo

    df = df.dropna(subset=["_ano"])
    return df, col_entidade


# ---------------------------------------------------------------------------
# Importadores por tipo
# ---------------------------------------------------------------------------

def importar_escola(df: pd.DataFrame, caminho: Path) -> int:
    """Importa para censo_escola_historico com todos os campos estruturados + dados_json."""
    df, col_entidade = preparar_df(df, caminho)
    if df is None:
        return 0

    df = df.drop_duplicates(subset=["_ano", col_entidade])
    total = len(df)
    cols = list(df.columns)

    def gc(campo):
        return resolver_coluna(cols, MAPA_COLUNAS_ESCOLA[campo])

    col_no   = gc("no_entidade")
    col_mun  = gc("co_municipio")
    col_nom  = gc("no_municipio")
    col_uf   = gc("sg_uf")
    col_dep  = gc("tp_dependencia")
    col_loc  = gc("tp_localizacao")
    col_sit  = gc("tp_situacao_funcionamento")
    col_end  = gc("ds_endereco")
    col_bai  = gc("no_bairro")
    col_cep  = gc("co_cep")
    col_ddd  = gc("nu_ddd")
    col_tel  = gc("nu_telefone")

    # Constrói JSON de todas as linhas de uma vez (muito mais rápido que iterrows)
    jsons = df_para_json_bulk(df, excluir=["_ano"])

    SQL_ESCOLA = """
        INSERT INTO censo_escola_historico (
            ano, co_entidade, no_entidade, co_municipio, no_municipio,
            sg_uf, tp_dependencia, tp_localizacao, tp_situacao_funcionamento,
            ds_endereco, no_bairro, co_cep, nu_ddd, nu_telefone, dados_json
        ) VALUES %s
        ON CONFLICT (ano, co_entidade) DO UPDATE SET
            no_entidade               = EXCLUDED.no_entidade,
            co_municipio              = EXCLUDED.co_municipio,
            no_municipio              = EXCLUDED.no_municipio,
            sg_uf                     = EXCLUDED.sg_uf,
            tp_dependencia            = EXCLUDED.tp_dependencia,
            tp_localizacao            = EXCLUDED.tp_localizacao,
            tp_situacao_funcionamento = EXCLUDED.tp_situacao_funcionamento,
            ds_endereco               = EXCLUDED.ds_endereco,
            no_bairro                 = EXCLUDED.no_bairro,
            co_cep                    = EXCLUDED.co_cep,
            nu_ddd                    = EXCLUDED.nu_ddd,
            nu_telefone               = EXCLUDED.nu_telefone,
            dados_json                = EXCLUDED.dados_json
    """

    data = [
        (
            int(row["_ano"]), int(row[col_entidade]),
            val_str(row, col_no), val_int(row, col_mun), val_str(row, col_nom),
            val_str(row, col_uf), val_int(row, col_dep), val_int(row, col_loc),
            val_int(row, col_sit), val_str(row, col_end), val_str(row, col_bai),
            val_str(row, col_cep), val_str(row, col_ddd), val_str(row, col_tel),
            jsons[i],
        )
        for i, (_, row) in enumerate(df.iterrows())
    ]

    raw_conn, cursor = _raw_cursor()
    try:
        for i in range(0, total, TAMANHO_LOTE):
            execute_values(cursor, SQL_ESCOLA, data[i: i + TAMANHO_LOTE],
                           template="(%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s::jsonb)",
                           page_size=TAMANHO_LOTE)
            inseridos = min(i + TAMANHO_LOTE, total)
            print(f"  Progresso: {inseridos:,}/{total:,}", end="\r")
        raw_conn.commit()
    finally:
        cursor.close()
        raw_conn.close()

    return total


def importar_upsert(df: pd.DataFrame, caminho: Path, tabela: str) -> int:
    """Importa para tabelas com PK (ano, co_entidade) — matricula e gestor."""
    df, col_entidade = preparar_df(df, caminho)
    if df is None:
        return 0

    df = df.drop_duplicates(subset=["_ano", col_entidade])
    total = len(df)
    jsons = df_para_json_bulk(df, excluir=["_ano"])

    data = [
        (int(row["_ano"]), int(row[col_entidade]), jsons[i])
        for i, (_, row) in enumerate(df.iterrows())
    ]

    sql = f"""
        INSERT INTO {tabela} (ano, co_entidade, dados_json) VALUES %s
        ON CONFLICT (ano, co_entidade) DO UPDATE SET dados_json = EXCLUDED.dados_json
    """

    raw_conn, cursor = _raw_cursor()
    try:
        for i in range(0, total, TAMANHO_LOTE):
            execute_values(cursor, sql, data[i: i + TAMANHO_LOTE],
                           template="(%s,%s,%s::jsonb)", page_size=TAMANHO_LOTE)
            print(f"  Progresso: {min(i + TAMANHO_LOTE, total):,}/{total:,}", end="\r")
        raw_conn.commit()
    finally:
        cursor.close()
        raw_conn.close()

    return total


def importar_multiplos(df: pd.DataFrame, caminho: Path, tabela: str) -> int:
    """Importa para tabelas com múltiplos registros por escola — docente e turma.
    Apaga os dados existentes do ano antes de reinserir (evita duplicatas em reimport)."""
    df, col_entidade = preparar_df(df, caminho)
    if df is None:
        return 0

    total = len(df)
    anos = df["_ano"].dropna().unique().tolist()
    jsons = df_para_json_bulk(df, excluir=["_ano"])

    data = [
        (int(row["_ano"]), int(row[col_entidade]), jsons[i])
        for i, (_, row) in enumerate(df.iterrows())
    ]

    sql = f"INSERT INTO {tabela} (ano, co_entidade, dados_json) VALUES %s"

    raw_conn, cursor = _raw_cursor()
    try:
        for ano in anos:
            cursor.execute(f"DELETE FROM {tabela} WHERE ano = %s", (int(ano),))
        for i in range(0, total, TAMANHO_LOTE):
            execute_values(cursor, sql, data[i: i + TAMANHO_LOTE],
                           template="(%s,%s,%s::jsonb)", page_size=TAMANHO_LOTE)
            print(f"  Progresso: {min(i + TAMANHO_LOTE, total):,}/{total:,}", end="\r")
        raw_conn.commit()
    finally:
        cursor.close()
        raw_conn.close()

    return total


# ---------------------------------------------------------------------------
# Processamento principal
# ---------------------------------------------------------------------------

ROTEADOR = {
    "escola":    ("censo_escola_historico", importar_escola),
    "matricula": ("matricula_escola",       importar_upsert),
    "gestor":    ("gestor_escolar",         importar_upsert),
    "docente":   ("docente_escola",         importar_multiplos),
    "turma":     ("turma_escola",           importar_multiplos),
}


def processar_arquivo(caminho: Path) -> int:
    tipo = detectar_tipo(caminho)

    if tipo == "desconhecido":
        print(f"\n→ {caminho.name} — tipo desconhecido, pulando.")
        return 0

    tabela, fn_importar = ROTEADOR[tipo]
    print(f"\n→ {caminho.name}")
    print(f"  Tipo: {tipo} | Tabela destino: {tabela}")

    df = ler_csv(caminho)
    if df is None or df.empty:
        return 0

    anos_str = ""
    col_ano = resolver_coluna(list(df.columns), COLUNAS_ANO)
    ano_arquivo = extrair_ano_do_nome(caminho)
    if col_ano:
        anos_uniq = sorted(df[col_ano].dropna().unique().tolist())[:5]
        anos_str = f"Anos no arquivo: {anos_uniq} | "
    else:
        anos_str = f"Ano pelo nome: {ano_arquivo} | "

    print(f"  {anos_str}Colunas: {len(df.columns)} | Linhas: {len(df):,}")

    if tipo in ("escola",):
        n = fn_importar(df, caminho)
    else:
        n = fn_importar(df, caminho, tabela)

    print(f"\n  ✓ {n:,} registros salvos em '{tabela}'")
    return n


def main():
    if not PASTA.exists():
        print(f"Pasta não encontrada: {PASTA}")
        sys.exit(1)

    extensoes = ["*.csv", "*.CSV"]
    arquivos = []
    for ext in extensoes:
        arquivos.extend(PASTA.rglob(ext))
    arquivos = sorted(set(arquivos))

    if not arquivos:
        print(f"Nenhum CSV encontrado em '{PASTA}'.")
        sys.exit(0)

    print(f"Arquivos encontrados: {len(arquivos)}")
    for a in arquivos:
        tipo = detectar_tipo(a)
        tabela = ROTEADOR.get(tipo, ("?", None))[0]
        print(f"  [{tipo:10s} → {tabela}] {a.name}")

    print()
    total_geral = 0
    for caminho in arquivos:
        total_geral += processar_arquivo(caminho)

    print(f"\n{'='*60}")
    print(f"Importação finalizada. Total: {total_geral:,} registros.")
    print(f"{'='*60}")


if __name__ == "__main__":
    main()
