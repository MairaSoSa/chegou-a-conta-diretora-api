import pandas as pd
from pathlib import Path
from sqlalchemy import text

from db_config import get_sqlalchemy_engine

engine = get_sqlalchemy_engine()

base_dir = Path(__file__).resolve().parent.parent
pasta = base_dir / "extracted"

print("Pasta analisada:", pasta)

arquivos_csv = list(pasta.rglob("*.csv")) + list(pasta.rglob("*.CSV"))
print("CSV encontrados:", len(arquivos_csv))

arquivos_alvo = []

for caminho in arquivos_csv:
    nome = caminho.name.lower()

    if (
        "microdados_ed_basica_2020" in nome
        or "microdados_ed_basica_2021" in nome
        or "microdados_ed_basica_2023" in nome
        or "microdados_ed_basica_2024" in nome
        or "tabela_escola_2025" in nome
    ):
        arquivos_alvo.append(caminho)

print("Arquivos do censo selecionados:")
for a in arquivos_alvo:
    print("-", a)

for caminho in arquivos_alvo:
    print(f"\nLendo: {caminho}")

    df = pd.read_csv(
        caminho,
        sep=";",
        encoding="latin1",
        low_memory=False
    )

    print("Primeiras colunas:", df.columns.tolist()[:15])

    colunas = [
        "CO_ENTIDADE",
        "NO_ENTIDADE",
        "CO_MUNICIPIO",
        "NO_MUNICIPIO",
        "SG_UF",
        "TP_DEPENDENCIA",
        "TP_LOCALIZACAO",
        "TP_SITUACAO_FUNCIONAMENTO"
    ]

    faltando = [c for c in colunas if c not in df.columns]
    if faltando:
        print("Pulando arquivo; faltam colunas:", faltando)
        continue

    df = df[colunas].copy()

    df.columns = [
        "co_entidade",
        "no_entidade",
        "co_municipio",
        "no_municipio",
        "sg_uf",
        "tp_dependencia",
        "tp_localizacao",
        "tp_situacao_funcionamento"
    ]

    df.drop_duplicates(subset=["co_entidade"], inplace=True)
    df = df[df["co_entidade"].notna()]

    with engine.begin() as conn:
        for _, row in df.iterrows():
            conn.execute(text("""
                INSERT INTO escolas (
                    co_entidade,
                    no_entidade,
                    co_municipio,
                    no_municipio,
                    sg_uf,
                    tp_dependencia,
                    tp_localizacao,
                    tp_situacao_funcionamento
                ) VALUES (
                    :co_entidade,
                    :no_entidade,
                    :co_municipio,
                    :no_municipio,
                    :sg_uf,
                    :tp_dependencia,
                    :tp_localizacao,
                    :tp_situacao_funcionamento
                )
                ON CONFLICT (co_entidade) DO NOTHING
            """), {
                "co_entidade": int(row["co_entidade"]) if pd.notna(row["co_entidade"]) else None,
                "no_entidade": row["no_entidade"],
                "co_municipio": int(row["co_municipio"]) if pd.notna(row["co_municipio"]) else None,
                "no_municipio": row["no_municipio"],
                "sg_uf": row["sg_uf"],
                "tp_dependencia": int(row["tp_dependencia"]) if pd.notna(row["tp_dependencia"]) else None,
                "tp_localizacao": int(row["tp_localizacao"]) if pd.notna(row["tp_localizacao"]) else None,
                "tp_situacao_funcionamento": int(row["tp_situacao_funcionamento"]) if pd.notna(row["tp_situacao_funcionamento"]) else None
            })

    print("Importado:", len(df), "escolas de", caminho.name)

print("\nFim da importação.")
