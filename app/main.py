import os
from fastapi import FastAPI
from sqlalchemy import create_engine, text

app = FastAPI(
    title="Chegou a Conta Diretora API",
    description="""
API pública com dados educacionais de escolas brasileiras.

Inclui:
- Indicadores por escola
- IDEB
- INSE
- Taxas de rendimento
- Comparações com município, estado e Brasil

Base de dados construída a partir de microdados públicos do INEP.
""",
    version="1.0.0",
    docs_url="/documentacao",
    redoc_url="/guia-api"
)

DATABASE_URL = os.getenv("DATABASE_URL")

if not DATABASE_URL:
    raise ValueError("DATABASE_URL não foi definida")

engine = create_engine(
    DATABASE_URL,
    pool_pre_ping=True
)

@app.get("/")
def home():
    return {
        "api": "Chegou a Conta Diretora",
        "status": "online",
        "versao": "1.0.0",
        "documentacao": "/documentacao"
    }

# conexão com banco
DATABASE_URL = os.getenv(
    "DATABASE_URL",
    "postgresql+psycopg2://localhost/educacao"
)

engine = create_engine(
    DATABASE_URL,
    pool_pre_ping=True
)


# rota inicial
@app.get("/")
def home():
    return {
        "api": "Chegou a Conta Diretora",
        "status": "online",
        "versao": "1.0.0",
        "documentacao": "/documentacao"
    }


# RAIO X DA ESCOLA
@app.get("/escolas/{co_entidade}")
def buscar_escola(co_entidade: int):
    with engine.connect() as conn:
        result = conn.execute(
            text("""
                SELECT *
                FROM vw_raiox_escola
                WHERE co_entidade = :co_entidade
            """),
            {"co_entidade": co_entidade}
        )
        row = result.mappings().first()

    if not row:
        return {
            "co_entidade": co_entidade,
            "encontrada": False,
            "mensagem": "Escola não encontrada."
        }

    return {
        "co_entidade": co_entidade,
        "encontrada": True,
        "mensagem": "Consulta realizada com sucesso.",
        "escola": dict(row)
    }


# HISTÓRICO COMPLETO
@app.get("/escolas/{co_entidade}/historico")
def buscar_historico_escola(co_entidade: int):
    with engine.connect() as conn:
        result = conn.execute(
            text("""
                SELECT *
                FROM vw_escola_indicadores
                WHERE co_entidade = :co_entidade
                ORDER BY ano
            """),
            {"co_entidade": co_entidade}
        )
        rows = result.mappings().all()

    if not rows:
        return {
            "co_entidade": co_entidade,
            "dados_disponiveis": False,
            "mensagem": "Esta escola não possui histórico de indicadores na base consultada.",
            "historico": []
        }

    return {
        "co_entidade": co_entidade,
        "dados_disponiveis": True,
        "mensagem": "Consulta realizada com sucesso.",
        "historico": [dict(row) for row in rows]
    }


# IDEB DA ESCOLA
@app.get("/escolas/{co_entidade}/ideb")
def buscar_ideb_escola(co_entidade: int):
    with engine.connect() as conn:
        result = conn.execute(
            text("""
                SELECT *
                FROM vw_escola_ideb
                WHERE co_entidade = :co_entidade
                ORDER BY ano, etapa
            """),
            {"co_entidade": co_entidade}
        )
        rows = result.mappings().all()

    if not rows:
        return {
            "co_entidade": co_entidade,
            "dados_disponiveis": False,
            "tem_ideb_divulgado": False,
            "mensagem": "Esta escola não possui registros de IDEB na base consultada.",
            "ideb": []
        }

    ideb_formatado = []
    tem_ideb_divulgado = False

    for row in rows:
        item = dict(row)

        if item.get("ideb") is None and item.get("nota_saeb") is None:
            item["observacao"] = (
                "Consulta realizada com sucesso, mas o IDEB não foi divulgado para este ano ou etapa."
            )
        else:
            tem_ideb_divulgado = True

        ideb_formatado.append(item)

    return {
        "co_entidade": co_entidade,
        "dados_disponiveis": True,
        "tem_ideb_divulgado": tem_ideb_divulgado,
        "mensagem": (
            "Consulta realizada com sucesso."
            if tem_ideb_divulgado
            else "Consulta realizada com sucesso, mas não há IDEB divulgado para os registros encontrados."
        ),
        "ideb": ideb_formatado
    }


# ESCOLAS DO MUNICÍPIO
@app.get("/municipios/{co_municipio}/escolas")
def buscar_escolas_municipio(co_municipio: int):
    with engine.connect() as conn:
        result = conn.execute(
            text("""
                SELECT *
                FROM vw_raiox_escola
                WHERE co_municipio = :co_municipio
                ORDER BY no_entidade
            """),
            {"co_municipio": co_municipio}
        )
        rows = result.mappings().all()

    if not rows:
        return {
            "co_municipio": co_municipio,
            "quantidade": 0,
            "mensagem": "Nenhuma escola encontrada para este município.",
            "escolas": []
        }

    return {
        "co_municipio": co_municipio,
        "quantidade": len(rows),
        "mensagem": "Consulta realizada com sucesso.",
        "escolas": [dict(row) for row in rows]
    }


# BUSCA POR NOME
@app.get("/buscar/escolas")
def buscar_escolas_por_nome(nome: str):
    with engine.connect() as conn:
        result = conn.execute(
            text("""
                SELECT *
                FROM vw_raiox_escola
                WHERE no_entidade ILIKE :nome
                ORDER BY no_entidade
                LIMIT 50
            """),
            {"nome": f"%{nome}%"}
        )
        rows = result.mappings().all()

    if not rows:
        return {
            "busca": nome,
            "quantidade": 0,
            "mensagem": "Nenhuma escola encontrada para o termo pesquisado.",
            "escolas": []
        }

    return {
        "busca": nome,
        "quantidade": len(rows),
        "mensagem": "Consulta realizada com sucesso.",
        "escolas": [dict(row) for row in rows]
    }
# ANOS DISPONÍVEIS DA ESCOLA
@app.get("/escolas/{co_entidade}/anos")
def buscar_anos_escola(co_entidade: int):
    with engine.connect() as conn:
        result = conn.execute(
            text("""
                SELECT DISTINCT ano
                FROM vw_escola_indicadores
                WHERE co_entidade = :co_entidade
                  AND ano IS NOT NULL
                ORDER BY ano
            """),
            {"co_entidade": co_entidade}
        )
        rows = result.mappings().all()

    anos = [row["ano"] for row in rows]

    if not anos:
        return {
            "co_entidade": co_entidade,
            "quantidade": 0,
            "mensagem": "Nenhum ano disponível para esta escola na base consultada.",
            "anos": []
        }

    return {
        "co_entidade": co_entidade,
        "quantidade": len(anos),
        "mensagem": "Consulta realizada com sucesso.",
        "anos": anos
    }


# RAIO-X COMPLETO DA ESCOLA
@app.get("/escolas/{co_entidade}/raio-x")
def buscar_raio_x_completo(co_entidade: int):
    with engine.connect() as conn:
        escola_result = conn.execute(
            text("""
                SELECT *
                FROM vw_raiox_escola
                WHERE co_entidade = :co_entidade
            """),
            {"co_entidade": co_entidade}
        )
        escola = escola_result.mappings().first()

        historico_result = conn.execute(
            text("""
                SELECT *
                FROM vw_escola_indicadores
                WHERE co_entidade = :co_entidade
                ORDER BY ano
            """),
            {"co_entidade": co_entidade}
        )
        historico = historico_result.mappings().all()

        ideb_result = conn.execute(
            text("""
                SELECT *
                FROM vw_escola_ideb
                WHERE co_entidade = :co_entidade
                ORDER BY ano, etapa
            """),
            {"co_entidade": co_entidade}
        )
        ideb_rows = ideb_result.mappings().all()

    if not escola:
        return {
            "co_entidade": co_entidade,
            "encontrada": False,
            "mensagem": "Escola não encontrada."
        }

    ideb_formatado = []
    tem_ideb_divulgado = False

    for row in ideb_rows:
        item = dict(row)

        if item.get("ideb") is None and item.get("nota_saeb") is None:
            item["observacao"] = (
                "Consulta realizada com sucesso, mas o IDEB não foi divulgado para este ano ou etapa."
            )
        else:
            tem_ideb_divulgado = True

        ideb_formatado.append(item)

    anos_historico = sorted(
        list({
            row["ano"]
            for row in historico
            if row.get("ano") is not None
        })
    )

    anos_ideb = sorted(
        list({
            row["ano"]
            for row in ideb_rows
            if row.get("ano") is not None
        })
    )

    anos_disponiveis = sorted(list(set(anos_historico + anos_ideb)))

    return {
        "co_entidade": co_entidade,
        "encontrada": True,
        "mensagem": "Consulta realizada com sucesso.",
        "anos_disponiveis": anos_disponiveis,
        "tem_ideb_divulgado": tem_ideb_divulgado,
        "escola": dict(escola),
        "historico": [dict(row) for row in historico],
        "ideb": ideb_formatado
    }


# PAINEL DA ESCOLA
@app.get("/escolas/{co_entidade}/painel")
def buscar_painel_escola(co_entidade: int):
    with engine.connect() as conn:
        escola_result = conn.execute(
            text("""
                SELECT *
                FROM vw_raiox_escola
                WHERE co_entidade = :co_entidade
            """),
            {"co_entidade": co_entidade}
        )
        escola = escola_result.mappings().first()

        ideb_result = conn.execute(
            text("""
                SELECT *
                FROM vw_escola_ideb
                WHERE co_entidade = :co_entidade
                ORDER BY ano DESC, etapa
            """),
            {"co_entidade": co_entidade}
        )
        ideb_rows = ideb_result.mappings().all()

        historico_result = conn.execute(
            text("""
                SELECT *
                FROM vw_escola_indicadores
                WHERE co_entidade = :co_entidade
                ORDER BY ano DESC
            """),
            {"co_entidade": co_entidade}
        )
        historico_rows = historico_result.mappings().all()

    if not escola:
        return {
            "co_entidade": co_entidade,
            "encontrada": False,
            "mensagem": "Escola não encontrada."
        }

    ultimo_historico = dict(historico_rows[0]) if historico_rows else None

    ideb_formatado = []
    tem_ideb_divulgado = False

    for row in ideb_rows:
        item = dict(row)

        if item.get("ideb") is None and item.get("nota_saeb") is None:
            item["observacao"] = "IDEB não divulgado para este ano ou etapa."
        else:
            tem_ideb_divulgado = True

        ideb_formatado.append(item)

    ultimo_ideb_valido = None
    for item in ideb_formatado:
        if item.get("ideb") is not None or item.get("nota_saeb") is not None:
            ultimo_ideb_valido = item
            break

    anos_disponiveis = sorted(
        list({
            row["ano"]
            for row in historico_rows
            if row.get("ano") is not None
        } | {
            row["ano"]
            for row in ideb_rows
            if row.get("ano") is not None
        })
    )

    return {
        "co_entidade": co_entidade,
        "encontrada": True,
        "mensagem": "Consulta realizada com sucesso.",
        "anos_disponiveis": anos_disponiveis,
        "resumo": {
            "tem_ideb_divulgado": tem_ideb_divulgado,
            "ultimo_historico": ultimo_historico,
            "ultimo_ideb": ultimo_ideb_valido
        },
        "escola": dict(escola),
        "historico": [dict(row) for row in historico_rows],
        "ideb": ideb_formatado
    }
# RESUMO DA ESCOLA
@app.get("/escolas/{co_entidade}/resumo")
def buscar_resumo_escola(co_entidade: int):
    with engine.connect() as conn:
        escola_result = conn.execute(
            text("""
                SELECT *
                FROM vw_raiox_escola
                WHERE co_entidade = :co_entidade
            """),
            {"co_entidade": co_entidade}
        )
        escola = escola_result.mappings().first()

        historico_result = conn.execute(
            text("""
                SELECT *
                FROM vw_escola_indicadores
                WHERE co_entidade = :co_entidade
                ORDER BY ano DESC
            """),
            {"co_entidade": co_entidade}
        )
        historico_rows = historico_result.mappings().all()

        ideb_result = conn.execute(
            text("""
                SELECT *
                FROM vw_escola_ideb
                WHERE co_entidade = :co_entidade
                ORDER BY ano DESC, etapa
            """),
            {"co_entidade": co_entidade}
        )
        ideb_rows = ideb_result.mappings().all()

    if not escola:
        return {
            "co_entidade": co_entidade,
            "encontrada": False,
            "mensagem": "Escola não encontrada."
        }

    ultimo_historico = dict(historico_rows[0]) if historico_rows else None

    ultimo_ideb_valido = None
    tem_ideb_divulgado = False
    for row in ideb_rows:
        item = dict(row)
        if item.get("ideb") is not None or item.get("nota_saeb") is not None:
            ultimo_ideb_valido = item
            tem_ideb_divulgado = True
            break

    anos_disponiveis = sorted(
        list({
            row["ano"] for row in historico_rows if row.get("ano") is not None
        } | {
            row["ano"] for row in ideb_rows if row.get("ano") is not None
        })
    )

    return {
        "co_entidade": co_entidade,
        "encontrada": True,
        "mensagem": "Consulta realizada com sucesso.",
        "anos_disponiveis": anos_disponiveis,
        "resumo": {
            "escola": dict(escola),
            "ultimo_historico": ultimo_historico,
            "ultimo_ideb": ultimo_ideb_valido,
            "tem_ideb_divulgado": tem_ideb_divulgado
        }
    }


# SÉRIES HISTÓRICAS DA ESCOLA
@app.get("/escolas/{co_entidade}/series-historicas")
def buscar_series_historicas_escola(co_entidade: int):
    with engine.connect() as conn:
        historico_result = conn.execute(
            text("""
                SELECT *
                FROM vw_escola_indicadores
                WHERE co_entidade = :co_entidade
                ORDER BY ano
            """),
            {"co_entidade": co_entidade}
        )
        historico_rows = historico_result.mappings().all()

        ideb_result = conn.execute(
            text("""
                SELECT *
                FROM vw_escola_ideb
                WHERE co_entidade = :co_entidade
                ORDER BY ano, etapa
            """),
            {"co_entidade": co_entidade}
        )
        ideb_rows = ideb_result.mappings().all()

    if not historico_rows and not ideb_rows:
        return {
            "co_entidade": co_entidade,
            "dados_disponiveis": False,
            "mensagem": "Esta escola não possui séries históricas na base consultada.",
            "historico": [],
            "ideb": []
        }

    ideb_formatado = []
    for row in ideb_rows:
        item = dict(row)
        if item.get("ideb") is None and item.get("nota_saeb") is None:
            item["observacao"] = "IDEB não divulgado para este ano ou etapa."
        ideb_formatado.append(item)

    return {
        "co_entidade": co_entidade,
        "dados_disponiveis": True,
        "mensagem": "Consulta realizada com sucesso.",
        "historico": [dict(row) for row in historico_rows],
        "ideb": ideb_formatado
    }


# ÚLTIMOS INDICADORES DA ESCOLA
@app.get("/escolas/{co_entidade}/indicadores/ultimo")
def buscar_ultimo_indicador_escola(co_entidade: int):
    with engine.connect() as conn:
        historico_result = conn.execute(
            text("""
                SELECT *
                FROM vw_escola_indicadores
                WHERE co_entidade = :co_entidade
                ORDER BY ano DESC
                LIMIT 1
            """),
            {"co_entidade": co_entidade}
        )
        ultimo_historico = historico_result.mappings().first()

        ideb_result = conn.execute(
            text("""
                SELECT *
                FROM vw_escola_ideb
                WHERE co_entidade = :co_entidade
                ORDER BY ano DESC, etapa
            """),
            {"co_entidade": co_entidade}
        )
        ideb_rows = ideb_result.mappings().all()

    ultimo_ideb = None
    tem_ideb_divulgado = False

    for row in ideb_rows:
        item = dict(row)
        if item.get("ideb") is not None or item.get("nota_saeb") is not None:
            ultimo_ideb = item
            tem_ideb_divulgado = True
            break

    if not ultimo_historico and not ultimo_ideb:
        return {
            "co_entidade": co_entidade,
            "dados_disponiveis": False,
            "mensagem": "Esta escola não possui indicadores recentes na base consultada."
        }

    return {
        "co_entidade": co_entidade,
        "dados_disponiveis": True,
        "mensagem": "Consulta realizada com sucesso.",
        "ultimo_historico": dict(ultimo_historico) if ultimo_historico else None,
        "ultimo_ideb": ultimo_ideb,
        "tem_ideb_divulgado": tem_ideb_divulgado
    }


# COMPARATIVO DA ESCOLA POR ANO
@app.get("/escolas/{co_entidade}/comparativo")
def buscar_comparativo_escola(co_entidade: int):
    with engine.connect() as conn:
        result = conn.execute(
            text("""
                SELECT *
                FROM vw_escola_indicadores
                WHERE co_entidade = :co_entidade
                ORDER BY ano
            """),
            {"co_entidade": co_entidade}
        )
        rows = result.mappings().all()

    if not rows:
        return {
            "co_entidade": co_entidade,
            "dados_disponiveis": False,
            "mensagem": "Esta escola não possui dados comparativos na base consultada.",
            "comparativo": []
        }

    return {
        "co_entidade": co_entidade,
        "dados_disponiveis": True,
        "mensagem": "Consulta realizada com sucesso.",
        "comparativo": [dict(row) for row in rows]
    }
# LISTAR UFs
@app.get("/ufs")
def listar_ufs():
    with engine.connect() as conn:
        result = conn.execute(
            text("""
                SELECT *
                FROM vw_ufs
                ORDER BY uf
            """)
        )
        rows = result.mappings().all()

    return {
        "quantidade": len(rows),
        "mensagem": "Consulta realizada com sucesso.",
        "ufs": [dict(row) for row in rows]
    }


# LISTAR MUNICÍPIOS POR UF
@app.get("/ufs/{uf}/municipios")
def listar_municipios_por_uf(uf: str):
    with engine.connect() as conn:
        result = conn.execute(
            text("""
                SELECT *
                FROM vw_municipios
                WHERE uf = :uf
                ORDER BY no_municipio
            """),
            {"uf": uf.upper()}
        )
        rows = result.mappings().all()

    if not rows:
        return {
            "uf": uf.upper(),
            "quantidade": 0,
            "mensagem": "Nenhum município encontrado para esta UF.",
            "municipios": []
        }

    return {
        "uf": uf.upper(),
        "quantidade": len(rows),
        "mensagem": "Consulta realizada com sucesso.",
        "municipios": [dict(row) for row in rows]
    }
# ALERTAS DA ESCOLA
@app.get("/escolas/{co_entidade}/alertas")
def buscar_alertas_escola(co_entidade: int):
    with engine.connect() as conn:
        escola_result = conn.execute(
            text("""
                SELECT *
                FROM vw_raiox_escola
                WHERE co_entidade = :co_entidade
            """),
            {"co_entidade": co_entidade}
        )
        escola = escola_result.mappings().first()

        historico_result = conn.execute(
            text("""
                SELECT *
                FROM vw_escola_indicadores
                WHERE co_entidade = :co_entidade
                ORDER BY ano DESC
            """),
            {"co_entidade": co_entidade}
        )
        historico_rows = historico_result.mappings().all()

        ideb_result = conn.execute(
            text("""
                SELECT *
                FROM vw_escola_ideb
                WHERE co_entidade = :co_entidade
                ORDER BY ano DESC, etapa
            """),
            {"co_entidade": co_entidade}
        )
        ideb_rows = ideb_result.mappings().all()

    if not escola:
        return {
            "co_entidade": co_entidade,
            "encontrada": False,
            "mensagem": "Escola não encontrada.",
            "alertas": []
        }

    escola_dict = dict(escola)
    alertas = []

    if not historico_rows:
        alertas.append({
            "nivel": "atencao",
            "codigo": "sem_historico",
            "mensagem": "A escola não possui histórico de indicadores na base consultada."
        })

    if not ideb_rows:
        alertas.append({
            "nivel": "atencao",
            "codigo": "sem_ideb",
            "mensagem": "A escola não possui registros de IDEB na base consultada."
        })
    else:
        tem_ideb_valido = any(
            (row.get("ideb") is not None or row.get("nota_saeb") is not None)
            for row in ideb_rows
        )
        if not tem_ideb_valido:
            alertas.append({
                "nivel": "atencao",
                "codigo": "ideb_nao_divulgado",
                "mensagem": "Há registros de IDEB para a escola, mas o indicador não foi divulgado."
            })

    if not escola_dict.get("tem_inse"):
        alertas.append({
            "nivel": "informacao",
            "codigo": "sem_inse",
            "mensagem": "A escola não possui INSE disponível na base consultada."
        })

    if not escola_dict.get("tem_rendimento"):
        alertas.append({
            "nivel": "informacao",
            "codigo": "sem_rendimento",
            "mensagem": "A escola não possui indicadores de rendimento disponíveis."
        })

    if not escola_dict.get("escola_ativa"):
        alertas.append({
            "nivel": "atencao",
            "codigo": "escola_inativa",
            "mensagem": "A escola não está marcada como ativa na base consultada."
        })

    ultimo_ano_censo = escola_dict.get("ultimo_ano_censo")
    if ultimo_ano_censo is not None and ultimo_ano_censo < 2024:
        alertas.append({
            "nivel": "informacao",
            "codigo": "base_nao_recente",
            "mensagem": f"O último ano de censo disponível para esta escola é {ultimo_ano_censo}."
        })

    return {
        "co_entidade": co_entidade,
        "encontrada": True,
        "mensagem": "Consulta realizada com sucesso.",
        "quantidade_alertas": len(alertas),
        "alertas": alertas
    }


# DISPONIBILIDADE DE INDICADORES DA ESCOLA
@app.get("/escolas/{co_entidade}/disponibilidade")
def buscar_disponibilidade_escola(co_entidade: int):
    with engine.connect() as conn:
        escola_result = conn.execute(
            text("""
                SELECT *
                FROM vw_raiox_escola
                WHERE co_entidade = :co_entidade
            """),
            {"co_entidade": co_entidade}
        )
        escola = escola_result.mappings().first()

        historico_result = conn.execute(
            text("""
                SELECT DISTINCT ano
                FROM vw_escola_indicadores
                WHERE co_entidade = :co_entidade
                  AND ano IS NOT NULL
                ORDER BY ano
            """),
            {"co_entidade": co_entidade}
        )
        historico_anos = historico_result.mappings().all()

        ideb_result = conn.execute(
            text("""
                SELECT DISTINCT ano, etapa
                FROM vw_escola_ideb
                WHERE co_entidade = :co_entidade
                  AND ano IS NOT NULL
                ORDER BY ano, etapa
            """),
            {"co_entidade": co_entidade}
        )
        ideb_rows = ideb_result.mappings().all()

    if not escola:
        return {
            "co_entidade": co_entidade,
            "encontrada": False,
            "mensagem": "Escola não encontrada."
        }

    escola_dict = dict(escola)

    anos_historico = [row["ano"] for row in historico_anos]
    anos_ideb = sorted(list({row["ano"] for row in ideb_rows if row.get("ano") is not None}))
    etapas_ideb = sorted(list({row["etapa"] for row in ideb_rows if row.get("etapa") is not None}))
    anos_disponiveis = sorted(list(set(anos_historico + anos_ideb)))

    return {
        "co_entidade": co_entidade,
        "encontrada": True,
        "mensagem": "Consulta realizada com sucesso.",
        "ultimo_ano_censo": escola_dict.get("ultimo_ano_censo"),
        "anos_disponiveis": anos_disponiveis,
        "anos_historico": anos_historico,
        "anos_ideb": anos_ideb,
        "etapas_ideb": etapas_ideb,
        "disponibilidade": {
            "tem_inse": bool(escola_dict.get("tem_inse")),
            "tem_afd": bool(escola_dict.get("tem_afd")),
            "tem_icg": bool(escola_dict.get("tem_icg")),
            "tem_ied": bool(escola_dict.get("tem_ied")),
            "tem_ird": bool(escola_dict.get("tem_ird")),
            "tem_atu": bool(escola_dict.get("tem_atu")),
            "tem_had": bool(escola_dict.get("tem_had")),
            "tem_tdi": bool(escola_dict.get("tem_tdi")),
            "tem_tnr": bool(escola_dict.get("tem_tnr")),
            "tem_rendimento": bool(escola_dict.get("tem_rendimento")),
            "tem_ideb_ai": escola_dict.get("ideb_ai_recente") is not None,
            "tem_ideb_af": escola_dict.get("ideb_af_recente") is not None,
            "tem_ideb_em": escola_dict.get("ideb_em_recente") is not None
        }
    }

 # DASHBOARD COMPLETO DA ESCOLA - FORMATO PARA FRONTEND
@app.get("/dashboard/escola/{co_entidade}")
def buscar_dashboard_escola(co_entidade: int):
    with engine.connect() as conn:
        escola_result = conn.execute(
            text("""
                SELECT *
                FROM vw_raiox_escola
                WHERE co_entidade = :co_entidade
            """),
            {"co_entidade": co_entidade}
        )
        escola = escola_result.mappings().first()

        historico_result = conn.execute(
            text("""
                SELECT *
                FROM vw_escola_indicadores
                WHERE co_entidade = :co_entidade
                ORDER BY ano
            """),
            {"co_entidade": co_entidade}
        )
        historico_rows = historico_result.mappings().all()

        ideb_result = conn.execute(
            text("""
                SELECT *
                FROM vw_escola_ideb
                WHERE co_entidade = :co_entidade
                ORDER BY ano, etapa
            """),
            {"co_entidade": co_entidade}
        )
        ideb_rows = ideb_result.mappings().all()

    if not escola:
        return {
            "co_entidade": co_entidade,
            "encontrada": False,
            "mensagem": "Escola não encontrada."
        }

    escola_dict = dict(escola)
    historico = [dict(row) for row in historico_rows]

    ideb_formatado = []
    tem_ideb_divulgado = False

    for row in ideb_rows:
        item = dict(row)

        if item.get("ideb") is None and item.get("nota_saeb") is None:
            item["observacao"] = "IDEB não divulgado para este ano ou etapa."
        else:
            tem_ideb_divulgado = True

        ideb_formatado.append(item)

    anos_historico = sorted(
        list({
            row["ano"] for row in historico_rows
            if row.get("ano") is not None
        })
    )

    anos_ideb = sorted(
        list({
            row["ano"] for row in ideb_rows
            if row.get("ano") is not None
        })
    )

    anos_disponiveis = sorted(list(set(anos_historico + anos_ideb)))

    # Último histórico
    ultimo_historico = historico[-1] if historico else None

    # Último IDEB válido
    ultimo_ideb = None
    for item in sorted(ideb_formatado, key=lambda x: (x.get("ano") or 0), reverse=True):
        if item.get("ideb") is not None or item.get("nota_saeb") is not None:
            ultimo_ideb = item
            break

    # Disponibilidade
    disponibilidade = {
        "tem_inse": bool(escola_dict.get("tem_inse")),
        "tem_afd": bool(escola_dict.get("tem_afd")),
        "tem_icg": bool(escola_dict.get("tem_icg")),
        "tem_ied": bool(escola_dict.get("tem_ied")),
        "tem_ird": bool(escola_dict.get("tem_ird")),
        "tem_atu": bool(escola_dict.get("tem_atu")),
        "tem_had": bool(escola_dict.get("tem_had")),
        "tem_tdi": bool(escola_dict.get("tem_tdi")),
        "tem_tnr": bool(escola_dict.get("tem_tnr")),
        "tem_rendimento": bool(escola_dict.get("tem_rendimento")),
        "tem_ideb_ai": escola_dict.get("ideb_ai_recente") is not None,
        "tem_ideb_af": escola_dict.get("ideb_af_recente") is not None,
        "tem_ideb_em": escola_dict.get("ideb_em_recente") is not None
    }

    # Indicadores recentes
    indicadores_recentes = {
        "ultimo_ano_censo": escola_dict.get("ultimo_ano_censo"),
        "inse": {
            "valor": escola_dict.get("inse_valor"),
            "grupo": escola_dict.get("inse_grupo"),
            "disponivel": disponibilidade["tem_inse"]
        },
        "ideb": {
            "anos_iniciais": {
                "ano": escola_dict.get("ideb_ai_ano"),
                "valor": escola_dict.get("ideb_ai_recente"),
                "disponivel": disponibilidade["tem_ideb_ai"]
            },
            "anos_finais": {
                "ano": escola_dict.get("ideb_af_ano"),
                "valor": escola_dict.get("ideb_af_recente"),
                "disponivel": disponibilidade["tem_ideb_af"]
            },
            "ensino_medio": {
                "ano": escola_dict.get("ideb_em_ano"),
                "valor": escola_dict.get("ideb_em_recente"),
                "disponivel": disponibilidade["tem_ideb_em"]
            }
        },
        "ultimo_historico": ultimo_historico,
        "ultimo_ideb": ultimo_ideb
    }

    # Séries para gráficos
    series_graficos = {
        "anos_disponiveis": anos_disponiveis,
        "historico_indicadores": historico,
        "ideb": ideb_formatado,
        "ideb_por_etapa": {
            "anos_iniciais": [
                item for item in ideb_formatado
                if item.get("etapa") == "anos_iniciais"
            ],
            "anos_finais": [
                item for item in ideb_formatado
                if item.get("etapa") == "anos_finais"
            ],
            "ensino_medio": [
                item for item in ideb_formatado
                if item.get("etapa") == "ensino_medio"
            ]
        }
    }

    # Avisos
    avisos = []

    if not historico_rows:
        avisos.append({
            "nivel": "atencao",
            "codigo": "sem_historico",
            "titulo": "Sem histórico disponível",
            "mensagem": "A escola não possui histórico de indicadores na base consultada."
        })

    if not ideb_rows:
        avisos.append({
            "nivel": "atencao",
            "codigo": "sem_ideb",
            "titulo": "Sem registros de IDEB",
            "mensagem": "A escola não possui registros de IDEB na base consultada."
        })
    elif not tem_ideb_divulgado:
        avisos.append({
            "nivel": "atencao",
            "codigo": "ideb_nao_divulgado",
            "titulo": "IDEB não divulgado",
            "mensagem": "Há registros de IDEB para a escola, mas o indicador não foi divulgado."
        })

    if not disponibilidade["tem_inse"]:
        avisos.append({
            "nivel": "informacao",
            "codigo": "sem_inse",
            "titulo": "INSE indisponível",
            "mensagem": "A escola não possui INSE disponível na base consultada."
        })

    if not disponibilidade["tem_rendimento"]:
        avisos.append({
            "nivel": "informacao",
            "codigo": "sem_rendimento",
            "titulo": "Sem rendimento disponível",
            "mensagem": "A escola não possui indicadores de rendimento disponíveis."
        })

    if not escola_dict.get("escola_ativa"):
        avisos.append({
            "nivel": "atencao",
            "codigo": "escola_inativa",
            "titulo": "Escola não ativa",
            "mensagem": "A escola não está marcada como ativa na base consultada."
        })

    ultimo_ano_censo = escola_dict.get("ultimo_ano_censo")
    if ultimo_ano_censo is not None and ultimo_ano_censo < 2024:
        avisos.append({
            "nivel": "informacao",
            "codigo": "base_nao_recente",
            "titulo": "Base não é a mais recente",
            "mensagem": f"O último ano de censo disponível para esta escola é {ultimo_ano_censo}."
        })

    identificacao = {
        "co_entidade": escola_dict.get("co_entidade"),
        "no_entidade": escola_dict.get("no_entidade"),
        "co_municipio": escola_dict.get("co_municipio"),
        "no_municipio": escola_dict.get("no_municipio"),
        "sg_uf": escola_dict.get("sg_uf"),
        "tp_dependencia": escola_dict.get("tp_dependencia"),
        "tp_localizacao": escola_dict.get("tp_localizacao"),
        "tp_situacao_funcionamento": escola_dict.get("tp_situacao_funcionamento"),
        "escola_ativa": escola_dict.get("escola_ativa")
    }

    return {
        "co_entidade": co_entidade,
        "encontrada": True,
        "mensagem": "Consulta realizada com sucesso.",
        "identificacao": identificacao,
        "indicadores_recentes": indicadores_recentes,
        "series_graficos": series_graficos,
        "avisos": avisos,
        "disponibilidade": disponibilidade
    }
   
# COMPARAÇÃO DA ESCOLA COM MUNICÍPIO, UF E BRASIL
@app.get("/comparar/escola/{co_entidade}")
def comparar_escola(co_entidade: int):
    with engine.connect() as conn:
        escola = conn.execute(
            text("""
                SELECT *
                FROM vw_raiox_escola
                WHERE co_entidade = :co_entidade
            """),
            {"co_entidade": co_entidade}
        ).mappings().first()

        if not escola:
            return {
                "co_entidade": co_entidade,
                "encontrada": False,
                "mensagem": "Escola não encontrada."
            }

        municipio = escola["co_municipio"]
        uf = escola["sg_uf"]

        media_municipio = conn.execute(
            text("""
                SELECT
                    AVG(inse_valor) AS inse,
                    AVG(ideb_ai_recente) AS ideb_ai,
                    AVG(ideb_af_recente) AS ideb_af,
                    AVG(ideb_em_recente) AS ideb_em
                FROM vw_raiox_escola
                WHERE co_municipio = :municipio
            """),
            {"municipio": municipio}
        ).mappings().first()

        media_uf = conn.execute(
            text("""
                SELECT
                    AVG(inse_valor) AS inse,
                    AVG(ideb_ai_recente) AS ideb_ai,
                    AVG(ideb_af_recente) AS ideb_af,
                    AVG(ideb_em_recente) AS ideb_em
                FROM vw_raiox_escola
                WHERE sg_uf = :uf
            """),
            {"uf": uf}
        ).mappings().first()

        media_brasil = conn.execute(
            text("""
                SELECT
                    AVG(inse_valor) AS inse,
                    AVG(ideb_ai_recente) AS ideb_ai,
                    AVG(ideb_af_recente) AS ideb_af,
                    AVG(ideb_em_recente) AS ideb_em
                FROM vw_raiox_escola
            """)
        ).mappings().first()

    return {
        "co_entidade": co_entidade,
        "encontrada": True,
        "mensagem": "Consulta realizada com sucesso.",
        "escola": {
            "co_entidade": escola["co_entidade"],
            "no_entidade": escola["no_entidade"],
            "co_municipio": escola["co_municipio"],
            "no_municipio": escola["no_municipio"],
            "sg_uf": escola["sg_uf"],
            "inse": escola["inse_valor"],
            "ideb_ai": escola["ideb_ai_recente"],
            "ideb_af": escola["ideb_af_recente"],
            "ideb_em": escola["ideb_em_recente"]
        },
        "comparacao": {
            "municipio": dict(media_municipio) if media_municipio else None,
            "uf": dict(media_uf) if media_uf else None,
            "brasil": dict(media_brasil) if media_brasil else None
        }
    }

# DIAGNÓSTICO DA ESCOLA
@app.get("/diagnostico/escola/{co_entidade}")
def diagnostico_escola(co_entidade: int):
    with engine.connect() as conn:
        escola = conn.execute(
            text("""
                SELECT *
                FROM vw_raiox_escola
                WHERE co_entidade = :co_entidade
            """),
            {"co_entidade": co_entidade}
        ).mappings().first()

        if not escola:
            return {
                "co_entidade": co_entidade,
                "encontrada": False,
                "mensagem": "Escola não encontrada."
            }

        historico_rows = conn.execute(
            text("""
                SELECT *
                FROM vw_escola_indicadores
                WHERE co_entidade = :co_entidade
                ORDER BY ano DESC
            """),
            {"co_entidade": co_entidade}
        ).mappings().all()

        ideb_rows = conn.execute(
            text("""
                SELECT *
                FROM vw_escola_ideb
                WHERE co_entidade = :co_entidade
                ORDER BY ano DESC, etapa
            """),
            {"co_entidade": co_entidade}
        ).mappings().all()

        media_municipio = conn.execute(
            text("""
                SELECT
                    AVG(inse_valor) AS inse,
                    AVG(ideb_ai_recente) AS ideb_ai,
                    AVG(ideb_af_recente) AS ideb_af,
                    AVG(ideb_em_recente) AS ideb_em
                FROM vw_raiox_escola
                WHERE co_municipio = :co_municipio
            """),
            {"co_municipio": escola["co_municipio"]}
        ).mappings().first()

        media_uf = conn.execute(
            text("""
                SELECT
                    AVG(inse_valor) AS inse,
                    AVG(ideb_ai_recente) AS ideb_ai,
                    AVG(ideb_af_recente) AS ideb_af,
                    AVG(ideb_em_recente) AS ideb_em
                FROM vw_raiox_escola
                WHERE sg_uf = :sg_uf
            """),
            {"sg_uf": escola["sg_uf"]}
        ).mappings().first()

    escola_dict = dict(escola)
    ultimo_historico = dict(historico_rows[0]) if historico_rows else None

    ultimo_ideb_valido = None
    for row in ideb_rows:
        item = dict(row)
        if item.get("ideb") is not None or item.get("nota_saeb") is not None:
            ultimo_ideb_valido = item
            break

    pontos_atencao = []
    pontos_fortes = []
    leituras = []

    if not escola_dict.get("escola_ativa"):
        pontos_atencao.append("A escola não está marcada como ativa na base consultada.")

    if not escola_dict.get("tem_inse"):
        pontos_atencao.append("Não há INSE disponível para a escola.")
    else:
        if escola_dict.get("inse_valor") is not None:
            leituras.append(
                f"O INSE disponível para a escola é {escola_dict['inse_valor']}."
            )

    if not ideb_rows:
        pontos_atencao.append("A escola não possui registros de IDEB na base consultada.")
    elif not ultimo_ideb_valido:
        pontos_atencao.append("Há registros de IDEB, mas o indicador não foi divulgado.")
    else:
        etapa = ultimo_ideb_valido.get("etapa")
        valor = ultimo_ideb_valido.get("ideb")
        ano = ultimo_ideb_valido.get("ano")

        if valor is not None:
            leituras.append(
                f"O IDEB mais recente disponível é {valor} em {ano}, na etapa {etapa}."
            )

            if etapa == "anos_iniciais" and media_municipio and media_municipio.get("ideb_ai") is not None:
                if valor >= media_municipio["ideb_ai"]:
                    pontos_fortes.append("O IDEB dos anos iniciais está igual ou acima da média municipal.")
                else:
                    pontos_atencao.append("O IDEB dos anos iniciais está abaixo da média municipal.")

            if etapa == "anos_finais" and media_municipio and media_municipio.get("ideb_af") is not None:
                if valor >= media_municipio["ideb_af"]:
                    pontos_fortes.append("O IDEB dos anos finais está igual ou acima da média municipal.")
                else:
                    pontos_atencao.append("O IDEB dos anos finais está abaixo da média municipal.")

            if etapa == "ensino_medio" and media_municipio and media_municipio.get("ideb_em") is not None:
                if valor >= media_municipio["ideb_em"]:
                    pontos_fortes.append("O IDEB do ensino médio está igual ou acima da média municipal.")
                else:
                    pontos_atencao.append("O IDEB do ensino médio está abaixo da média municipal.")

    if escola_dict.get("tem_rendimento"):
        pontos_fortes.append("A escola possui informações de rendimento disponíveis.")
    else:
        pontos_atencao.append("A escola não possui indicadores de rendimento disponíveis.")

    if historico_rows:
        pontos_fortes.append("A escola possui série histórica de indicadores na base.")
    else:
        pontos_atencao.append("A escola não possui histórico de indicadores na base consultada.")

    if escola_dict.get("ultimo_ano_censo") is not None:
        leituras.append(
            f"O último ano de censo disponível para esta escola é {escola_dict['ultimo_ano_censo']}."
        )

    resumo_geral = "A escola possui base mínima para acompanhamento no painel."
    if len(pontos_atencao) >= 3:
        resumo_geral = "A escola apresenta lacunas importantes de informação ou desempenho que merecem atenção."
    elif len(pontos_atencao) >= 1:
        resumo_geral = "A escola possui dados relevantes para análise, mas há pontos de atenção a observar."

    return {
        "co_entidade": co_entidade,
        "encontrada": True,
        "mensagem": "Diagnóstico gerado com sucesso.",
        "identificacao": {
            "no_entidade": escola_dict.get("no_entidade"),
            "no_municipio": escola_dict.get("no_municipio"),
            "sg_uf": escola_dict.get("sg_uf"),
            "tp_dependencia": escola_dict.get("tp_dependencia"),
            "tp_localizacao": escola_dict.get("tp_localizacao"),
            "escola_ativa": escola_dict.get("escola_ativa")
        },
        "resumo_geral": resumo_geral,
        "leituras": leituras,
        "pontos_fortes": pontos_fortes,
        "pontos_atencao": pontos_atencao,
        "indicadores_recentes": {
            "inse_valor": escola_dict.get("inse_valor"),
            "inse_grupo": escola_dict.get("inse_grupo"),
            "ideb_ai_recente": escola_dict.get("ideb_ai_recente"),
            "ideb_af_recente": escola_dict.get("ideb_af_recente"),
            "ideb_em_recente": escola_dict.get("ideb_em_recente"),
            "ultimo_historico": ultimo_historico,
            "ultimo_ideb": ultimo_ideb_valido
        },
        "comparativos": {
            "municipio": dict(media_municipio) if media_municipio else None,
            "uf": dict(media_uf) if media_uf else None
        }
    }
# MAPA DAS ESCOLAS
@app.get("/mapa/escolas")
def mapa_escolas(
    uf: str | None = None,
    co_municipio: int | None = None,
    limite: int = 2000
):

    query = """
        SELECT
            co_entidade,
            no_entidade,
            co_municipio,
            no_municipio,
            sg_uf,
            latitude,
            longitude,
            inse_valor,
            ideb_ai_recente,
            ideb_af_recente,
            ideb_em_recente
        FROM vw_escolas_mapa
        WHERE latitude IS NOT NULL
    """

    params = {"limite": limite}

    if uf:
        query += " AND sg_uf = :uf"
        params["uf"] = uf.upper()

    if co_municipio:
        query += " AND co_municipio = :co_municipio"
        params["co_municipio"] = co_municipio

    query += " ORDER BY no_entidade LIMIT :limite"

    with engine.connect() as conn:
        rows = conn.execute(text(query), params).mappings().all()

    return {
        "quantidade": len(rows),
        "mensagem": "Consulta realizada com sucesso.",
        "escolas": [dict(row) for row in rows]
    }
# MAPA DAS ESCOLAS EM GEOJSON
@app.get("/mapa/escolas/geojson")
def mapa_escolas_geojson(
    uf: str | None = None,
    co_municipio: int | None = None,
    limite: int = 2000
):
    query = """
        SELECT
            co_entidade,
            no_entidade,
            co_municipio,
            no_municipio,
            sg_uf,
            tp_dependencia,
            latitude,
            longitude,
            inse_valor,
            ideb_ai_recente,
            ideb_af_recente,
            ideb_em_recente
        FROM vw_escolas_mapa
        WHERE latitude IS NOT NULL
          AND longitude IS NOT NULL
    """

    params = {"limite": limite}

    if uf:
        query += " AND sg_uf = :uf"
        params["uf"] = uf.upper()

    if co_municipio:
        query += " AND co_municipio = :co_municipio"
        params["co_municipio"] = co_municipio

    query += " ORDER BY no_entidade LIMIT :limite"

    with engine.connect() as conn:
        rows = conn.execute(text(query), params).mappings().all()

    features = []
    for row in rows:
        item = dict(row)
        features.append({
            "type": "Feature",
            "geometry": {
                "type": "Point",
                "coordinates": [
                    float(item["longitude"]),
                    float(item["latitude"])
                ]
            },
            "properties": {
                "co_entidade": item["co_entidade"],
                "no_entidade": item["no_entidade"],
                "co_municipio": item["co_municipio"],
                "no_municipio": item["no_municipio"],
                "sg_uf": item["sg_uf"],
                "tp_dependencia": item["tp_dependencia"],
                "inse_valor": item["inse_valor"],
                "ideb_ai_recente": item["ideb_ai_recente"],
                "ideb_af_recente": item["ideb_af_recente"],
                "ideb_em_recente": item["ideb_em_recente"]
            }
        })

    return {
        "type": "FeatureCollection",
        "quantidade": len(features),
        "features": features
    }


# MAPA DAS ESCOLAS PÚBLICAS EM GEOJSON
@app.get("/mapa/escolas-publicas/geojson")
def mapa_escolas_publicas_geojson(
    uf: str | None = None,
    co_municipio: int | None = None,
    limite: int = 2000
):
    query = """
        SELECT
            co_entidade,
            no_entidade,
            co_municipio,
            no_municipio,
            sg_uf,
            tp_dependencia,
            latitude,
            longitude,
            inse_valor,
            ideb_ai_recente,
            ideb_af_recente,
            ideb_em_recente
        FROM vw_escolas_mapa
        WHERE latitude IS NOT NULL
          AND longitude IS NOT NULL
          AND tp_dependencia IN (1, 2, 3)
    """

    params = {"limite": limite}

    if uf:
        query += " AND sg_uf = :uf"
        params["uf"] = uf.upper()

    if co_municipio:
        query += " AND co_municipio = :co_municipio"
        params["co_municipio"] = co_municipio

    query += " ORDER BY no_entidade LIMIT :limite"

    with engine.connect() as conn:
        rows = conn.execute(text(query), params).mappings().all()

    features = []
    for row in rows:
        item = dict(row)
        features.append({
            "type": "Feature",
            "geometry": {
                "type": "Point",
                "coordinates": [
                    float(item["longitude"]),
                    float(item["latitude"])
                ]
            },
            "properties": {
                "co_entidade": item["co_entidade"],
                "no_entidade": item["no_entidade"],
                "co_municipio": item["co_municipio"],
                "no_municipio": item["no_municipio"],
                "sg_uf": item["sg_uf"],
                "tp_dependencia": item["tp_dependencia"],
                "inse_valor": item["inse_valor"],
                "ideb_ai_recente": item["ideb_ai_recente"],
                "ideb_af_recente": item["ideb_af_recente"],
                "ideb_em_recente": item["ideb_em_recente"]
            }
        })

    return {
        "type": "FeatureCollection",
        "quantidade": len(features),
        "features": features
    }
# ESCOLA PÚBLICA - DADOS BÁSICOS
@app.get("/publicas/escolas/{co_entidade}")
def buscar_escola_publica(co_entidade: int):
    with engine.connect() as conn:
        result = conn.execute(
            text("""
                SELECT *
                FROM vw_raiox_escola_publica
                WHERE co_entidade = :co_entidade
            """),
            {"co_entidade": co_entidade}
        )
        row = result.mappings().first()

    if not row:
        return {
            "co_entidade": co_entidade,
            "encontrada": False,
            "mensagem": "Escola pública não encontrada."
        }

    return {
        "co_entidade": co_entidade,
        "encontrada": True,
        "mensagem": "Consulta realizada com sucesso.",
        "escola": dict(row)
    }


# BUSCA DE ESCOLAS PÚBLICAS POR NOME
@app.get("/publicas/buscar/escolas")
def buscar_escolas_publicas_por_nome(nome: str):
    with engine.connect() as conn:
        result = conn.execute(
            text("""
                SELECT *
                FROM vw_raiox_escola_publica
                WHERE no_entidade ILIKE :nome
                ORDER BY no_entidade
                LIMIT 50
            """),
            {"nome": f"%{nome}%"}
        )
        rows = result.mappings().all()

    if not rows:
        return {
            "busca": nome,
            "quantidade": 0,
            "mensagem": "Nenhuma escola pública encontrada para o termo pesquisado.",
            "escolas": []
        }

    return {
        "busca": nome,
        "quantidade": len(rows),
        "mensagem": "Consulta realizada com sucesso.",
        "escolas": [dict(row) for row in rows]
    }


# ESCOLAS PÚBLICAS DO MUNICÍPIO
@app.get("/publicas/municipios/{co_municipio}/escolas")
def buscar_escolas_publicas_municipio(co_municipio: int):
    with engine.connect() as conn:
        result = conn.execute(
            text("""
                SELECT *
                FROM vw_raiox_escola_publica
                WHERE co_municipio = :co_municipio
                ORDER BY no_entidade
            """),
            {"co_municipio": co_municipio}
        )
        rows = result.mappings().all()

    if not rows:
        return {
            "co_municipio": co_municipio,
            "quantidade": 0,
            "mensagem": "Nenhuma escola pública encontrada para este município.",
            "escolas": []
        }

    return {
        "co_municipio": co_municipio,
        "quantidade": len(rows),
        "mensagem": "Consulta realizada com sucesso.",
        "escolas": [dict(row) for row in rows]
    }


# UFs COM ESCOLAS PÚBLICAS
@app.get("/publicas/ufs")
def listar_ufs_publicas():
    with engine.connect() as conn:
        result = conn.execute(
            text("""
                SELECT *
                FROM vw_ufs_publicas
                ORDER BY uf
            """)
        )
        rows = result.mappings().all()

    return {
        "quantidade": len(rows),
        "mensagem": "Consulta realizada com sucesso.",
        "ufs": [dict(row) for row in rows]
    }


# MUNICÍPIOS COM ESCOLAS PÚBLICAS POR UF
@app.get("/publicas/ufs/{uf}/municipios")
def listar_municipios_publicos_por_uf(uf: str):
    with engine.connect() as conn:
        result = conn.execute(
            text("""
                SELECT *
                FROM vw_municipios_publicos
                WHERE uf = :uf
                ORDER BY no_municipio
            """),
            {"uf": uf.upper()}
        )
        rows = result.mappings().all()

    if not rows:
        return {
            "uf": uf.upper(),
            "quantidade": 0,
            "mensagem": "Nenhum município com escola pública encontrado para esta UF.",
            "municipios": []
        }

    return {
        "uf": uf.upper(),
        "quantidade": len(rows),
        "mensagem": "Consulta realizada com sucesso.",
        "municipios": [dict(row) for row in rows]
    }


# DASHBOARD DE ESCOLA PÚBLICA
@app.get("/publicas/dashboard/escola/{co_entidade}")
def buscar_dashboard_escola_publica(co_entidade: int):
    with engine.connect() as conn:
        escola_result = conn.execute(
            text("""
                SELECT *
                FROM vw_raiox_escola_publica
                WHERE co_entidade = :co_entidade
            """),
            {"co_entidade": co_entidade}
        )
        escola = escola_result.mappings().first()

        historico_result = conn.execute(
            text("""
                SELECT *
                FROM vw_escola_indicadores
                WHERE co_entidade = :co_entidade
                ORDER BY ano
            """),
            {"co_entidade": co_entidade}
        )
        historico_rows = historico_result.mappings().all()

        ideb_result = conn.execute(
            text("""
                SELECT *
                FROM vw_escola_ideb
                WHERE co_entidade = :co_entidade
                ORDER BY ano, etapa
            """),
            {"co_entidade": co_entidade}
        )
        ideb_rows = ideb_result.mappings().all()

    if not escola:
        return {
            "co_entidade": co_entidade,
            "encontrada": False,
            "mensagem": "Escola pública não encontrada."
        }

    escola_dict = dict(escola)
    historico = [dict(row) for row in historico_rows]

    ideb_formatado = []
    tem_ideb_divulgado = False

    for row in ideb_rows:
        item = dict(row)
        if item.get("ideb") is None and item.get("nota_saeb") is None:
            item["observacao"] = "IDEB não divulgado para este ano ou etapa."
        else:
            tem_ideb_divulgado = True
        ideb_formatado.append(item)

    anos_historico = sorted(list({
        row["ano"] for row in historico_rows if row.get("ano") is not None
    }))

    anos_ideb = sorted(list({
        row["ano"] for row in ideb_rows if row.get("ano") is not None
    }))

    anos_disponiveis = sorted(list(set(anos_historico + anos_ideb)))

    ultimo_historico = historico[-1] if historico else None

    ultimo_ideb = None
    for item in sorted(ideb_formatado, key=lambda x: (x.get("ano") or 0), reverse=True):
        if item.get("ideb") is not None or item.get("nota_saeb") is not None:
            ultimo_ideb = item
            break

    disponibilidade = {
        "tem_inse": bool(escola_dict.get("tem_inse")),
        "tem_afd": bool(escola_dict.get("tem_afd")),
        "tem_icg": bool(escola_dict.get("tem_icg")),
        "tem_ied": bool(escola_dict.get("tem_ied")),
        "tem_ird": bool(escola_dict.get("tem_ird")),
        "tem_atu": bool(escola_dict.get("tem_atu")),
        "tem_had": bool(escola_dict.get("tem_had")),
        "tem_tdi": bool(escola_dict.get("tem_tdi")),
        "tem_tnr": bool(escola_dict.get("tem_tnr")),
        "tem_rendimento": bool(escola_dict.get("tem_rendimento")),
        "tem_ideb_ai": escola_dict.get("ideb_ai_recente") is not None,
        "tem_ideb_af": escola_dict.get("ideb_af_recente") is not None,
        "tem_ideb_em": escola_dict.get("ideb_em_recente") is not None
    }

    avisos = []

    if not historico_rows:
        avisos.append({
            "nivel": "atencao",
            "codigo": "sem_historico",
            "titulo": "Sem histórico disponível",
            "mensagem": "A escola não possui histórico de indicadores na base consultada."
        })

    if not ideb_rows:
        avisos.append({
            "nivel": "atencao",
            "codigo": "sem_ideb",
            "titulo": "Sem registros de IDEB",
            "mensagem": "A escola não possui registros de IDEB na base consultada."
        })
    elif not tem_ideb_divulgado:
        avisos.append({
            "nivel": "atencao",
            "codigo": "ideb_nao_divulgado",
            "titulo": "IDEB não divulgado",
            "mensagem": "Há registros de IDEB para a escola, mas o indicador não foi divulgado."
        })

    identificacao = {
        "co_entidade": escola_dict.get("co_entidade"),
        "no_entidade": escola_dict.get("no_entidade"),
        "co_municipio": escola_dict.get("co_municipio"),
        "no_municipio": escola_dict.get("no_municipio"),
        "sg_uf": escola_dict.get("sg_uf"),
        "tp_dependencia": escola_dict.get("tp_dependencia"),
        "tp_localizacao": escola_dict.get("tp_localizacao"),
        "tp_situacao_funcionamento": escola_dict.get("tp_situacao_funcionamento"),
        "escola_ativa": escola_dict.get("escola_ativa")
    }

    indicadores_recentes = {
        "ultimo_ano_censo": escola_dict.get("ultimo_ano_censo"),
        "inse": {
            "valor": escola_dict.get("inse_valor"),
            "grupo": escola_dict.get("inse_grupo"),
            "disponivel": disponibilidade["tem_inse"]
        },
        "ideb": {
            "anos_iniciais": {
                "ano": escola_dict.get("ideb_ai_ano"),
                "valor": escola_dict.get("ideb_ai_recente"),
                "disponivel": disponibilidade["tem_ideb_ai"]
            },
            "anos_finais": {
                "ano": escola_dict.get("ideb_af_ano"),
                "valor": escola_dict.get("ideb_af_recente"),
                "disponivel": disponibilidade["tem_ideb_af"]
            },
            "ensino_medio": {
                "ano": escola_dict.get("ideb_em_ano"),
                "valor": escola_dict.get("ideb_em_recente"),
                "disponivel": disponibilidade["tem_ideb_em"]
            }
        },
        "ultimo_historico": ultimo_historico,
        "ultimo_ideb": ultimo_ideb
    }

    series_graficos = {
        "anos_disponiveis": anos_disponiveis,
        "historico_indicadores": historico,
        "ideb": ideb_formatado
    }

    return {
        "co_entidade": co_entidade,
        "encontrada": True,
        "mensagem": "Consulta realizada com sucesso.",
        "identificacao": identificacao,
        "indicadores_recentes": indicadores_recentes,
        "series_graficos": series_graficos,
        "avisos": avisos,
        "disponibilidade": disponibilidade
    }
