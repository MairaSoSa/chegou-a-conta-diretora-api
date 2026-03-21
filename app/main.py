from fastapi import FastAPI, HTTPException, Query
from sqlalchemy import text
from typing import Optional

from app.db import engine

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


# ESCOLA COMPLETA (IDENTIFICAÇÃO + CENSO + HISTÓRICO)
@app.get("/escolas/{co_entidade}/completo")
def buscar_escola_completa(co_entidade: int):
    def buscar_um_registro(conn, table_name: str, ordem_coluna: str):
        if table_name not in tabelas_disponiveis:
            return None

        row = conn.execute(
            text(
                f"""
                SELECT *
                FROM {table_name}
                WHERE co_entidade = :co_entidade
                ORDER BY {ordem_coluna} DESC
                LIMIT 1
                """
            ),
            {"co_entidade": co_entidade},
        ).mappings().first()

        return dict(row) if row else None

    def buscar_varios_registros(conn, table_name: str, order_by: str):
        if table_name not in tabelas_disponiveis:
            return []

        rows = conn.execute(
            text(
                f"""
                SELECT *
                FROM {table_name}
                WHERE co_entidade = :co_entidade
                ORDER BY {order_by}
                """
            ),
            {"co_entidade": co_entidade},
        ).mappings().all()

        return [dict(row) for row in rows]

    with engine.connect() as conn:
        tabelas_disponiveis = {
            row[0]
            for row in conn.execute(
                text(
                    """
                    SELECT table_name
                    FROM information_schema.tables
                    WHERE table_schema = 'public'
                      AND table_name IN (
                          'gestor_escolar',
                          'matricula_escola',
                          'turma_escola',
                          'inse_escola',
                          'afd_escola',
                          'icg_escola',
                          'rendimento_escola',
                          'tnr_escola',
                          'ideb_escola',
                          'ideb_escola_historico'
                      )
                    """
                )
            )
        }

        escola = conn.execute(
            text("""
                SELECT *
                FROM censo_escolas
                WHERE co_entidade = :co_entidade
                ORDER BY ano DESC
                LIMIT 1
            """),
            {"co_entidade": co_entidade}
        ).mappings().first()

        if not escola:
            raise HTTPException(status_code=404, detail="Escola não encontrada")

        gestor = buscar_um_registro(conn, "gestor_escolar", "nu_ano_censo")
        matricula = buscar_um_registro(conn, "matricula_escola", "nu_ano_censo")
        turmas = buscar_um_registro(conn, "turma_escola", "nu_ano_censo")

        inse = buscar_varios_registros(conn, "inse_escola", "ano DESC")
        afd = buscar_varios_registros(conn, "afd_escola", "ano DESC")
        icg = buscar_varios_registros(conn, "icg_escola", "ano DESC")
        rendimento = buscar_varios_registros(conn, "rendimento_escola", "ano DESC")
        tnr = buscar_varios_registros(conn, "tnr_escola", "ano DESC")
        ideb_recente = buscar_varios_registros(conn, "ideb_escola", "ano DESC, etapa")
        ideb_historico = buscar_varios_registros(conn, "ideb_escola_historico", "ano DESC, etapa")

        escola_raiox = conn.execute(
            text("""
                SELECT *
                FROM vw_raiox_escola
                WHERE co_entidade = :co_entidade
            """),
            {"co_entidade": co_entidade}
        ).mappings().first()

        historico_rows = conn.execute(
            text("""
                SELECT *
                FROM vw_escola_indicadores
                WHERE co_entidade = :co_entidade
                ORDER BY ano
            """),
            {"co_entidade": co_entidade}
        ).mappings().all()

    escola_dict = dict(escola)
    escola_raiox_dict = dict(escola_raiox) if escola_raiox else {}

    return {
        "co_entidade": co_entidade,
        "encontrada": True,
        "mensagem": "Consulta realizada com sucesso.",
        "escola": {
            "identificacao": {
                "co_entidade": escola_dict.get("co_entidade"),
                "no_entidade": escola_dict.get("no_entidade"),
                "co_uf": escola_dict.get("co_uf"),
                "sg_uf": escola_dict.get("sg_uf"),
                "no_uf": escola_dict.get("no_uf"),
                "co_municipio": escola_dict.get("co_municipio"),
                "no_municipio": escola_dict.get("no_municipio"),
                "co_distrito": escola_dict.get("co_distrito"),
                "tp_dependencia": escola_dict.get("tp_dependencia"),
                "tp_localizacao": escola_dict.get("tp_localizacao"),
                "tp_categoria_escola_privada": escola_dict.get("tp_categoria_escola_privada")
            },

            "regionalizacao": {
                "co_regiao": escola_dict.get("co_regiao"),
                "no_regiao": escola_dict.get("no_regiao"),
                "co_mesorregiao": escola_dict.get("co_mesorregiao"),
                "no_mesorregiao": escola_dict.get("no_mesorregiao"),
                "co_microrregiao": escola_dict.get("co_microrregiao"),
                "no_microrregiao": escola_dict.get("no_microrregiao")
            },

            "localizacao_contato": {
                "nu_cep": escola_dict.get("nu_cep"),
                "endereco": escola_dict.get("ds_endereco"),
                "numero": escola_dict.get("nu_endereco"),
                "bairro": escola_dict.get("no_bairro"),
                "ddd": escola_dict.get("nu_ddd"),
                "telefone": escola_dict.get("nu_telefone"),
                "outro_telefone": escola_dict.get("nu_outro_telefone"),
                "email": escola_dict.get("email")
            },

            "funcionamento": {
                "ano": escola_dict.get("ano"),
                "tp_situacao_funcionamento": escola_dict.get("tp_situacao_funcionamento"),
                "in_escola_ativa": escola_raiox_dict.get("escola_ativa"),
                "in_conveniada_pp": escola_dict.get("in_conveniada_pp"),
                "in_regulamentacao": escola_dict.get("in_regulamentacao"),
                "in_local_func_predio_escolar": escola_dict.get("in_local_func_predio_escolar"),
                "in_local_func_salas_empresa": escola_dict.get("in_local_func_salas_empresa"),
                "in_local_func_socioeducativo": escola_dict.get("in_local_func_unidade_socioeducativa"),
                "in_local_func_prisional": escola_dict.get("in_local_func_unidade_prisional")
            },

            "infraestrutura_saneamento": {
                "agua_potavel": escola_dict.get("in_agua_potavel"),
                "agua_rede_publica": escola_dict.get("in_agua_rede_publica"),
                "agua_poco_artesiano": escola_dict.get("in_agua_poco_artesiano"),
                "agua_cacimba": escola_dict.get("in_agua_cacimba"),
                "agua_fonte_rio": escola_dict.get("in_agua_fonte_rio"),
                "agua_carro_pipa": escola_dict.get("in_agua_carro_pipa"),
                "agua_inexistente": escola_dict.get("in_agua_inexistente"),
                "energia_rede_publica": escola_dict.get("in_energia_rede_publica"),
                "energia_gerador_fossil": escola_dict.get("in_energia_gerador_fossil"),
                "energia_renovavel": escola_dict.get("in_energia_renovavel"),
                "energia_inexistente": escola_dict.get("in_energia_inexistente"),
                "esgoto_rede_publica": escola_dict.get("in_esgoto_rede_publica"),
                "esgoto_fossa_septica": escola_dict.get("in_esgoto_fossa_septica"),
                "esgoto_fossa_comum": escola_dict.get("in_esgoto_fossa_comum"),
                "esgoto_fossa": escola_dict.get("in_esgoto_fossa"),
                "esgoto_inexistente": escola_dict.get("in_esgoto_inexistente"),
                "lixo_coleta_publica": escola_dict.get("in_lixo_servico_coleta"),
                "lixo_queima": escola_dict.get("in_lixo_queima"),
                "lixo_enterra": escola_dict.get("in_lixo_enterra"),
                "lixo_destino_publico": escola_dict.get("in_lixo_destino_final_publico"),
                "tratamento_lixo_separacao": escola_dict.get("in_tratamento_lixo_separacao"),
                "tratamento_lixo_reutiliza": escola_dict.get("in_tratamento_lixo_reutiliza"),
                "tratamento_lixo_reciclagem": escola_dict.get("in_tratamento_lixo_reciclagem")
            },

            "dependencias": {
                "sala_diretoria": escola_dict.get("in_sala_diretoria"),
                "sala_professor": escola_dict.get("in_sala_professor"),
                "laboratorio_informatica": escola_dict.get("in_laboratorio_informatica"),
                "laboratorio_ciencias": escola_dict.get("in_laboratorio_ciencias"),
                "biblioteca": escola_dict.get("in_biblioteca"),
                "biblioteca_sala_leitura": escola_dict.get("in_biblioteca_sala_leitura"),
                "cozinha": escola_dict.get("in_cozinha"),
                "refeitorio": escola_dict.get("in_refeitorio"),
                "almoxarifado": escola_dict.get("in_almoxarifado"),
                "auditorio": escola_dict.get("in_auditorio"),
                "patio_coberto": escola_dict.get("in_patio_coberto"),
                "patio_descoberto": escola_dict.get("in_patio_descoberto"),
                "quadra_esportes": escola_dict.get("in_quadra_esportes"),
                "quadra_esportes_coberta": escola_dict.get("in_quadra_esportes_coberta"),
                "parque_infantil": escola_dict.get("in_parque_infantil"),
                "banheiro": escola_dict.get("in_banheiro"),
                "banheiro_ei": escola_dict.get("in_banheiro_ei"),
                "banheiro_pne": escola_dict.get("in_banheiro_pne"),
                "banheiro_funcionarios": escola_dict.get("in_banheiro_funcionarios"),
                "dormitorio_aluno": escola_dict.get("in_dormitorio_aluno"),
                "dormitorio_professor": escola_dict.get("in_dormitorio_professor")
            },

            "tecnologia_conectividade": {
                "internet": escola_dict.get("in_internet"),
                "internet_alunos": escola_dict.get("in_internet_alunos"),
                "internet_administrativo": escola_dict.get("in_internet_administrativo"),
                "internet_aprendizagem": escola_dict.get("in_internet_aprendizagem"),
                "banda_larga": escola_dict.get("in_banda_larga"),
                "computadores": escola_dict.get("in_computador"),
                "tablet_aluno": escola_dict.get("in_tablet_aluno"),
                "equip_tv": escola_dict.get("in_equip_tv"),
                "equip_multimidia": escola_dict.get("in_equip_multimidia"),
                "impressora": escola_dict.get("in_equip_impressora")
            },

            "acessibilidade": {
                "corrimao": escola_dict.get("in_acessibilidade_corrimao"),
                "elevador": escola_dict.get("in_acessibilidade_elevador"),
                "pisos_tateis": escola_dict.get("in_acessibilidade_pisos_tateis"),
                "rampas": escola_dict.get("in_acessibilidade_rampas"),
                "sinal_visual": escola_dict.get("in_acessibilidade_sinal_visual"),
                "acessibilidade_inexistente": escola_dict.get("in_acessibilidade_inexistente")
            },

            "alimentacao_servicos": {
                "alimentacao_escolar": escola_dict.get("in_alimentacao"),
                "atendimento_aee": escola_dict.get("in_atendimento_especializado"),
                "atividade_complementar": escola_dict.get("in_atividade_complementar")
            },

            "estrutura": {
                "matriculas": {
                    "qt_mat_bas": escola_dict.get("qt_mat_bas"),
                    "qt_mat_inf": escola_dict.get("qt_mat_inf"),
                    "qt_mat_fund": escola_dict.get("qt_mat_fund"),
                    "qt_mat_med": escola_dict.get("qt_mat_med"),
                    "qt_mat_eja": escola_dict.get("qt_mat_eja")
                },
                "docentes": {
                    "qt_doc_bas": escola_dict.get("qt_doc_bas"),
                    "qt_doc_inf": escola_dict.get("qt_doc_inf"),
                    "qt_doc_fund": escola_dict.get("qt_doc_fund"),
                    "qt_doc_med": escola_dict.get("qt_doc_med")
                },
                "turmas": {
                    "qt_tur_bas": escola_dict.get("qt_tur_bas"),
                    "qt_tur_inf": escola_dict.get("qt_tur_inf"),
                    "qt_tur_fund": escola_dict.get("qt_tur_fund"),
                    "qt_tur_med": escola_dict.get("qt_tur_med")
                }
            },

            "capacidade_fisica": {
                "qt_salas_utilizadas": escola_dict.get("qt_salas_utilizadas"),
                "qt_salas_utilizadas_dentro": escola_dict.get("qt_salas_utilizadas_dentro"),
                "qt_salas_utilizadas_fora": escola_dict.get("qt_salas_utilizadas_fora")
            },

            "gestao": gestor,
            "matricula": matricula,
            "turmas": turmas,

            "indicadores_recentes": {
                "ultimo_ano_censo": escola_raiox_dict.get("ultimo_ano_censo", escola_dict.get("ano")),
                "inse": {
                    "valor": escola_raiox_dict.get("inse_valor"),
                    "grupo": escola_raiox_dict.get("inse_grupo"),
                    "disponivel": bool(escola_raiox_dict.get("tem_inse")),
                },
                "ideb": {
                    "anos_iniciais": {
                        "ano": escola_raiox_dict.get("ideb_ai_ano"),
                        "valor": escola_raiox_dict.get("ideb_ai_recente"),
                        "disponivel": escola_raiox_dict.get("ideb_ai_recente") is not None,
                    },
                    "anos_finais": {
                        "ano": escola_raiox_dict.get("ideb_af_ano"),
                        "valor": escola_raiox_dict.get("ideb_af_recente"),
                        "disponivel": escola_raiox_dict.get("ideb_af_recente") is not None,
                    },
                    "ensino_medio": {
                        "ano": escola_raiox_dict.get("ideb_em_ano"),
                        "valor": escola_raiox_dict.get("ideb_em_recente"),
                        "disponivel": escola_raiox_dict.get("ideb_em_recente") is not None,
                    },
                },
                "disponibilidade": {
                    "tem_inse": bool(escola_raiox_dict.get("tem_inse")),
                    "tem_afd": bool(escola_raiox_dict.get("tem_afd")),
                    "tem_icg": bool(escola_raiox_dict.get("tem_icg")),
                    "tem_ied": bool(escola_raiox_dict.get("tem_ied")),
                    "tem_ird": bool(escola_raiox_dict.get("tem_ird")),
                    "tem_atu": bool(escola_raiox_dict.get("tem_atu")),
                    "tem_had": bool(escola_raiox_dict.get("tem_had")),
                    "tem_tdi": bool(escola_raiox_dict.get("tem_tdi")),
                    "tem_tnr": bool(escola_raiox_dict.get("tem_tnr")),
                    "tem_rendimento": bool(escola_raiox_dict.get("tem_rendimento")),
                }
            },
            "indicadores": {
                "inse": inse,
                "afd": afd,
                "icg": icg,
                "rendimento": rendimento,
                "tnr": tnr,
                "ideb_recente": ideb_recente,
                "ideb_historico": ideb_historico
            },

            "historico": [dict(row) for row in historico_rows],
            "raw": escola_dict
        }
    }


# AFD - ADEQUAÇÃO DA FORMAÇÃO DOCENTE

@app.get("/afd")
def listar_afd(
    ano: Optional[int] = Query(None, description="Filtrar por ano"),
    limit: int = Query(100, ge=1, le=10000, description="Máximo de registros"),
    offset: int = Query(0, ge=0, description="Deslocamento para paginação"),
):
    """
    Retorna todos os registros de AFD (Adequação da Formação Docente) do banco.
    Suporta filtro por ano e paginação.
    """
    with engine.connect() as conn:
        filters = "WHERE ano = :ano" if ano else ""
        params = {"ano": ano, "limit": limit, "offset": offset}

        total_result = conn.execute(
            text(f"SELECT COUNT(*) FROM afd_escola {filters}"),
            params,
        )
        total = total_result.scalar()

        rows = conn.execute(
            text(f"""
                SELECT ano, co_entidade, dados_json
                FROM afd_escola
                {filters}
                ORDER BY ano DESC, co_entidade
                LIMIT :limit OFFSET :offset
            """),
            params,
        ).mappings().all()

    return {
        "indicador": "AFD",
        "descricao": "Adequação da Formação Docente",
        "total": total,
        "limit": limit,
        "offset": offset,
        "registros": [
            {
                "ano": row["ano"],
                "co_entidade": row["co_entidade"],
                **row["dados_json"],
            }
            for row in rows
        ],
    }


@app.get("/afd/{co_entidade}")
def buscar_afd_escola(co_entidade: int):
    """
    Retorna todos os registros de AFD de uma escola específica, ordenados por ano.
    """
    with engine.connect() as conn:
        rows = conn.execute(
            text("""
                SELECT ano, co_entidade, dados_json
                FROM afd_escola
                WHERE co_entidade = :co_entidade
                ORDER BY ano DESC
            """),
            {"co_entidade": co_entidade},
        ).mappings().all()

    if not rows:
        return {
            "co_entidade": co_entidade,
            "indicador": "AFD",
            "encontrado": False,
            "mensagem": "Nenhum dado de AFD encontrado para esta escola.",
            "registros": [],
        }

    return {
        "co_entidade": co_entidade,
        "indicador": "AFD",
        "descricao": "Adequação da Formação Docente",
        "encontrado": True,
        "total_anos": len(rows),
        "registros": [
            {
                "ano": row["ano"],
                **row["dados_json"],
            }
            for row in rows
        ],
    }


@app.get("/afd/{co_entidade}/{ano}")
def buscar_afd_escola_ano(co_entidade: int, ano: int):
    """
    Retorna o registro de AFD de uma escola para um ano específico.
    """
    with engine.connect() as conn:
        row = conn.execute(
            text("""
                SELECT ano, co_entidade, dados_json
                FROM afd_escola
                WHERE co_entidade = :co_entidade
                  AND ano = :ano
            """),
            {"co_entidade": co_entidade, "ano": ano},
        ).mappings().first()

    if not row:
        return {
            "co_entidade": co_entidade,
            "ano": ano,
            "indicador": "AFD",
            "encontrado": False,
            "mensagem": f"Nenhum dado de AFD encontrado para esta escola no ano {ano}.",
        }

    return {
        "co_entidade": co_entidade,
        "ano": ano,
        "indicador": "AFD",
        "descricao": "Adequação da Formação Docente",
        "encontrado": True,
        "dados": row["dados_json"],
    }


# CENSO ESCOLAR — DADOS COMPLETOS

@app.get("/censo")
def listar_censo(
    ano: Optional[int] = Query(None, description="Filtrar por ano"),
    sg_uf: Optional[str] = Query(None, description="Filtrar por UF (ex: SP)"),
    co_municipio: Optional[int] = Query(None, description="Filtrar por código do município"),
    tp_dependencia: Optional[int] = Query(None, description="1=Federal 2=Estadual 3=Municipal 4=Privada"),
    limit: int = Query(100, ge=1, le=10000, description="Máximo de registros"),
    offset: int = Query(0, ge=0, description="Deslocamento para paginação"),
):
    """
    Lista registros do Censo Escolar com todos os dados disponíveis.
    Suporta filtros por ano, UF, município e dependência administrativa.
    """
    conditions = []
    params: dict = {"limit": limit, "offset": offset}

    if ano:
        conditions.append("ano = :ano")
        params["ano"] = ano
    if sg_uf:
        conditions.append("sg_uf = :sg_uf")
        params["sg_uf"] = sg_uf.upper()
    if co_municipio:
        conditions.append("co_municipio = :co_municipio")
        params["co_municipio"] = co_municipio
    if tp_dependencia:
        conditions.append("tp_dependencia = :tp_dependencia")
        params["tp_dependencia"] = tp_dependencia

    where = ("WHERE " + " AND ".join(conditions)) if conditions else ""

    with engine.connect() as conn:
        total = conn.execute(
            text(f"SELECT COUNT(*) FROM censo_escola_historico {where}"), params
        ).scalar()

        rows = conn.execute(
            text(f"""
                SELECT ano, co_entidade, no_entidade, co_municipio, no_municipio,
                       sg_uf, tp_dependencia, tp_localizacao, tp_situacao_funcionamento,
                       dados_json
                FROM censo_escola_historico
                {where}
                ORDER BY ano DESC, co_entidade
                LIMIT :limit OFFSET :offset
            """),
            params,
        ).mappings().all()

    anos_disponiveis = None
    if not ano:
        with engine.connect() as conn:
            anos_rows = conn.execute(
                text("SELECT DISTINCT ano FROM censo_escola_historico ORDER BY ano")
            ).fetchall()
            anos_disponiveis = [r[0] for r in anos_rows]

    return {
        "total": total,
        "limit": limit,
        "offset": offset,
        "anos_disponiveis": anos_disponiveis,
        "registros": [
            {
                "ano": r["ano"],
                "co_entidade": r["co_entidade"],
                "no_entidade": r["no_entidade"],
                "co_municipio": r["co_municipio"],
                "no_municipio": r["no_municipio"],
                "sg_uf": r["sg_uf"],
                "tp_dependencia": r["tp_dependencia"],
                "tp_localizacao": r["tp_localizacao"],
                "tp_situacao_funcionamento": r["tp_situacao_funcionamento"],
                **(r["dados_json"] if r["dados_json"] else {}),
            }
            for r in rows
        ],
    }


@app.get("/censo/anos")
def listar_anos_censo():
    """
    Retorna os anos disponíveis no censo e a quantidade de escolas por ano.
    """
    with engine.connect() as conn:
        rows = conn.execute(
            text("""
                SELECT ano, COUNT(*) as qtd_escolas
                FROM censo_escola_historico
                GROUP BY ano
                ORDER BY ano
            """)
        ).fetchall()

    return {
        "anos": [{"ano": r[0], "qtd_escolas": r[1]} for r in rows],
        "total_anos": len(rows),
    }


@app.get("/censo/{co_entidade}")
def buscar_censo_escola(co_entidade: int):
    """
    Retorna o histórico completo do Censo Escolar de uma escola,
    com todos os dados disponíveis para cada ano.
    """
    with engine.connect() as conn:
        rows = conn.execute(
            text("""
                SELECT ano, co_entidade, no_entidade, co_municipio, no_municipio,
                       sg_uf, tp_dependencia, tp_localizacao, tp_situacao_funcionamento,
                       dados_json
                FROM censo_escola_historico
                WHERE co_entidade = :co_entidade
                ORDER BY ano DESC
            """),
            {"co_entidade": co_entidade},
        ).mappings().all()

    if not rows:
        return {
            "co_entidade": co_entidade,
            "encontrado": False,
            "mensagem": "Nenhum dado do censo encontrado para esta escola.",
            "historico": [],
        }

    return {
        "co_entidade": co_entidade,
        "no_entidade": rows[0]["no_entidade"],
        "encontrado": True,
        "total_anos": len(rows),
        "historico": [
            {
                "ano": r["ano"],
                "co_municipio": r["co_municipio"],
                "no_municipio": r["no_municipio"],
                "sg_uf": r["sg_uf"],
                "tp_dependencia": r["tp_dependencia"],
                "tp_localizacao": r["tp_localizacao"],
                "tp_situacao_funcionamento": r["tp_situacao_funcionamento"],
                **(r["dados_json"] if r["dados_json"] else {}),
            }
            for r in rows
        ],
    }


@app.get("/censo/{co_entidade}/{ano}")
def buscar_censo_escola_ano(co_entidade: int, ano: int):
    """
    Retorna todos os dados do Censo Escolar de uma escola em um ano específico.
    """
    with engine.connect() as conn:
        row = conn.execute(
            text("""
                SELECT ano, co_entidade, no_entidade, co_municipio, no_municipio,
                       sg_uf, tp_dependencia, tp_localizacao, tp_situacao_funcionamento,
                       dados_json
                FROM censo_escola_historico
                WHERE co_entidade = :co_entidade AND ano = :ano
            """),
            {"co_entidade": co_entidade, "ano": ano},
        ).mappings().first()

    if not row:
        return {
            "co_entidade": co_entidade,
            "ano": ano,
            "encontrado": False,
            "mensagem": f"Nenhum dado do censo encontrado para esta escola no ano {ano}.",
        }

    return {
        "co_entidade": co_entidade,
        "ano": ano,
        "encontrado": True,
        "dados": {
            "no_entidade": row["no_entidade"],
            "co_municipio": row["co_municipio"],
            "no_municipio": row["no_municipio"],
            "sg_uf": row["sg_uf"],
            "tp_dependencia": row["tp_dependencia"],
            "tp_localizacao": row["tp_localizacao"],
            "tp_situacao_funcionamento": row["tp_situacao_funcionamento"],
            **(row["dados_json"] if row["dados_json"] else {}),
        },
    }


# ---------------------------------------------------------------------------
# MATRÍCULA
# ---------------------------------------------------------------------------

def _endpoint_upsert_tabela(tabela: str, co_entidade: int, ano: Optional[int]):
    """Helper para endpoints de tabelas com PK (ano, co_entidade)."""
    with engine.connect() as conn:
        if ano:
            row = conn.execute(
                text(f"SELECT ano, co_entidade, dados_json FROM {tabela} WHERE co_entidade = :e AND ano = :a"),
                {"e": co_entidade, "a": ano},
            ).mappings().first()
            if not row:
                return {"co_entidade": co_entidade, "ano": ano, "encontrado": False, "dados": None}
            return {"co_entidade": co_entidade, "ano": ano, "encontrado": True,
                    "dados": row["dados_json"] or {}}
        else:
            rows = conn.execute(
                text(f"SELECT ano, co_entidade, dados_json FROM {tabela} WHERE co_entidade = :e ORDER BY ano DESC"),
                {"e": co_entidade},
            ).mappings().all()
            if not rows:
                return {"co_entidade": co_entidade, "encontrado": False, "historico": []}
            return {
                "co_entidade": co_entidade,
                "encontrado": True,
                "total_anos": len(rows),
                "historico": [{"ano": r["ano"], **(r["dados_json"] or {})} for r in rows],
            }


def _endpoint_multiplos_tabela(tabela: str, co_entidade: int, ano: Optional[int], limit: int, offset: int):
    """Helper para endpoints de tabelas com múltiplos registros por escola (docente, turma)."""
    conditions = ["co_entidade = :e"]
    params: dict = {"e": co_entidade, "limit": limit, "offset": offset}
    if ano:
        conditions.append("ano = :a")
        params["a"] = ano
    where = "WHERE " + " AND ".join(conditions)

    with engine.connect() as conn:
        total = conn.execute(text(f"SELECT COUNT(*) FROM {tabela} {where}"), params).scalar()
        rows = conn.execute(
            text(f"SELECT ano, co_entidade, dados_json FROM {tabela} {where} ORDER BY ano DESC LIMIT :limit OFFSET :offset"),
            params,
        ).mappings().all()

    if total == 0:
        return {"co_entidade": co_entidade, "encontrado": False, "total": 0, "registros": []}
    return {
        "co_entidade": co_entidade,
        "encontrado": True,
        "total": total,
        "limit": limit,
        "offset": offset,
        "registros": [{"ano": r["ano"], **(r["dados_json"] or {})} for r in rows],
    }


@app.get("/matricula/{co_entidade}")
def buscar_matricula(co_entidade: int, ano: Optional[int] = Query(None)):
    """Dados de matrícula de uma escola. Use ?ano= para filtrar por ano específico."""
    return _endpoint_upsert_tabela("matricula_escola", co_entidade, ano)


@app.get("/gestor/{co_entidade}")
def buscar_gestor(co_entidade: int, ano: Optional[int] = Query(None)):
    """Dados do gestor escolar. Use ?ano= para filtrar por ano específico."""
    return _endpoint_upsert_tabela("gestor_escolar", co_entidade, ano)


@app.get("/docentes/{co_entidade}")
def buscar_docentes(
    co_entidade: int,
    ano: Optional[int] = Query(None),
    limit: int = Query(100, ge=1, le=5000),
    offset: int = Query(0, ge=0),
):
    """Lista de docentes de uma escola. Use ?ano= para filtrar por ano."""
    return _endpoint_multiplos_tabela("docente_escola", co_entidade, ano, limit, offset)


@app.get("/turmas/{co_entidade}")
def buscar_turmas(
    co_entidade: int,
    ano: Optional[int] = Query(None),
    limit: int = Query(100, ge=1, le=5000),
    offset: int = Query(0, ge=0),
):
    """Lista de turmas de uma escola. Use ?ano= para filtrar por ano."""
    return _endpoint_multiplos_tabela("turma_escola", co_entidade, ano, limit, offset)
