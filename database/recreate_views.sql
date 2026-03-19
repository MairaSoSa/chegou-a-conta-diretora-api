BEGIN;

DROP VIEW IF EXISTS vw_municipios_publicos;
DROP VIEW IF EXISTS vw_ufs_publicas;
DROP VIEW IF EXISTS vw_municipios;
DROP VIEW IF EXISTS vw_ufs;
DROP VIEW IF EXISTS vw_escolas_mapa;
DROP VIEW IF EXISTS vw_raiox_escola_publica;
DROP VIEW IF EXISTS vw_raiox_escola;
DROP VIEW IF EXISTS vw_escola_indicadores;
DROP VIEW IF EXISTS vw_escola_ideb;

CREATE OR REPLACE VIEW vw_escola_ideb AS
WITH ideb_historico AS (
    SELECT
        ano::integer AS ano,
        co_entidade::bigint AS co_entidade,
        etapa::text AS etapa,
        ideb::numeric AS ideb,
        nota_saeb::numeric AS nota_saeb,
        taxa_aprovacao::numeric AS taxa_aprovacao,
        dados_json
    FROM ideb_escola_historico
),
ideb_base AS (
    SELECT
        i.ano::integer AS ano,
        i.co_entidade::bigint AS co_entidade,
        i.etapa::text AS etapa,
        NULL::numeric AS ideb,
        NULL::numeric AS nota_saeb,
        NULL::numeric AS taxa_aprovacao,
        i.dados_json
    FROM ideb_escola i
    WHERE NOT EXISTS (
        SELECT 1
        FROM ideb_historico h
        WHERE h.ano = i.ano
          AND h.co_entidade = i.co_entidade
          AND h.etapa = i.etapa
    )
)
SELECT * FROM ideb_historico
UNION ALL
SELECT * FROM ideb_base;

CREATE OR REPLACE VIEW vw_escola_indicadores AS
WITH anos_escola AS (
    SELECT co_entidade::bigint AS co_entidade, ano::integer AS ano FROM censo_escola_historico
    UNION
    SELECT co_entidade::bigint, ano::integer FROM inse_escola
    UNION
    SELECT co_entidade::bigint, ano::integer FROM afd_escola
    UNION
    SELECT co_entidade::bigint, ano::integer FROM atu_escola
    UNION
    SELECT co_entidade::bigint, ano::integer FROM had_escola
    UNION
    SELECT co_entidade::bigint, ano::integer FROM icg_escola
    UNION
    SELECT co_entidade::bigint, ano::integer FROM ied_escola
    UNION
    SELECT co_entidade::bigint, ano::integer FROM ird_escola
    UNION
    SELECT co_entidade::bigint, ano::integer FROM tdi_escola
    UNION
    SELECT co_entidade::bigint, ano::integer FROM tnr_escola
    UNION
    SELECT co_entidade::bigint, ano::integer FROM rendimento_escola
    UNION
    SELECT co_entidade::bigint, ano::integer FROM ideb_escola_historico
),
inse_valores AS (
    SELECT
        i.co_entidade::bigint AS co_entidade,
        i.ano::integer AS ano,
        CASE
            WHEN regexp_replace(
                replace(
                    COALESCE(
                        i.dados_json->>'INSE_VALOR',
                        i.dados_json->>'NU_INSE',
                        i.dados_json->>'PC_NIVEL_SOCIO_ECONOMICO',
                        i.dados_json->>'NIVEL_SOCIO_ECONOMICO'
                    ),
                    ',',
                    '.'
                ),
                '[^0-9.-]',
                '',
                'g'
            ) ~ '^-?[0-9]+(\.[0-9]+)?$'
            THEN regexp_replace(
                replace(
                    COALESCE(
                        i.dados_json->>'INSE_VALOR',
                        i.dados_json->>'NU_INSE',
                        i.dados_json->>'PC_NIVEL_SOCIO_ECONOMICO',
                        i.dados_json->>'NIVEL_SOCIO_ECONOMICO'
                    ),
                    ',',
                    '.'
                ),
                '[^0-9.-]',
                '',
                'g'
            )::numeric
            ELSE NULL
        END AS inse_valor,
        CASE
            WHEN regexp_replace(
                COALESCE(
                    i.dados_json->>'INSE_GRUPO',
                    i.dados_json->>'NU_GRUPO_INSE',
                    i.dados_json->>'CO_GRUPO_INSE'
                ),
                '[^0-9-]',
                '',
                'g'
            ) ~ '^-?[0-9]+$'
            THEN regexp_replace(
                COALESCE(
                    i.dados_json->>'INSE_GRUPO',
                    i.dados_json->>'NU_GRUPO_INSE',
                    i.dados_json->>'CO_GRUPO_INSE'
                ),
                '[^0-9-]',
                '',
                'g'
            )::integer
            ELSE NULL
        END AS inse_grupo,
        i.dados_json
    FROM inse_escola i
)
SELECT
    a.co_entidade,
    a.ano,
    c.no_entidade,
    c.co_municipio,
    c.no_municipio,
    c.sg_uf,
    c.tp_dependencia,
    c.tp_localizacao,
    c.tp_situacao_funcionamento,
    iv.inse_valor,
    iv.inse_grupo,
    ia.ideb AS ideb_ai,
    ifi.ideb AS ideb_af,
    im.ideb AS ideb_em,
    ia.nota_saeb AS nota_saeb_ai,
    ifi.nota_saeb AS nota_saeb_af,
    im.nota_saeb AS nota_saeb_em,
    ia.taxa_aprovacao AS taxa_aprovacao_ai,
    ifi.taxa_aprovacao AS taxa_aprovacao_af,
    im.taxa_aprovacao AS taxa_aprovacao_em,
    iv.dados_json AS inse_dados_json,
    afd.dados_json AS afd_dados_json,
    atu.dados_json AS atu_dados_json,
    had.dados_json AS had_dados_json,
    icg.dados_json AS icg_dados_json,
    ied.dados_json AS ied_dados_json,
    ird.dados_json AS ird_dados_json,
    tdi.dados_json AS tdi_dados_json,
    tnr.dados_json AS tnr_dados_json,
    rend.dados_json AS rendimento_dados_json
FROM anos_escola a
LEFT JOIN censo_escola_historico c
       ON c.co_entidade = a.co_entidade
      AND c.ano = a.ano
LEFT JOIN inse_valores iv
       ON iv.co_entidade = a.co_entidade
      AND iv.ano = a.ano
LEFT JOIN afd_escola afd
       ON afd.co_entidade = a.co_entidade
      AND afd.ano = a.ano
LEFT JOIN atu_escola atu
       ON atu.co_entidade = a.co_entidade
      AND atu.ano = a.ano
LEFT JOIN had_escola had
       ON had.co_entidade = a.co_entidade
      AND had.ano = a.ano
LEFT JOIN icg_escola icg
       ON icg.co_entidade = a.co_entidade
      AND icg.ano = a.ano
LEFT JOIN ied_escola ied
       ON ied.co_entidade = a.co_entidade
      AND ied.ano = a.ano
LEFT JOIN ird_escola ird
       ON ird.co_entidade = a.co_entidade
      AND ird.ano = a.ano
LEFT JOIN tdi_escola tdi
       ON tdi.co_entidade = a.co_entidade
      AND tdi.ano = a.ano
LEFT JOIN tnr_escola tnr
       ON tnr.co_entidade = a.co_entidade
      AND tnr.ano = a.ano
LEFT JOIN rendimento_escola rend
       ON rend.co_entidade = a.co_entidade
      AND rend.ano = a.ano
LEFT JOIN ideb_escola_historico ia
       ON ia.co_entidade = a.co_entidade
      AND ia.ano = a.ano
      AND ia.etapa = 'anos_iniciais'
LEFT JOIN ideb_escola_historico ifi
       ON ifi.co_entidade = a.co_entidade
      AND ifi.ano = a.ano
      AND ifi.etapa = 'anos_finais'
LEFT JOIN ideb_escola_historico im
       ON im.co_entidade = a.co_entidade
      AND im.ano = a.ano
      AND im.etapa = 'ensino_medio';

CREATE OR REPLACE VIEW vw_raiox_escola AS
WITH censo_recente AS (
    SELECT DISTINCT ON (co_entidade)
        co_entidade::bigint AS co_entidade,
        ano::integer AS ultimo_ano_censo,
        no_entidade,
        co_municipio,
        no_municipio,
        sg_uf,
        tp_dependencia,
        tp_localizacao,
        tp_situacao_funcionamento
    FROM censo_escola_historico
    ORDER BY co_entidade, ano DESC
),
base_escolas AS (
    SELECT
        COALESCE(c.co_entidade, e.co_entidade)::bigint AS co_entidade,
        COALESCE(c.no_entidade, e.no_entidade)::text AS no_entidade,
        COALESCE(c.co_municipio, e.co_municipio)::bigint AS co_municipio,
        COALESCE(c.no_municipio, e.no_municipio)::text AS no_municipio,
        COALESCE(c.sg_uf, e.sg_uf)::text AS sg_uf,
        COALESCE(c.tp_dependencia, e.tp_dependencia)::integer AS tp_dependencia,
        COALESCE(c.tp_localizacao, e.tp_localizacao)::integer AS tp_localizacao,
        COALESCE(c.tp_situacao_funcionamento, e.tp_situacao_funcionamento)::integer AS tp_situacao_funcionamento,
        c.ultimo_ano_censo,
        CASE
            WHEN COALESCE(c.tp_situacao_funcionamento, e.tp_situacao_funcionamento) = 1 THEN TRUE
            ELSE FALSE
        END AS escola_ativa
    FROM censo_recente c
    FULL OUTER JOIN escolas e
      ON e.co_entidade = c.co_entidade
    WHERE COALESCE(c.co_entidade, e.co_entidade) IS NOT NULL
),
inse_recente_bruto AS (
    SELECT
        i.co_entidade::bigint AS co_entidade,
        i.ano::integer AS ano,
        COALESCE(
            i.dados_json->>'INSE_VALOR',
            i.dados_json->>'NU_INSE',
            i.dados_json->>'PC_NIVEL_SOCIO_ECONOMICO',
            i.dados_json->>'NIVEL_SOCIO_ECONOMICO'
        ) AS inse_valor_raw,
        COALESCE(
            i.dados_json->>'INSE_GRUPO',
            i.dados_json->>'NU_GRUPO_INSE',
            i.dados_json->>'CO_GRUPO_INSE'
        ) AS inse_grupo_raw,
        ROW_NUMBER() OVER (PARTITION BY i.co_entidade ORDER BY i.ano DESC) AS rn
    FROM inse_escola i
),
inse_recente AS (
    SELECT
        co_entidade,
        ano AS inse_ano,
        CASE
            WHEN regexp_replace(replace(inse_valor_raw, ',', '.'), '[^0-9.-]', '', 'g') ~ '^-?[0-9]+(\.[0-9]+)?$'
            THEN regexp_replace(replace(inse_valor_raw, ',', '.'), '[^0-9.-]', '', 'g')::numeric
            ELSE NULL
        END AS inse_valor,
        CASE
            WHEN regexp_replace(inse_grupo_raw, '[^0-9-]', '', 'g') ~ '^-?[0-9]+$'
            THEN regexp_replace(inse_grupo_raw, '[^0-9-]', '', 'g')::integer
            ELSE NULL
        END AS inse_grupo
    FROM inse_recente_bruto
    WHERE rn = 1
),
ideb_recente AS (
    SELECT DISTINCT ON (co_entidade, etapa)
        co_entidade,
        etapa,
        ano,
        ideb
    FROM vw_escola_ideb
    ORDER BY co_entidade, etapa, ano DESC
),
ideb_pivot AS (
    SELECT
        co_entidade,
        MAX(CASE WHEN etapa = 'anos_iniciais' THEN ano END) AS ideb_ai_ano,
        MAX(CASE WHEN etapa = 'anos_iniciais' THEN ideb END) AS ideb_ai_recente,
        MAX(CASE WHEN etapa = 'anos_finais' THEN ano END) AS ideb_af_ano,
        MAX(CASE WHEN etapa = 'anos_finais' THEN ideb END) AS ideb_af_recente,
        MAX(CASE WHEN etapa = 'ensino_medio' THEN ano END) AS ideb_em_ano,
        MAX(CASE WHEN etapa = 'ensino_medio' THEN ideb END) AS ideb_em_recente
    FROM ideb_recente
    GROUP BY co_entidade
),
flags AS (
    SELECT
        b.co_entidade,
        EXISTS (SELECT 1 FROM inse_escola x WHERE x.co_entidade = b.co_entidade) AS tem_inse,
        EXISTS (SELECT 1 FROM afd_escola x WHERE x.co_entidade = b.co_entidade) AS tem_afd,
        EXISTS (SELECT 1 FROM icg_escola x WHERE x.co_entidade = b.co_entidade) AS tem_icg,
        EXISTS (SELECT 1 FROM ied_escola x WHERE x.co_entidade = b.co_entidade) AS tem_ied,
        EXISTS (SELECT 1 FROM ird_escola x WHERE x.co_entidade = b.co_entidade) AS tem_ird,
        EXISTS (SELECT 1 FROM atu_escola x WHERE x.co_entidade = b.co_entidade) AS tem_atu,
        EXISTS (SELECT 1 FROM had_escola x WHERE x.co_entidade = b.co_entidade) AS tem_had,
        EXISTS (SELECT 1 FROM tdi_escola x WHERE x.co_entidade = b.co_entidade) AS tem_tdi,
        EXISTS (SELECT 1 FROM tnr_escola x WHERE x.co_entidade = b.co_entidade) AS tem_tnr,
        EXISTS (SELECT 1 FROM rendimento_escola x WHERE x.co_entidade = b.co_entidade) AS tem_rendimento
    FROM base_escolas b
)
SELECT
    b.co_entidade,
    b.no_entidade,
    b.co_municipio,
    b.no_municipio,
    b.sg_uf,
    b.tp_dependencia,
    b.tp_localizacao,
    b.tp_situacao_funcionamento,
    b.ultimo_ano_censo,
    b.escola_ativa,
    ir.inse_valor,
    ir.inse_grupo,
    ip.ideb_ai_ano,
    ip.ideb_ai_recente,
    ip.ideb_af_ano,
    ip.ideb_af_recente,
    ip.ideb_em_ano,
    ip.ideb_em_recente,
    f.tem_inse,
    f.tem_afd,
    f.tem_icg,
    f.tem_ied,
    f.tem_ird,
    f.tem_atu,
    f.tem_had,
    f.tem_tdi,
    f.tem_tnr,
    f.tem_rendimento
FROM base_escolas b
LEFT JOIN inse_recente ir
       ON ir.co_entidade = b.co_entidade
LEFT JOIN ideb_pivot ip
       ON ip.co_entidade = b.co_entidade
LEFT JOIN flags f
       ON f.co_entidade = b.co_entidade;

CREATE OR REPLACE VIEW vw_raiox_escola_publica AS
SELECT *
FROM vw_raiox_escola
WHERE tp_dependencia IN (1, 2, 3);

CREATE OR REPLACE VIEW vw_escolas_mapa AS
SELECT
    co_entidade,
    no_entidade,
    co_municipio,
    no_municipio,
    sg_uf,
    tp_dependencia,
    NULL::double precision AS latitude,
    NULL::double precision AS longitude,
    inse_valor,
    ideb_ai_recente,
    ideb_af_recente,
    ideb_em_recente
FROM vw_raiox_escola;

CREATE OR REPLACE VIEW vw_ufs AS
SELECT DISTINCT
    sg_uf AS uf
FROM vw_raiox_escola
WHERE sg_uf IS NOT NULL;

CREATE OR REPLACE VIEW vw_municipios AS
SELECT DISTINCT
    co_municipio,
    no_municipio,
    sg_uf AS uf
FROM vw_raiox_escola
WHERE co_municipio IS NOT NULL;

CREATE OR REPLACE VIEW vw_ufs_publicas AS
SELECT DISTINCT
    sg_uf AS uf
FROM vw_raiox_escola_publica
WHERE sg_uf IS NOT NULL;

CREATE OR REPLACE VIEW vw_municipios_publicos AS
SELECT DISTINCT
    co_municipio,
    no_municipio,
    sg_uf AS uf
FROM vw_raiox_escola_publica
WHERE co_municipio IS NOT NULL;

COMMIT;
