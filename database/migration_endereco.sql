-- Migração: adicionar campos de endereço e tabela de cursos técnicos
-- Rode no Railway:
--   psql "postgresql://postgres:...@hopper.proxy.rlwy.net:24550/railway" -f database/migration_endereco.sql

-- Campos de endereço em censo_escola_historico
ALTER TABLE censo_escola_historico
    ADD COLUMN IF NOT EXISTS ds_endereco  TEXT,
    ADD COLUMN IF NOT EXISTS no_bairro    TEXT,
    ADD COLUMN IF NOT EXISTS co_cep       VARCHAR(8),
    ADD COLUMN IF NOT EXISTS nu_ddd       VARCHAR(4),
    ADD COLUMN IF NOT EXISTS nu_telefone  VARCHAR(20);

-- Tabela de cursos técnicos
CREATE TABLE IF NOT EXISTS curso_tecnico_escola (
    id          SERIAL PRIMARY KEY,
    ano         INTEGER NOT NULL,
    co_entidade INTEGER NOT NULL,
    dados_json  JSONB,
    UNIQUE (ano, co_entidade)
);

CREATE INDEX IF NOT EXISTS idx_curso_tecnico_co_entidade ON curso_tecnico_escola(co_entidade);
CREATE INDEX IF NOT EXISTS idx_curso_tecnico_ano         ON curso_tecnico_escola(ano);
