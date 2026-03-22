-- Migração: adicionar campos de endereço à tabela censo_escola_historico
-- Rode no Railway: psql "postgresql://postgres:...@hopper.proxy.rlwy.net:24550/railway" -f database/migration_endereco.sql

ALTER TABLE censo_escola_historico
    ADD COLUMN IF NOT EXISTS ds_endereco  TEXT,
    ADD COLUMN IF NOT EXISTS no_bairro    TEXT,
    ADD COLUMN IF NOT EXISTS co_cep       VARCHAR(8),
    ADD COLUMN IF NOT EXISTS nu_ddd       VARCHAR(4),
    ADD COLUMN IF NOT EXISTS nu_telefone  VARCHAR(20);
