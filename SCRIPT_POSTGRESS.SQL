
-- ==========================================
-- 1. CRIAÇÃO DO BANCO DE DADOS
-- ==========================================

CREATE DATABASE app_garantia;

CREATE SCHEMA IF NOT EXISTS schm;


CREATE TABLE APP_GARANTIA.schm.tb_login (
    sessao VARCHAR(32) NOT NULL,
    id_login VARCHAR(255) NOT NULL,
    nome VARCHAR(255),
    profissao VARCHAR(150),
    start_session_timestamp TIMESTAMP NOT NULL,
    ip_v4 VARCHAR(15),
    ip_v6 VARCHAR(39),
    mac_address VARCHAR(12),
    latitude_login NUMERIC(10, 7),
    longitude_login NUMERIC(10, 7),

    CONSTRAINT PK_tb_login PRIMARY KEY (sessao, id_login)
);

-- ==========================================
-- TABELA 2: NAVEGACAO
-- ==========================================

CREATE TABLE APP_GARANTIA.schm.tb_navegacao (
    id_nav_trans VARCHAR(32) PRIMARY KEY,
    id_login VARCHAR(255) NOT NULL,
	sessao VARCHAR(32) NOT NULL,
    navegacao_menu VARCHAR(150) NOT NULL,
    nav_timestamp TIMESTAMP NOT NULL,           
    latitude_nav NUMERIC(10, 7),
    longitude_nav NUMERIC(10, 7),

    CONSTRAINT fk_navegacao_login
        FOREIGN KEY (sessao, id_login) 
        REFERENCES APP_GARANTIA.dbo.tb_login (sessao, id_login)
);



