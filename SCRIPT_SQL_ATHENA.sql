
-- ==========================================
-- 1. CRIACAO DO DATABASE
-- ==========================================

CREATE DATABASE IF NOT EXISTS app_garantia;


-- ==========================================
-- 2. TABELA LOGIN
-- ==========================================

CREATE EXTERNAL TABLE IF NOT EXISTS app_garantia.tb_login (

    sessao STRING,
    id_login STRING,
    nome STRING,
    profissao STRING,
    start_session_timestamp TIMESTAMP,
    ip_v4 STRING,
    ip_v6 STRING,
    mac_address STRING,
    latitude_login DECIMAL(10,7),
    longitude_login DECIMAL(10,7)

)
PARTITIONED BY (
    anomesdia int,
    hora_minuto int
)

STORED AS PARQUET

LOCATION 's3://937525203292-sor-937525203292-us-east-1-an/app-garantia/tb_login/'

TBLPROPERTIES (
    'parquet.compression' = 'SNAPPY'
);


-- ==========================================
-- 3. TABELA NAVEGACAO
-- ==========================================

CREATE EXTERNAL TABLE IF NOT EXISTS app_garantia.tb_navegacao (

    id_nav_trans STRING,
    id_login STRING,
    sessao STRING,
    navegacao_menu STRING,
    nav_timestamp TIMESTAMP,
    latitude_nav DECIMAL(10,7),
    longitude_nav DECIMAL(10,7)

)
PARTITIONED BY (
    anomesdia int,
    hora_minuto int
)

STORED AS PARQUET

LOCATION 's3://937525203292-sor-937525203292-us-east-1-an/app-garantia/tb_navegacao/'

TBLPROPERTIES (
    'parquet.compression' = 'SNAPPY'
);
