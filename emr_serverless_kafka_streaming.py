import copy
import json
import logging
import os
import time
from datetime import datetime, timedelta, timezone

import boto3
from pyspark.sql import SparkSession, functions as F, types as T
from pyspark.sql.streaming import StreamingQueryListener

# ---------------------------------------------------------------------------
# Configuração
# ---------------------------------------------------------------------------

AWS_REGION = os.getenv("AWS_REGION", "us-east-1")
SECRET_NAME = os.getenv("RDS_SECRET_NAME", "leads/rds/postgres")
RDS_DATABASE = os.getenv("RDS_DATABASE", "app_garantia")

KAFKA_SERVERS = os.getenv(
    "KAFKA_BOOTSTRAP_SERVERS",
    "boot-oyv8aapg.c3.kafka-serverless.us-east-1.amazonaws.com:9098",
)

S3_BUCKET = "s3://937525203292-sor-937525203292-us-east-1-an"
CHECKPOINT_BASE = os.getenv(
    "CHECKPOINT_BASE", "s3://937525203292-temp/_checkpoints/app-garantia/"
)

TIMEZONE = "America/Sao_Paulo"  # UTC-3 (sem horário de verão desde 2019)
BRT = timezone(timedelta(hours=-3))

RDS_TRIGGER = os.getenv("RDS_TRIGGER", "5 seconds")
S3_TRIGGER = os.getenv("S3_TRIGGER", "1 hour")

# navegacao tem FK para login: se o login ainda não chegou, tenta de novo.
WRITE_RETRIES = int(os.getenv("WRITE_RETRIES", "12"))
RETRY_SECONDS = int(os.getenv("RETRY_SECONDS", "5"))

logging.basicConfig(
    level=os.getenv("LOG_LEVEL", "INFO"),
    format="%(asctime)s %(levelname)s %(name)s - %(message)s",
)
logger = logging.getLogger("app-garantia-streaming")

GLUE_DATABASE = os.getenv("GLUE_DATABASE", "app-garantia")
# Quantos dias para trás varrer no S3 a cada sincronização.
PARTITION_LOOKBACK_DAYS = int(os.getenv("PARTITION_LOOKBACK_DAYS", "2"))

# nome da query do S3 -> (tabela no Glue, path onde o sink grava)
PARTITION_TARGETS = {
    "logins_to_s3": ("tb_login", S3_BUCKET + "/app-garantia/tb_login/"),
    "navegacao_to_s3": ("tb_navegacao", S3_BUCKET + "/app-garantia/tb_navegacao/"),
}

LOGIN_SCHEMA = T.StructType([
    T.StructField("sessao", T.StringType()),
    T.StructField("id_login", T.StringType()),
    T.StructField("nome", T.StringType()),
    T.StructField("profissao", T.StringType()),
    T.StructField("start_session_timestamp", T.StringType()),
    T.StructField("ip_v4", T.StringType()),
    T.StructField("ip_v6", T.StringType()),
    T.StructField("mac_address", T.StringType()),
    T.StructField("latitude_login", T.StringType()),
    T.StructField("longitude_login", T.StringType()),
])

NAVEGACAO_SCHEMA = T.StructType([
    T.StructField("id_nav_trans", T.StringType()),
    T.StructField("id_login", T.StringType()),
    T.StructField("sessao", T.StringType()),
    T.StructField("navegacao_menu", T.StringType()),
    T.StructField("nav_timestamp", T.StringType()),
    T.StructField("latitude_nav", T.StringType()),
    T.StructField("longitude_nav", T.StringType()),
])

def config_spark():
    spark = (
        SparkSession.builder
        .appName("app-garantia-kafka-to-rds-s3")
        .config("spark.sql.session.timeZone", TIMEZONE)   # UTC-3
        # O writer JDBC converte timestamp pelo fuso da JVM: alinha os executors.
        .config("spark.executor.extraJavaOptions", "-Duser.timezone=" + TIMEZONE)
        .config("spark.sql.ansi.enabled", "false")   # cast inválido -> NULL
        .config("spark.scheduler.mode", "FAIR")     # RDS não espera o job do S3
        .getOrCreate()
    )
    spark.streams.addListener(PartitionSyncListener())  # sincroniza com o Glue
    return spark

# ---------------------------------------------------------------------------
# Credenciais
# ---------------------------------------------------------------------------

def get_rds_config():
    client = boto3.client("secretsmanager", region_name=AWS_REGION)
    s = json.loads(client.get_secret_value(SecretId=SECRET_NAME)["SecretString"])
    return {
        "url": "jdbc:postgresql://{}:{}/{}?sslmode=require"
               "&reWriteBatchedInserts=true".format(
                   s["host"], s["port"], RDS_DATABASE),
        "user": s["username"],
        "password": s["password"],
    }


# ---------------------------------------------------------------------------
# Leitura + normalização
# ---------------------------------------------------------------------------

def read_kafka(spark, topic, schema):
    """Lê um tópico e devolve um DataFrame streaming com o JSON em colunas."""
    max_offsets = os.getenv("MAX_OFFSETS_PER_TRIGGER")  # opcional
    reader = (
        spark.readStream.format("kafka")
        .option("kafka.bootstrap.servers", KAFKA_SERVERS)
        .option("subscribe", topic)
        .option("startingOffsets", os.getenv("KAFKA_STARTING_OFFSETS", "earliest"))
        .option("failOnDataLoss", "false")
        # MSK Serverless (porta 9098): SASL_SSL + IAM
        .option("kafka.security.protocol", "SASL_SSL")
        .option("kafka.sasl.mechanism", "AWS_MSK_IAM")
        .option("kafka.sasl.jaas.config",
                "software.amazon.msk.auth.iam.IAMLoginModule required;")
        .option("kafka.sasl.client.callback.handler.class",
                "software.amazon.msk.auth.iam.IAMClientCallbackHandler")
    )
    if max_offsets:
        reader = reader.option("maxOffsetsPerTrigger", max_offsets)

    return (
        reader.load()
        .select(F.from_json(F.col("value").cast("string"), schema).alias("d"))
        .select("d.*")
    )


def dec(col):
    return F.col(col).cast(T.DecimalType(10, 7))


def normalize_login(df):
    return (
        df.select(
            F.regexp_replace("sessao", "-", "").alias("sessao"),
            "id_login",
            "nome",
            "profissao",
            F.to_timestamp("start_session_timestamp").alias(
                "start_session_timestamp"),
            "ip_v4",
            "ip_v6",
            F.regexp_replace("mac_address", "[:.\\-]", "").alias("mac_address"),
            dec("latitude_login").alias("latitude_login"),
            dec("longitude_login").alias("longitude_login"),
        )
        .dropna(subset=["sessao", "id_login", "start_session_timestamp"])
    )


def normalize_navegacao(df):
    return (
        df.select(
            F.regexp_replace("id_nav_trans", "-", "").alias("id_nav_trans"),
            "id_login",
            F.regexp_replace("sessao", "-", "").alias("sessao"),
            "navegacao_menu",
            F.to_timestamp("nav_timestamp").alias("nav_timestamp"),
            dec("latitude_nav").alias("latitude_nav"),
            dec("longitude_nav").alias("longitude_nav"),
        )
        .dropna(subset=["id_nav_trans", "id_login", "sessao",
                        "navegacao_menu", "nav_timestamp"])
    )


def add_partitions(df):
    """anomesdia (yyyyMMdd) e hora_minuto (HHmm), como int, pela hora de
    ingestão no fuso da sessão (America/Sao_Paulo)."""
    now = F.current_timestamp()
    return (
        df.withColumn("anomesdia", F.date_format(now, "yyyyMMdd").cast("int"))
        .withColumn("hora_minuto", F.date_format(now, "HHmm").cast("int"))
    )


# ---------------------------------------------------------------------------
# Partições no Glue Data Catalog
# ---------------------------------------------------------------------------
# O sink Parquet só grava arquivos em anomesdia=.../hora_minuto=.... Depois de
# cada batch do S3, create_partitions() descobre as pastas novas e as registra
# no Glue. Não altera a escrita: apenas lê o S3 e chama a API do Glue.
def _split_s3(path):
    bucket, _, prefix = path.replace("s3://", "", 1).partition("/")
    return bucket, prefix


def _list_subdirs(s3, bucket, prefix):
    paginator = s3.get_paginator("list_objects_v2")
    for page in paginator.paginate(Bucket=bucket, Prefix=prefix, Delimiter="/"):
        for common in page.get("CommonPrefixes", []):
            yield common["Prefix"]


def _partition_value(prefix, key):
    """'a/b/anomesdia=20261120/' + 'anomesdia' -> '20261120' (ou None)."""
    name = prefix.rstrip("/").rsplit("/", 1)[-1]
    k, _, value = name.partition("=")
    return value if k == key and value.isdigit() else None


def create_partitions(table, base_path, database=GLUE_DATABASE,
                      lookback_days=PARTITION_LOOKBACK_DAYS):
    """
    Registra no Glue as partições (anomesdia, hora_minuto) que existem no S3 e
    ainda não estão no catálogo. Idempotente. Retorna quantas foram criadas.
    """
    s3 = boto3.client("s3", region_name=AWS_REGION)
    glue = boto3.client("glue", region_name=AWS_REGION)
    bucket, prefix = _split_s3(base_path)
    first_day = int(
        (datetime.now(BRT) - timedelta(days=lookback_days)).strftime("%Y%m%d")
    )

    found = []  # ([anomesdia, hora_minuto], location)
    for day_prefix in _list_subdirs(s3, bucket, prefix):
        day = _partition_value(day_prefix, "anomesdia")
        if day is None or int(day) < first_day:
            continue
        for hm_prefix in _list_subdirs(s3, bucket, day_prefix):
            hm = _partition_value(hm_prefix, "hora_minuto")
            if hm is not None:
                found.append(
                    ([day, hm], "s3://{}/{}".format(bucket, hm_prefix.rstrip("/")))
                )
    if not found:
        return 0

    storage = glue.get_table(DatabaseName=database, Name=table)[
        "Table"]["StorageDescriptor"]
    inputs = []
    for values, location in found:
        descriptor = copy.deepcopy(storage)
        descriptor["Location"] = location  # onde o sink realmente gravou
        inputs.append({"Values": values, "StorageDescriptor": descriptor})

    created = 0
    for i in range(0, len(inputs), 100):  # limite da API: 100 por chamada
        chunk = inputs[i:i + 100]
        response = glue.batch_create_partition(
            DatabaseName=database, TableName=table, PartitionInputList=chunk
        )
        errors = response.get("Errors", [])
        for err in errors:
            code = err["ErrorDetail"]["ErrorCode"]
            if code != "AlreadyExistsException":
                logger.warning("Glue %s.%s partição %s: %s",
                               database, table, err.get("PartitionValues"), code)
        created += len(chunk) - len(errors)
    return created


class PartitionSyncListener(StreamingQueryListener):
    """Chama create_partitions() ao fim de cada batch das queries do S3."""

    def onQueryStarted(self, event):
        pass

    def onQueryProgress(self, event):
        progress = event.progress
        target = PARTITION_TARGETS.get(progress.name)
        if target is None or not progress.numInputRows:
            return
        try:
            created = create_partitions(*target)
            logger.info("Glue %s: %s partição(ões) nova(s)", target[0], created)
        except Exception:
            # Nunca derruba a query: a próxima sincronização repete.
            logger.exception("Falha ao sincronizar partições de %s", target[0])

    def onQueryIdle(self, event):
        pass

    def onQueryTerminated(self, event):
        pass


# ---------------------------------------------------------------------------
# Escrita no RDS
# ---------------------------------------------------------------------------
# Não existe writeStream.format("jdbc") no Spark. O equivalente nativo é
# foreachBatch + write.format("jdbc"): cada micro-batch é inserido direto na
# tabela final. coalesce(1) = uma conexão = uma transação por batch, então a
# nova tentativa (ex.: login ainda não existe para a FK) não grava pela metade.

def write_jdbc(db, table, keys):
    def _write(batch_df, batch_id):
        df = batch_df.dropDuplicates(keys).coalesce(1)
        for attempt in range(1, WRITE_RETRIES + 1):
            try:
                (
                    df.write.format("jdbc")
                    .option("url", db["url"])
                    .option("dbtable", table)
                    .option("user", db["user"])
                    .option("password", db["password"])
                    .option("driver", "org.postgresql.Driver")
                    .option("batchsize", "1000")
                    .mode("append")
                    .save()
                )
                logger.info("RDS %s batch_id=%s ok", table, batch_id)
                return
            except Exception:
                if attempt == WRITE_RETRIES:
                    raise
                logger.warning("RDS %s batch_id=%s falhou (%s/%s); retry em %ss",
                               table, batch_id, attempt, WRITE_RETRIES,
                               RETRY_SECONDS, exc_info=True)
                time.sleep(RETRY_SECONDS)
    return _write


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def main():
    spark = config_spark()
    db = get_rds_config()

    #Coleta dos Tópicos
    df_logins = normalize_login(read_kafka(spark, "logins", LOGIN_SCHEMA))
    df_navegacao = normalize_navegacao(read_kafka(spark, "navegacao", NAVEGACAO_SCHEMA))

    #print em tela do topico para debug
    df_logins.writeStream.queryName("debug_logins").format("console").option("numRows", 5).option("truncate", "false").trigger(processingTime="10 seconds").start()
    df_navegacao.writeStream.queryName("debug_nav").format("console").option("numRows", 5).option("truncate", "false").trigger(processingTime="10 seconds").start()

    # Escrita PostgreSQL em tempo real ----
    df_logins.writeStream.queryName("logins_to_rds")\
        .foreachBatch(write_jdbc(db, "tb_login", ["sessao", "id_login"])).option("checkpointLocation", CHECKPOINT_BASE + "login/rds/")\
        .trigger(processingTime=RDS_TRIGGER).start()
    
    df_navegacao.writeStream.queryName("navegacao_to_rds")\
        .foreachBatch(write_jdbc(db, "tb_navegacao", ["id_nav_trans"])).option("checkpointLocation", CHECKPOINT_BASE + "tb_navegacao/rds/")\
        .trigger(processingTime=RDS_TRIGGER).start()
    

    # Escrita em Parquet no S3 a cada 1 hora ----
    add_partitions(df_logins).writeStream.queryName("logins_to_s3").format("parquet")\
        .option("path", S3_BUCKET + "/app-garantia/tb_login/").option("checkpointLocation", CHECKPOINT_BASE + "tb_login/s3/")\
        .partitionBy("anomesdia", "hora_minuto").trigger(processingTime=S3_TRIGGER).start()
    
    add_partitions(df_navegacao).writeStream.queryName("navegacao_to_s3").format("parquet")\
        .option("path", S3_BUCKET + "/app-garantia/tb_navegacao/").option("checkpointLocation", CHECKPOINT_BASE + "tb_navegacao/s3/")\
        .partitionBy("anomesdia", "hora_minuto").trigger(processingTime=S3_TRIGGER).start()

    # Mantém a aplicação rodando para todas as queries
    spark.streams.awaitAnyTermination()


if __name__ == "__main__":
    main()
