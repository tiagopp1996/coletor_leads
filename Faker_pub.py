from faker import Faker
from faker.providers import DynamicProvider
import time
import random
import json
import os
from datetime import datetime
import socket
from kafka import KafkaProducer
from kafka.net.sasl.oauth import AbstractTokenProvider
from aws_msk_iam_sasl_signer import MSKAuthTokenProvider

class MSKTokenProvider(AbstractTokenProvider):
    def token(self):
        token, _ = MSKAuthTokenProvider.generate_auth_token(REGION)
        return token

def cria_conn_kafka(BOOTSTRAP_SERVERS):

    tp = MSKTokenProvider()

    producer = KafkaProducer(
        bootstrap_servers=BOOTSTRAP_SERVERS,
        security_protocol="SASL_SSL",
        sasl_mechanism="OAUTHBEARER",
        sasl_oauth_token_provider=tp,
        client_id=socket.gethostname(),

        value_serializer=lambda value: json.dumps(
            value,
            ensure_ascii=False,
            default=converter_json
        ).encode("utf-8"),

        key_serializer=lambda key: (
            key.encode("utf-8") if key else None
        ),

        # Tempo para estabelecer a conexão inicial
        bootstrap_timeout_ms=60000,

        # Confirmação de entrega
        acks="all",
        retries=5
    )

    print("Conexão com Kafka inicializada!")

    return producer

def enviar_kfk(dados, TOPIC):
    try:
        if isinstance(dados, str):
            dados = json.loads(dados)

        future = producer.send(
            TOPIC,
            key=dados.get("sessao"),
            value=dados
        )

        resultado = future.get(timeout=30)

        print(
            f"Mensagem enviada: "
            f"topic={resultado.topic}, "
            f"partition={resultado.partition}, "
            f"offset={resultado.offset}"
        )

    except Exception as erro:
        print(f"Erro ao enviar mensagem para {TOPIC}: {erro}")
        raise

def converter_json(obj):
    if isinstance(obj, datetime):
        return obj.isoformat()
    return str(obj)

def exportar_json(dados, arquivo):

    with open(arquivo, "a", encoding="utf-8") as f:

        json.dump(
            dados,
            f,
            ensure_ascii=False,
            default=converter_json
        )

        f.write("\n")

def cria_menus(fake):
    Menus_nav = DynamicProvider(
        provider_name="menu_nav",
        elements=[
            "Extrato Conta",
            "Extrato Cartao",
            "PIX",
            "Gestão do Cartão de Crédito"
            "Financiamento Imobliario",
            "Consorcio",
            "Emprestimo Pessoal",
            "Financiamento Veicular",
            "Contato com o Gerente",
            "Capitalização",
            "Seguro de Vida",
            "Seguro Veicular",
            "Seguro Habitacional",
            "Seguro de Cartão",
            "Investimentos",
            "Beneficios da Conta",
            "Gestão de Veiculos"
        ]
    )
    fake.add_provider(Menus_nav)

    return fake

def pub_logins(fake):
    login = {
        #Start Login
        'sessao' : fake.unique.uuid4(),
        'id_login' : fake.unique.safe_email(),
        'nome' : fake.name(),
        'Profissao' : fake.job(),
        'start_session_timestamp' : fake.date_time(),
        'ip_v4' : fake.ipv4(),
        'ip_v6' : fake.ipv6(),
        'mac_address' : fake.mac_address(),
        'latitude_login' : fake.latitude(),
        'longitude_login' :fake.longitude()
    }
    return login

def pub_navegacao(fake,login):
    navegacao = {
        'id_nav_trans' : fake.unique.uuid4(),
        'id_login' : login['id_login'],
        'sessao' : login['sessao'],                
        'navegacao_menu' : fake.menu_nav(),
        'nav_timestamp' : fake.date_time(),
        'latitude_nav' : fake.latitude(),
        'longitude_nav' :fake.longitude()
    }    
    return navegacao

fake = Faker("pt_BR")
fake = cria_menus(fake)

# #Define Paths
# DIRETORIO = "C:\\Users\\tiago\\Desktop\\Migracao\\POS\\XPe - Engenharia de Dados\\TCC\\dado_json"
# ARQUIVO_LOGIN = os.path.join(DIRETORIO, "tb_login.json")
# ARQUIVO_NAVEGACAO = os.path.join(DIRETORIO, "tb_navegacao.json")

# Configurações do MSK
REGION = "us-east-1"
BOOTSTRAP_SERVERS = [
    "boot-jpn6tcjq.c3.kafka-serverless.us-east-1.amazonaws.com:9098"
]
TOPIC_LOG = "logins"
TOPIC_NAV = "navegacao"

producer = cria_conn_kafka(BOOTSTRAP_SERVERS)
    

try:
    while True:
        print("Executando... Pressione Ctrl + C para parar.")
        login = pub_logins(fake)

        # exportar_json(login, ARQUIVO_LOGIN)
        enviar_kfk(login,TOPIC_LOG)

        print("Tabela de Login:\n",login)
        r_nav = random.randint(1,7)
        print(r_nav)

        for q in range(0, r_nav):
            navegacao = pub_navegacao(fake,login)
            # exportar_json(navegacao, ARQUIVO_NAVEGACAO)
            enviar_kfk(navegacao,TOPIC_NAV)
            time.sleep(10)
            print("Navegacao:\n",navegacao)
        time.sleep(60)

except KeyboardInterrupt:
    print("\nExecução interrompida pelo usuário (Ctrl + C).")        



