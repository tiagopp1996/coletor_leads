from faker import Faker
from faker.providers import DynamicProvider
import time
import random
import json
import os
from datetime import datetime

# ==========================================
# FUNCAO DE CONVERSAO JSON
# ==========================================

def converter_json(obj):
    if isinstance(obj, datetime):
        return obj.isoformat()
    return str(obj)


# ==========================================
# FUNCAO PARA EXPORTAR JSON
# ==========================================

def exportar_json(dados, arquivo):

    with open(arquivo, "a", encoding="utf-8") as f:

        json.dump(
            dados,
            f,
            ensure_ascii=False,
            default=converter_json
        )

        f.write("\n")



fake = Faker("pt_BR")

DIRETORIO = "C:\\Users\\tiago\\Desktop\\Migracao\\POS\\XPe - Engenharia de Dados\\TCC\\dado_json"

ARQUIVO_LOGIN = os.path.join(DIRETORIO, "tb_login.json")
ARQUIVO_NAVEGACAO = os.path.join(DIRETORIO, "tb_navegacao.json")


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

try:
    while True:
        print("Executando... Pressione Ctrl + C para parar.")

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
        exportar_json(login, ARQUIVO_LOGIN)

        print("Tabela de Login:\n",login)
        r_nav = random.randint(1,7)
        print(r_nav)

        for q in range(0, r_nav):
            navegacao = {
                'id_nav_trans' : fake.unique.uuid4(),
                'id_login' : login['id_login'],
                'sessao' : login['sessao'],                
                'navegacao_menu' : fake.menu_nav(),
                'nav_timestamp' : fake.date_time(),
                'latitude_nav' : fake.latitude(),
                'longitude_nav' :fake.longitude()
            }
            exportar_json(navegacao, ARQUIVO_NAVEGACAO)
            time.sleep(10)
            print("Navegacao:\n",navegacao)
        time.sleep(60)

except KeyboardInterrupt:
    print("\nExecução interrompida pelo usuário (Ctrl + C).")        



