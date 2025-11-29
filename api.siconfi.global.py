import requests
import pandas as pd
import time

# ==========================================================
# CONFIGURAÇÕES GERAIS
# ==========================================================

# Escolha o endpoint desejado (exemplos abaixo)
# base_url = "https://apidatalake.tesouro.gov.br/ords/siconfi/tt/rreo?"
# base_url = "https://apidatalake.tesouro.gov.br/ords/siconfi/tt/rgf?"
# base_url = "https://apidatalake.tesouro.gov.br/ords/siconfi/tt/dca?"
# base_url = "https://apidatalake.tesouro.gov.br/ords/siconfi/tt/matriz?"
base_url = "https://apidatalake.tesouro.gov.br/ords/siconfi/tt/rreo?"  # padrão

# ==========================================================
# PARÂMETROS DISPONÍVEIS
# (Descomente apenas os que for usar)
# ==========================================================

params_template = {
    # -------- FILTROS COMUNS --------
    # "id_ente": 2304400,          # Código IBGE do ente
    # "an_exercicio": 2024,        # Ano
    # "nr_periodo": 1,             # Período (1,2,3...)
    # "co_esfera": "E",            # Esfera: E, M, U
    # "co_poder": "E",             # Poder: E, L, J
    # "co_tipo_demonstrativo": "RREO",  # Tipo demonstrativo
    # "no_anexo": "RREO-Anexo 01", # Nome do anexo

    # -------- FILTROS ESPECIAIS DO SICONFI --------
    # "no_ente": "FORTALEZA",           # Nome do ente
    # "dt_ini_entrega": "2024-01-01",   # Data inicial de entrega
    # "dt_fim_entrega": "2024-12-31",   # Data final
    # "tp_resultado": "A",              # Tipo de resultado
    # "texto_busca": "saúde",           # Busca textual
}

# ==========================================================
# FUNÇÃO DE BUSCA COM PAGINAÇÃO
# ==========================================================

def consultar_api(base_url, params):
    print("\n🔍 Iniciando consulta na API...")
    page = 1
    todos_registros = []

    while True:
        print(f" → Página {page}...")

        params_page = params.copy()
        params_page["page"] = page

        r = requests.get(base_url, params=params_page)

        if r.status_code != 200:
            print(" ❌ Erro na requisição:", r.status_code)
            break

        data = r.json()

        # Se não houver itens, parou
        if "items" not in data or len(data["items"]) == 0:
            print(" ✔ Todas as páginas foram coletadas.")
            break

        df_page = pd.DataFrame(data["items"])
        todos_registros.append(df_page)

        page += 1
        time.sleep(0.3)

    # Concatena tudo
    if len(todos_registros) == 0:
        print("⚠ Nenhum dado retornado.")
        return pd.DataFrame()

    df_final = pd.concat(todos_registros, ignore_index=True)
    print(f"\n📊 Total coletado: {len(df_final)} registros.")
    return df_final

# ==========================================================
# EXECUÇÃO
# ==========================================================

df = consultar_api(base_url, params_template)

# Salvar resultado
df.to_csv("resultado_siconfi.csv", index=False)
print("\n💾 Arquivo 'resultado_siconfi.csv' salvo com sucesso!")

