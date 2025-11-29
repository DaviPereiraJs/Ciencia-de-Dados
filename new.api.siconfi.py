import requests
import pandas as pd

# URL base da API do Siconfi para a coleta de dados
base_url = "https://apidatalake.tesouro.gov.br/ords/siconfi/tt/rreo"

# Parâmetros desejados
anos = [2023, 2024, 2025]
entes = [21, 22, 23, 24, 25, 26, 27, 28, 29]
periodos = [1, 2, 4, 5, 6]
tipos = ["RREO"]
anexos = ["RREO-Anexo 01"]
esferas = ["E"]

# Lista final com todos os DataFrames
registros = []

# Função que coleta TODAS as páginas (quebra dos 50 mil)
def coletar_paginas(url, params):
    dados_completos = []
    while url:
        resposta = requests.get(url, params=params)
        params = None  # Só envia params na 1ª chamada

        if resposta.status_code != 200:
            print("Erro HTTP:", resposta.text)
            break

        data = resposta.json()

        # Se não houver items, acaba
        if "items" not in data or len(data["items"]) == 0:
            break

        dados_completos.extend(data["items"])

        # Verifica se existe próxima página
        next_link = None
        for link in data.get("links", []):
            if link.get("rel") == "next":
                next_link = link.get("href")

        if next_link:
            url = next_link
        else:
            url = None

    return pd.DataFrame(dados_completos)


# ----------- LOOP PRINCIPAL ------------
for ano in anos:
    for ente in entes:
        for periodo in periodos:
            for anexo in anexos:
                for tipo in tipos:
                    for esfera in esferas:

                        print(f"\n Consultando: ano={ano}, ente={ente}, período={periodo}, anexo={anexo}")

                        params = {
                            "an_exercicio": ano,
                            "nr_periodo": periodo,
                            "co_tipo_demonstrativo": tipo,
                            #"no_anexo": anexo,
                            #"co_esfera": esfera,
                            "id_ente": ente
                        }

                        df = coletar_paginas(base_url, params)

                        print(f"   → {len(df)} registros coletados (somando todas páginas)")

                        if not df.empty:
                            registros.append(df)


# Concatena todos os resultados
df_final = pd.concat(registros, ignore_index=True)

# Salva no CSV
df_final.to_csv("Extrato_BVG_CE_23-25.csv", index=False)

print("\n✅ Arquivo final gerado: Extrato_BVG_CE_23-25.csv")
