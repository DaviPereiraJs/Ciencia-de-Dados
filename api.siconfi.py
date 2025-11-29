
import requests
import pandas as pd

# URL base da API do Siconfi para a coleta de dados de extrato de entregas
base_url = "https://apidatalake.tesouro.gov.br/ords/siconfi/tt//rreo?"

# Cria lista com os códigos do IGBE dos entes e períodos de interesse
anos = [2023, 2024, 2025]
entes = [21, 22, 23, 24, 25, 26, 27, 28, 29]
periodos = [1, 2, 4, 5, 6]
tipo_d = ["RREO"]
anexos = ["RREO-Anexo 01"]
esfera = ["E"]


# Criar lista com os registros das consultas individuais
registros = []

# Laço para as consultas individuais na API
for ano in anos:
    for ente in entes:
        for periodo in periodos:
            for anexo in anexos:
                print(f"Consultando: anexo={anexo}, ano={ano}, periodo={periodo}, ente={ente}")
                # Parâmetro da consulta da API
                params = { 
                    "an_exercicio": ano,
                    "nr_periodo": periodo,
                    "co_tipo_demonstrativo": tipo_d,
                    #"no_anexo": anexo,
                    #"co_esfera": esfera,
                    "id_ente": ente
                }

                # Requisição
                response = requests.get(base_url, params)

                # tratar a limitação
                # quebra de dados

                # Extrai os dados retornados e transforma em dataframe
                data = response.json()
                df = pd.DataFrame(data['items'])

                print(f" -> {len(df)} registros coletados.")
                    
                # Adiciona um dataframe em uma lista de dataframes
                registros.append(df)

# Concatena todos os dataframes da lista em um único
df_all = pd.concat(registros)

# Salva o dataframe final em um arquivo único
df_all.to_csv("Extrato_BVG_CE_23-25.csv", index=False)