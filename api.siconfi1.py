import requests
import pandas as pd

# URL base da API do Siconfi para a coleta de dados de extrato de entregas
base_url = "https://apidatalake.tesouro.gov.br/ords/siconfi/tt//rgf"

# Listas de parâmetros
anos = [2024, 2025]
entes = [21, 22, 23, 24, 25, 26, 27, 28, 29]
periodos = [1, 2, 4, 5, 6]
tipos_demonstrativo = ["RGF"]
anexos = ["RREO-Anexo 01"]
esferas = ["E"]
periodicidades = ["S"]
poderes = ["E"]

# Lista para armazenar todos os dataframes
registros = []

# Loop principal
for ano in anos:
    for ente in entes:
        for periodo in periodos:
            for anexo in anexos:
                for tipo in tipos_demonstrativo:
                    for esfera in esferas:
                        for periodicidade in periodicidades:
                            for poder in poderes:

                                print(
                                    f"Consultando: ano={ano}, ente={ente}, periodo={periodo}, "
                                    f"anexo={anexo}, tipo={tipo}, esfera={esfera}, "
                                    f"periodicidade={periodicidade}, poder={poder}"
                                )

                                # Parâmetros da requisição
                                params = {
                                    "an_exercicio": ano,
                                    "nr_periodo": periodo,
                                    "co_tipo_demonstrativo": tipo,
                                    "no_anexo": anexo,
                                    "co_esfera": esfera,
                                    "id_ente": ente,
                                    "in_periodicidade": periodicidade,
                                    "co_poder": poder
                                }

                                # Requisição GET
                                response = requests.get(base_url, params=params)

                                # Tratamento de erros HTTP
                                if response.status_code != 200:
                                    print(f" ❌ Erro HTTP {response.status_code}: {response.text}")
                                    continue

                                data = response.json()

                                if "items" not in data:
                                    print(" ❌ API não retornou 'items'. Pulando...")
                                    continue

                                df = pd.DataFrame(data["items"])
                                print(f" -> {len(df)} registros coletados.")

                                registros.append(df)

# Concatena todos os dataframes
df_all = pd.concat(registros, ignore_index=True)

# Salvar em CSV
df_all.to_csv("Extrato_BVG_CE_23-25.csv", index=False)

print("\n✅ Arquivo salvo: Extrato_BVG_CE_23-25.csv")
