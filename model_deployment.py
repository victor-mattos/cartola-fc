import pandas as pd
import os
import requests
import unidecode




def process_current_round_data(df:pd.DataFrame, verbose:bool = False):

    print(f"### Starting data processing")

    df["year"] = 2024
    columns = df.columns
    new_columns = [column_name.replace("atletas.","") for column_name in columns]

    df.columns = new_columns

    return(df)

def flat_request_json(data:dict)-> dict:
    # Flatten the JSON structure
    flattened_data = []
    for item in data:
        flat_item = {}
        for key, value in item.items():
            if key == "gato_mestre":
                continue  # Skip the "gato_mestre" dictionary
            if isinstance(value, dict):
                for sub_key, sub_value in value.items():
                    flat_item[f"{key}_{sub_key}"] = sub_value
            else:
                flat_item[key] = value
        flattened_data.append(flat_item)

def filter_json_data(data:dict):
    # Chaves a serem mantidas
    keys_to_keep = [
        'jogos_num', 'atleta_id', 'rodada_id', 'clube_id', 'posicao_id', 
        'status_id', 'pontos_num', 'media_num', 'variacao_num', 'preco_num', 'apelido'
    ]

    # Filtrando os dados
    filtered_data = [{key: item.get(key) for key in keys_to_keep} for item in data]

    return filtered_data

def get_market_data():
    
    mkt_url = "https://api.cartola.globo.com/atletas/mercado/"


    res = requests.get(mkt_url)
    res.raise_for_status()  # Verifica se a resposta HTTP tem status de erro
    data = res.json()  
    data = data["atletas"]     # Tenta decodificar o JSON da resposta

    data = flat_request_json(data = data)
    filtered_data = filter_json_data(data = data)

    #data = flat_json(data = data)
    df = pd.DataFrame(filtered_data)
    #Normaliza a coluna "atletas" e cria um novo DataFrame
    #df = pd.json_normalize(df['atletas'])
    #Remove o prefixo "scout." das colunas
    #df.rename(columns=lambda x: x.replace('scout.', ''), inplace=True)


    return df