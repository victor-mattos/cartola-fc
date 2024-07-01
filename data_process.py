import pandas as pd
import os
import requests

def process_raw_data(yr_list:list, verbose:bool = False):

    print(f"### Starting data processing")

    root = os.getcwd()
    save_path = os.path.join(root,"data")

    #yr_list = [2020,2022,2023]
    round_list = [i for i in range(1,39)]

    aux_list = []
    for yr in yr_list:
        
        print(f"    ## Processing data of {yr}")

        for r in round_list: 

            file_name = f"rodada-{r}.csv"
            file_path = os.path.join(save_path,str(yr),file_name)
            if verbose:
                print(f"    -- Reading file {file_name}")
            try: 
                df_aux = pd.read_csv(file_path, sep = ",")
                df_aux[:,"year"] = yr
                columns = df_aux.columns
                new_columns = [column_name.replace("atletas.","") for column_name in columns]

                df_aux.columns = new_columns

                aux_list.append(df_aux)
            except:
                continue

    
    df = pd.concat(aux_list)

    return(df)

##################################################
##################################################

def process_curated_data(df_raw:pd.DataFrame):

    df_cur = df_raw.copy()

    df_cur = df_raw[["apelido","atleta_id","rodada_id","clube_id","posicao_id","preco_num","variacao_num","media_num","jogos_num"]] 


    position_dict = {1:"gol",
                     2:"lat",
                     3:"zag",
                     4:"mei",
                     5:"ata",
                     6:"tec"}
    
    # df_cur['posicao_id'] = df_cur["posicao_id"].apply(lambda x: position_dict[x] if x in position_dict.keys() else x)

    df_cur.loc[:, 'posicao_id'] = df_cur['posicao_id'].apply(lambda x: position_dict.get(x, x))

    df_cur.loc[:,'apelido'] = df_cur["apelido"].str.lower()
    df_cur.loc[:,'apelido'] = df_cur["apelido"].replace(" ","_")

    df_cur.loc[:,"preco_inicial"] = df_cur["preco_num"] - df_cur["variacao_num"]

    df_cur.drop(columns = ["preco_num","variacao_num","jogos_num"], inplace = True)
    return df_cur


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

def get_market_data(round:int):
    
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