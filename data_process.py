import pandas as pd
import os
import requests
import unidecode


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
                df_aux["year"] = yr
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

    current_players = df_raw[df_raw["year"] == 2024]["apelido"].unique()
    current_teams = df_raw[df_raw["year"] == 2024]["clube_id"].unique()

    df_cur = df_raw.copy()

    df_cur = df_raw[["apelido","atleta_id","rodada_id","clube_id","posicao_id","preco_num","variacao_num","media_num","jogos_num","pontos_num","entrou_em_campo"]] 
    df_cur = df_cur[df_cur["clube_id"].isin(current_teams)]
    df_cur = df_cur[df_cur["apelido"].isin(current_players)]
    df_cur = df_cur[df_cur["entrou_em_campo"] == True]

    position_dict = {1:"gol",
                     2:"lat",
                     3:"zag",
                     4:"mei",
                     5:"ata",
                     6:"tec"}
    
    # df_cur['posicao_id'] = df_cur["posicao_id"].apply(lambda x: position_dict[x] if x in position_dict.keys() else x)
    df_cur = df_cur.reset_index(drop=True)
    df_cur['posicao_id'] = df_cur['posicao_id'].apply(lambda x: position_dict.get(x, x))

    df_cur['apelido'] = df_cur["apelido"].str.lower()
    df_cur['apelido'] = df_cur['apelido'].apply(lambda x: unidecode.unidecode(x).replace(" ", "_"))


    df_cur["preco_inicial"] = df_cur["preco_num"] - df_cur["variacao_num"]
    df_cur["benefit_ratio"] = df_cur["pontos_num"]/df_cur["preco_inicial"]

    df_cur.drop(columns = ["preco_num","jogos_num","entrou_em_campo"], inplace = True)

    df_grouped = df_cur.groupby(["apelido","atleta_id"]).mean("media_num").reset_index()
    df_grouped[df_grouped['media_num'] == 0]

    # Passo 2: Identificar jogadores com média igual a zero
    players_with_zero_mean = df_grouped[df_grouped['media_num'] == 0][['apelido', 'atleta_id']]

    # Passo 3: Filtrar esses jogadores do DataFrame original
    filtered_df_cur = df_cur[~df_cur.set_index(['apelido', 'atleta_id']).index.isin(players_with_zero_mean.set_index(['apelido', 'atleta_id']).index)]

    return filtered_df_cur

def data_encoding(df_cur:pd.DataFrame)->pd.DataFrame:

    from sklearn.preprocessing import OneHotEncoder
        
    cols_to_encode = ['apelido', 'clube_id', 'posicao_id']
    df_encoded = df_cur[cols_to_encode].copy()

    encoder = OneHotEncoder(categories='auto', handle_unknown='ignore')

    # Ajustar e transformar os dados
    df_encoded_onehot = encoder.fit_transform(df_encoded)

    # Obtendo as colunas one-hot encoded
    encoded_columns = encoder.get_feature_names_out(cols_to_encode)

    # Criando um novo dataframe com as colunas one-hot encoded
    df_encoded_onehot_df = pd.DataFrame(df_encoded_onehot.toarray(), columns=encoded_columns)

    # Removendo as colunas originais que foram codificadas
    df_cur_encoded = df_cur.drop(cols_to_encode, axis=1)

    # Resetando o índice para garantir que os dataframes tenham o mesmo índice
    df_cur_encoded.reset_index(drop=True, inplace=True)
    df_encoded_onehot_df.reset_index(drop=True, inplace=True)

    # Concatenando o dataframe original com o dataframe one-hot encoded
    df_cur_encoded = pd.concat([df_cur_encoded, df_encoded_onehot_df], axis=1)

    df_cur_encoded.drop(columns = "atleta_id", inplace = True)

    # Reordenando as colunas
    df_cur_encoded = df_cur_encoded[['pontos_num'] + [col for col in df_cur_encoded.columns if col != 'pontos_num']]

    return df_cur_encoded