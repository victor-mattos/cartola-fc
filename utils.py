
import os
import pandas as pd



def get_historical_data(year:int):

    root = os.getcwd()
    data_path = os.path.join(root, 'data')

    files_in_folder = os.listdir(data_path)

    folder_paths = [os.path.join(data_path, folder) for folder in files_in_folder]
    
    for year_folder in folder_paths:

        all_rounds = os.listdir(year_folder)
        all_rounds_path = [os.path.join(year_folder, round) for round in all_rounds]

        for round in all_rounds_path:
            aux_df = pd.read_csv(round)