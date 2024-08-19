import os
import pandas as pd
from alive_progress import alive_bar

characters = [('\x07', '\\x07'), ('\x01', '\\x01'), ('\x10', '\\x10'), ('\x8f', '\\x8f'), ('\x1f', '\\x1f'),
              ('\x9f', '\\x9f'), ('\xad', '\\xad'), ('\x87', '\\x87'), ('\x1e', '\\x1e'), ('\x0f', '\\x0f'),
              ('\x18', '\\x18'), ('\x04', '\\x04'), ('\x13', '\\x13'), ('\x81', '\\x81'), ('\x84', '\\x84'),
              ('\x00', '\\x00'), ('\x1b', '\\x1b'), ('\x0c', '\\x0c'), ('\x93', '\\x93'), ('\x85', '\\x85'),
              ('\x15', '\\x15'), ('\x97', '\\x97'), ('\x95', '\\x95'), ('\x86', '\\x86'), ('\x9d', '\\x9d'),
              ('\x1d', '\\x1d'), ('\x9e', '\\x9e'), ('\x0e', '\\x0e'), ('\x92', '\\x92'), ('\x08', '\\x08'),
              ('\x1c', '\\x1c'), ('\x19', '\\x19'), ('\x89', '\\x89'), ('\x98', '\\x98'), ('\x7f', '\\x7f'),
              ('\x82', '\\x82'), ('\x12', '\\x12'), ('\x06', '\\x06'), ('\x91', '\\x91'), ('\x83', '\\x83'),
              ('\xa0', '\\xa0'), ('\x11', '\\x11'), ('\x8b', '\\x8b'), ('\x03', '\\x03'), ('\x8c', '\\x8c'),
              ('\x14', '\\x14'), ('\x80', '\\x80'), ('\x8d', '\\x8d'), ('\x8a', '\\x8a'), ('\x96', '\\x96'),
              ('\x8e', '\\x8e'), ('\x05', '\\x05'), ('\x88', '\\x88'), ('\x9b', '\\x9b'), ('\x0b', '\\x0b'),
              ('\x17', '\\x17'), ('\x16', '\\x16'), ('\x99', '\\x99'), ('\x9c', '\\x9c'), ('\x94', '\\x94'),
              ('\x90', '\\x90'), ('\x02', '\\x02'), ('\x1a', '\\x1a'), ('\x9a', '\\x9a'),
              ('\r', '\\r'), ('\t', '\\t'), ('\n', '\\n')]

def combine_csv_files(path_list):
    main_csv_file = pd.DataFrame()
    all_files = []
    dic = {}
    total = 0
    try:
        for path in path_list:
            for file in os.listdir(path):
                file_path = os.path.join(path, file)
                all_files.append(file_path)
        
        with alive_bar(len(all_files)) as bar:
            for file in all_files:
                pd_file = pd.read_csv(file)
                dic[file] = pd_file.shape
                total += pd_file.shape[0]
                main_csv_file = pd.concat([main_csv_file, pd_file], ignore_index=True)
                bar()
        
        total_test = sum(shape[0] for shape in dic.values())

        print(total, total_test)

        main_csv_file.to_csv(r"H:\Operator Awareness Tool\Curated Files - 2024-04-24 - 2024-04-30.csv", index=False)
    except Exception as e:
        print(e)
        raise e

def main():
    path = r"H:\Operator Awareness Tool"
    folders = list(os.walk(path))[0][1]
    folder_list = []
    for folder in folders:
        if "Curated" in folder:
            if "2024-04-24 - 2024-04-30" in folder:
                folder_list.append(os.path.join(path, folder))
    try:
        combine_csv_files(folder_list)
    except Exception as e:
        print(e)
        print(folder_list)

if __name__=='__main__':
    main()