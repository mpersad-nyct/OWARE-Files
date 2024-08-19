import pandas as pd
from alive_progress import alive_bar
import os

def clean_file(file):
     csv_file = pd.read_csv(file)
     csv_file.drop('DateTime Backup', inplace=True, axis=1)
     csv_file.to_csv(file, index=False)

def main():
    path = r"H:\Operator Awareness Tool\New folder"
    files = os.listdir(path)
    for i in range(len(files)):
        files[i] = os.path.join(path, files[i])
    with alive_bar(len(files)) as bar:
            for file in files:
                #lambda_handler(os.path.join(path, file), output_path)
                #lambda_handler(os.path.join(path, file))
                print(file)
                clean_file(file)
                bar()

if __name__=='__main__':
    main()