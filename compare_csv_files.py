import os
from alive_progress import alive_bar
import pandas as pd

def compare_csv_files(path1, path2):
    try:
        path1_files = os.listdir(path1)
        
        path2_files = os.listdir(path2)

        file_count_equal = len(path1_files) == len(path2_files)
        print(f"Total Number of Files Equal: {file_count_equal}")

        if not file_count_equal:
            return
        
        path1_files.sort()
        path2_files.sort()
        print(f"File Lists Equal: {path1_files == path2_files}")

        with alive_bar(len(path1_files)) as bar:
            for i in range(len(path1_files)):
                file1 = path1_files[i]
                file2 = path2_files[i]
                if file1 != file2:
                    print(f"Not Equal Files: '{file1}', '{file2}'")
                    return
                filepath1 = os.path.join(path1, file1)
                filepath2 = os.path.join(path2, file2)

                df1 = pd.read_csv(filepath1)
                df2 = pd.read_csv(filepath2)
                df1_shape = df1.shape
                #df2_shape = (df2.shape[0], df2.shape[1]-2)
                df2_shape = (df2.shape[0], df2.shape[1]-15)
                if df1_shape != df2_shape:
                    print(f"CSV files are not the same: '{filepath1}' with shape {df1.shape}, '{filepath2}' with shape '{df2.shape}'")
                    #return
                bar()
        print("All files are equal.")
    except Exception as e:
        raise e

def main():
    #path1 = r"H:\Operator Awareness Tool\Curated Files - All - 2024-04-24 - 2024-04-30"
    path1 = r"H:\Operator Awareness Tool\Curated Files - All - 2024-04-24 - 2024-04-30 - Updated"
    #path2 = r"H:\Operator Awareness Tool\Curated Files - All - 2024-04-24 - 2024-04-30 - Updated"
    path2 = r"H:\Operator Awareness Tool\Curated Files - All - 2024-04-24 - 2024-04-30 - Updated2"
    try:
        compare_csv_files(path1, path2)
    except Exception as e:
        print(e)
        print()
    print("Finished!")

if __name__=='__main__':
    main()