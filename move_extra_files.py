import os
import shutil
from alive_progress import alive_bar

def main():
    path = r"C:\Users\1292786\OneDrive - The Metropolitan Transportation Authority\Documents\Bus Vehicle Diagnostics\Zipped Files - Additional"
    new_path = r"C:\Users\1292786\OneDrive - The Metropolitan Transportation Authority\Documents\Bus Vehicle Diagnostics\Zipped Files - Additional - Extra"
    if not os.path.exists(new_path):
        os.mkdir(new_path)
    
    files = os.listdir(path)
    with alive_bar(len(files)) as bar:
        for file in files:
            if "240430" not in file:
                shutil.move(os.path.join(path, file), os.path.join(new_path, file))
            bar()

if __name__=='__main__':
    main()