import os
from alive_progress import alive_bar

def search_folder(path):
    main_set = set()

    with alive_bar(len(os.listdir(path))) as bar:
        for file in os.listdir(path):
            line_counter = 0
            filepath = os.path.join(path, file)
            with open(filepath, 'rb') as text:
                for line in text.readlines():
                    main_set.update(set(line.decode("utf-8")))
            bar()
    
    return main_set

def main():
    main_set = set()
    path = r"H:\Operator Awareness Tool"
    folders = list(os.walk(path))[0][1]
    for folder in folders:
        if "Unzipped" in folder:
            if "2024-04-24 - 2024-04-30" in folder:
                try:
                    main_set.update(search_folder(os.path.join(path, folder)))
                    print()
                except Exception as e:
                    print(e)
                    print(folder)
    print()
                    

if __name__=='__main__':
    main()