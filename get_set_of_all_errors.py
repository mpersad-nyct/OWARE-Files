from alive_progress import alive_bar

def get_unique_values(path, output_path):
    try:
        main_set = set()

        content=[]
        with open(path, 'r') as file:
            #content = [line.strip() for line in file.readlines()]
            content = file.readlines()

        main_set = set(content)
        main_set_sorted = list(main_set)
        main_set_sorted.sort()
        with alive_bar(len(main_set_sorted)) as bar:
            with open(output_path, 'w') as out_file:
                for line in main_set_sorted:
                    if line=='>\\r\\n':
                        print()
                    out_file.write(line)
                    bar()
    except Exception as e:
        raise e

def main():
    path = r"H:\Operator Awareness Tool\All Errors.txt"
    output_path = r"H:\Operator Awareness Tool\All Errors Unique.txt"
    try:
        get_unique_values(path, output_path)
    except Exception as e:
        print(e)
        print()
    print("Finished!")

if __name__=='__main__':
    main()