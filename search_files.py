import os
import sys
from alive_progress import alive_bar
import csv

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

def search_folder(path):
    #path = sys.argv[1]
    #path = r"C:\Users\1292786\OneDrive - The Metropolitan Transportation Authority\Documents\Bus Vehicle Diagnostics\Unzipped Files - Test"
    dic = {}

    with alive_bar(len(os.listdir(path))) as bar:
        for file in os.listdir(path):
            line_counter = 0
            filepath = os.path.join(path, file)
            with open(filepath, 'rb') as text:
                for line in text.readlines():
                    line_counter += 1
                    if line.count(b'|') != 15:
                        if filepath not in dic:
                            dic[filepath] = []
                        #dic[filepath].append(f"Line {line_counter}:\t".encode('utf-8')+line)
                        #dic[filepath].append((line_counter, line.replace(b'\r', b'\\r').replace(b'\n', b'\\n')))
                        dic[filepath].append((line_counter, line))
                        #dic[filepath].append(b'\n')
            bar()
    
    #keys = len(dic.keys())
    
    if dic:
        #key_counter = 0
        #with open(f"{path}_not_16_columns.txt", 'wb') as text:
        with open(f"{path}_not_16_columns.csv", 'w', newline="") as csvFile:
            #text.write(b"Failed Files\n")
            writer = csv.writer(csvFile)
            writer.writerow(["File", "Line Number", "Line Content"])
            for key in dic:
                #key_counter+=1
                #if keys == key_counter:
                #    dic[key].pop()
                #text.write(key.encode("utf-8"))
                #text.write(b'\n')
                #text.write(f"Total Occurrences: ".encode('utf-8'))
                for line in dic[key]:
                    #text.write(line)
                    #writer.writerow([key, line[0], line[1].decode("utf-8")])
                    final_line = line[1].decode("utf-8")
                    for rep in characters:
                        final_line = final_line.replace(rep[0], rep[1])
                    writer.writerow([key, line[0], final_line])
                
                #if keys != key_counter:
                #    text.write(b'\n')

def main():
    path = r"H:\Operator Awareness Tool"
    #folders = list(os.walk(path))[0][1]
    folders = [r"H:\Operator Awareness Tool\Unzipped Files - BP - 2024-04-24 - 2024-04-30"]
    for folder in folders:
        if "Unzipped" in folder:
            if "2024-04-24 - 2024-04-30" in folder:
                try:
                    search_folder(os.path.join(path, folder))
                except Exception as e:
                    print(e)
                    print(folder)

if __name__=='__main__':
    main()