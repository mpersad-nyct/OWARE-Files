import zipfile
import io
import os
#from alive_progress import alive_bar
import datetime
from tqdm import tqdm
import multiprocessing

#def lambda_handler(file, output):
def lambda_handler(file):
    #output = r"C:\Users\1292786\OneDrive - The Metropolitan Transportation Authority\Documents\Bus Vehicle Diagnostics\Zipped Files - Additional - Unzipped"
    #output = r"C:\Users\1292786\OneDrive - The Metropolitan Transportation Authority\Documents\Bus Vehicle Diagnostics\Unzipped Files - CS - 2024-04-24 - 2024-04-30"
    #output = r"C:\Users\1292786\OneDrive - The Metropolitan Transportation Authority\Documents\Bus Vehicle Diagnostics\Unzipped Files - BP - 2024-04-24 - 2024-04-30"
    #output = r"C:\Users\1292786\OneDrive - The Metropolitan Transportation Authority\Documents\Bus Vehicle Diagnostics\Unzipped Files - CA - 2024-04-24 - 2024-04-30"
    #output = r"C:\Users\1292786\OneDrive - The Metropolitan Transportation Authority\Documents\Bus Vehicle Diagnostics\Unzipped Files - EC - 2024-04-24 - 2024-04-30"
    #output = r"C:\Users\1292786\OneDrive - The Metropolitan Transportation Authority\Documents\Bus Vehicle Diagnostics\Unzipped Files - JG - 2024-04-24 - 2024-04-30"
    #output = r"C:\Users\1292786\OneDrive - The Metropolitan Transportation Authority\Documents\Bus Vehicle Diagnostics\Unzipped Files - MQ - 2024-04-24 - 2024-04-30"
    output = r"C:\Users\1292786\OneDrive - The Metropolitan Transportation Authority\Documents\Bus Vehicle Diagnostics\Unzipped Files - Test"
    if not file.endswith(".zip"):
        print(f"Skipping '{file}' - Not a zip file.")
    else:
        try:
            with open(file, 'rb') as bfile:
                with io.BytesIO(bfile.read()) as tf:
                    # rewind the file
                    tf.seek(0)
        
                    # Read the file as a zipfile and process the members
                    with zipfile.ZipFile(tf, mode='r') as zipf:
                        for file in zipf.infolist():
                            output_filepath = os.path.join(output, file.filename)
                            if os.path.exists(output_filepath):
                                file_content = []
                                with open(output_filepath, 'rb') as output_file:
                                    file_content = output_file.read().split(b'\r\n')
                                new_file_content = zipf.read(file).split(b'\r\n')
                                if new_file_content[0] == file_content[0]:
                                    print("Same files")
                                else:
                                    new_output_filepath = file.filename.replace(".txt", "")
                                    new_output_filepath += "_"+datetime.datetime.now().strftime("%Y-%m-%d_%H-%M-%S.%f")+".txt"
                                    new_output_filepath = os.path.join(output, new_output_filepath)
                                    with open(new_output_filepath, 'wb') as output_file:
                                        file_content = zipf.read(file)
                                        output_file.write(file_content)

                            else:
                                with open(output_filepath, 'wb') as output_file:
                                    file_content = zipf.read(file)
                                    output_file.write(file_content)
        except Exception as e:
            print(e)
            raise e

if __name__=='__main__':
    try:
        #path = r"C:\Users\1292786\OneDrive - The Metropolitan Transportation Authority\Documents\Bus Vehicle Diagnostics\Zipped Files - Additional"
        #output_path = r"C:\Users\1292786\OneDrive - The Metropolitan Transportation Authority\Documents\Bus Vehicle Diagnostics\Zipped Files - Additional - Unzipped"

        #path = r"C:\Users\1292786\OneDrive - The Metropolitan Transportation Authority\Documents\Bus Vehicle Diagnostics\Zipped Files - CS - 2024-04-24 - 2024-04-30"
        #path = r"C:\Users\1292786\OneDrive - The Metropolitan Transportation Authority\Documents\Bus Vehicle Diagnostics\Zipped Files - BP - 2024-04-24 - 2024-04-30"
        #path = r"C:\Users\1292786\OneDrive - The Metropolitan Transportation Authority\Documents\Bus Vehicle Diagnostics\Zipped Files - CA - 2024-04-24 - 2024-04-30"
        #path = r"C:\Users\1292786\OneDrive - The Metropolitan Transportation Authority\Documents\Bus Vehicle Diagnostics\Zipped Files - EC - 2024-04-24 - 2024-04-30"
        #path = r"C:\Users\1292786\OneDrive - The Metropolitan Transportation Authority\Documents\Bus Vehicle Diagnostics\Zipped Files - JG - 2024-04-24 - 2024-04-30"
        #path = r"C:\Users\1292786\OneDrive - The Metropolitan Transportation Authority\Documents\Bus Vehicle Diagnostics\Zipped Files - MQ - 2024-04-24 - 2024-04-30"
        path = r"C:\Users\1292786\OneDrive - The Metropolitan Transportation Authority\Documents\Bus Vehicle Diagnostics\Zipped Files - Test"
        #output_path = r"C:\Users\1292786\OneDrive - The Metropolitan Transportation Authority\Documents\Bus Vehicle Diagnostics\Unzipped Files - CS - 2024-04-24 - 2024-04-30"
        #output_path = r"C:\Users\1292786\OneDrive - The Metropolitan Transportation Authority\Documents\Bus Vehicle Diagnostics\Unzipped Files - BP - 2024-04-24 - 2024-04-30"
        #output_path = r"C:\Users\1292786\OneDrive - The Metropolitan Transportation Authority\Documents\Bus Vehicle Diagnostics\Unzipped Files - CA - 2024-04-24 - 2024-04-30"
        #output_path = r"C:\Users\1292786\OneDrive - The Metropolitan Transportation Authority\Documents\Bus Vehicle Diagnostics\Unzipped Files - EC - 2024-04-24 - 2024-04-30"
        #output_path = r"C:\Users\1292786\OneDrive - The Metropolitan Transportation Authority\Documents\Bus Vehicle Diagnostics\Unzipped Files - JG - 2024-04-24 - 2024-04-30"
        #output_path = r"C:\Users\1292786\OneDrive - The Metropolitan Transportation Authority\Documents\Bus Vehicle Diagnostics\Unzipped Files - MQ - 2024-04-24 - 2024-04-30"
        output_path = r"C:\Users\1292786\OneDrive - The Metropolitan Transportation Authority\Documents\Bus Vehicle Diagnostics\Unzipped Files - Test"
        if not os.path.exists(output_path):
            os.mkdir(output_path)
        
        files = os.listdir(path)
        for i in range(len(files)):
            files[i] = os.path.join(path, files[i])
        
        for file in files:
            lambda_handler(file)

        exit(-1)

        num_processes = 12
        pool = multiprocessing.Pool(processes=num_processes)

        with tqdm(total=len(files)) as pbar:
            # def update(*a):
            #     pbar.update()

            # pool.map_async(lambda_handler, files, callback=update)
            for _ in pool.imap(lambda_handler, files):
                pbar.update()

        pool.close()
        
        pool.join()
        # with alive_bar(len(files)) as bar:
        #     for file in files:
        #         lambda_handler(os.path.join(path, file), output_path)
        #         bar()
    except Exception as e:
        print(e)
        raise e