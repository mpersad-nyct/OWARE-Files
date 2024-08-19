import os
import datetime
import math
import regex
from alive_progress import alive_bar
import shutil
from tqdm import tqdm
import csv
import multiprocessing

def get_buses(bus_file):
    buses = []
    with open(bus_file, 'r') as csv_file:
        reader = csv.reader(csv_file)
        next(reader, None)
        buses = set([int(line[0]) for line in reader])
    return buses

def get_files(server_path):
    #bus_file = r"C:\Users\1292786\OneDrive - The Metropolitan Transportation Authority\Documents\Bus Vehicle Diagnostics\FleetView-Internal - Buses for CS Depot.csv"
    #bus_file = r"C:\Users\1292786\OneDrive - The Metropolitan Transportation Authority\Documents\Bus Vehicle Diagnostics\FleetView-Internal - Buses for BP Depot.csv"
    #bus_file = r"C:\Users\1292786\OneDrive - The Metropolitan Transportation Authority\Documents\Bus Vehicle Diagnostics\FleetView-Internal - Buses for CA Depot.csv"
    #bus_file = r"C:\Users\1292786\OneDrive - The Metropolitan Transportation Authority\Documents\Bus Vehicle Diagnostics\FleetView-Internal - Buses for EC Depot.csv"
    #bus_file = r"C:\Users\1292786\OneDrive - The Metropolitan Transportation Authority\Documents\Bus Vehicle Diagnostics\FleetView-Internal - Buses for JG Depot.csv"
    bus_file = r"C:\Users\1292786\OneDrive - The Metropolitan Transportation Authority\Documents\Bus Vehicle Diagnostics\FleetView-Internal - Buses for MQ Depot.csv"
    buses = get_buses(bus_file)

    files = []
    dir = os.listdir(server_path)
    with alive_bar(len(dir)) as bar:
        for file in dir:
            file_name = regex.match(r"CleverWare0{0,}(\d+)", file).group(1)
            bus_id = int(file_name[:-6])
            if bus_id in buses:
                if os.path.isfile(os.path.join(server_path, file)):
                    #files.append(file)
                    files.append(os.path.join(server_path, file))
            bar()
    return files
    

#def parse_avn_server(server_path, destination_path, bus_file):
#def parse_avn_server(server_path, destination_path):
#def parse_avn_server(server_path):
#def parse_avn_server(files):
def parse_avn_server(file):
    try:
        #destination_path = r"C:\Users\1292786\OneDrive - The Metropolitan Transportation Authority\Documents\Bus Vehicle Diagnostics\Zipped Files - Additional2"
        #destination_path = r"C:\Users\1292786\OneDrive - The Metropolitan Transportation Authority\Documents\Bus Vehicle Diagnostics\Zipped Files - CS - 2024-04-24 - 2024-04-30"
        #destination_path = r"C:\Users\1292786\OneDrive - The Metropolitan Transportation Authority\Documents\Bus Vehicle Diagnostics\Zipped Files - BP - 2024-04-24 - 2024-04-30"
        #destination_path = r"C:\Users\1292786\OneDrive - The Metropolitan Transportation Authority\Documents\Bus Vehicle Diagnostics\Zipped Files - CA - 2024-04-24 - 2024-04-30"
        #destination_path = r"C:\Users\1292786\OneDrive - The Metropolitan Transportation Authority\Documents\Bus Vehicle Diagnostics\Zipped Files - EC - 2024-04-24 - 2024-04-30"
        #destination_path = r"C:\Users\1292786\OneDrive - The Metropolitan Transportation Authority\Documents\Bus Vehicle Diagnostics\Zipped Files - JG - 2024-04-24 - 2024-04-30"
        destination_path = r"C:\Users\1292786\OneDrive - The Metropolitan Transportation Authority\Documents\Bus Vehicle Diagnostics\Zipped Files - MQ - 2024-04-24 - 2024-04-30"
        # if not os.path.exists(destination_path):
        #     os.mkdir(destination_path)

        #files = [file for file in os.listdir(server_path) if os.path.isfile(os.path.join(server_path, file))]

        #final_files = []
        beginning_day = datetime.datetime.strptime("240424", "%y%m%d")
        ending_day = datetime.datetime.strptime("240430", "%y%m%d")
        #compare_day = datetime.datetime.strptime("240430", "%y%m%d")
        #with alive_bar(len(dir)) as bar:

        #for file in files:

        #file_stats = os.stat(os.path.join(server_path, file))
        file_stats = os.stat(file)
        # date_created = datetime.datetime.fromtimestamp(file_stats.st_birthtime)
        # print(date_created)
        date_modified = datetime.datetime.fromtimestamp(file_stats.st_mtime)
        #print(date_modified)
        #file_size = round(file_stats.st_size/1024)
        file_size = math.ceil(file_stats.st_size/1024)
        #print(file_size)
        file_name = regex.match(r"CleverWare0{0,}(\d+)", os.path.basename(file)).group(1)
        bus_id = int(file_name[:-6])
        timestamp = datetime.datetime.strptime(file_name[-6:], "%y%m%d")
        #converted_timestamp = datetime.datetime.strftime(timestamp, "%Y-%m-%d")
        #converted_timestamp = datetime.datetime.strftime(timestamp, "%Y-%m")
        #print(f"File Name: {file}, Date Modified: {date_modified}, File Size: {file_size}, Bus ID: {bus_id}, File Name Date: {timestamp}, Converted Timestamp: {converted_timestamp}")
        #if converted_timestamp == "2024-01-07":
        #if converted_timestamp == "2024-04":
        # beginning_day = datetime.datetime.strptime("240424", "%y%m%d")
        # ending_day = datetime.datetime.strptime("240430", "%y%m%d")
        #if beginning_day <= timestamp and timestamp <= ending_day:
        # if timestamp == compare_day:
        #     final_files.append(file)
        #     final_files.append("\n")
        #     source = os.path.join(server_path, file)
        #     destination = os.path.join(destination_path, file)
        #     shutil.copy2(source, destination)
        #if bus_id in buses:
        if beginning_day <= timestamp and timestamp <= ending_day:
            # final_files.append(file)
            # final_files.append("\n")
            #source = os.path.join(server_path, file)
            #destination = os.path.join(destination_path, file)
            destination = os.path.join(destination_path, os.path.basename(file))
            #shutil.copy2(source, destination)
            shutil.copy2(file, destination)

                #bar()
        #final_files.pop()
        #with open("final_files.txt", "w+") as txt:
        #    txt.writelines(final_files)
    except Exception as e:
        print(e)

def main():
    #avn_path = r"\\10.52.25.46\AVM3BusLink_Archive\CleverWare\CleverWare_Logs"
    #avn_path = r"C:\Users\1292786\OneDrive - The Metropolitan Transportation Authority\Documents\Benefit Forms"
    #avn_path = r"C:\Users\1292786\OneDrive - The Metropolitan Transportation Authority\Documents\Bus Vehicle Diagnostics\Zipped Files"
    #destination_path = r"C:\Users\1292786\OneDrive - The Metropolitan Transportation Authority\Documents\Bus Vehicle Diagnostics\Zipped Files - Additional"
    #bus_file = r"C:\Users\1292786\OneDrive - The Metropolitan Transportation Authority\Documents\Bus Vehicle Diagnostics\FleetView-Internal - Buses for CS Depot.csv"
    #parse_avn_server(avn_path, destination_path, bus_file)
    #parse_avn_server(avn_path, destination_path)
    #parse_avn_server(avn_path)
    files = get_files()
    parse_avn_server(files)

    

if __name__=='__main__':
    #destination_path = r"C:\Users\1292786\OneDrive - The Metropolitan Transportation Authority\Documents\Bus Vehicle Diagnostics\Zipped Files - Additional2"
    #destination_path = r"C:\Users\1292786\OneDrive - The Metropolitan Transportation Authority\Documents\Bus Vehicle Diagnostics\Zipped Files - CS - 2024-04-24 - 2024-04-30"
    #destination_path = r"C:\Users\1292786\OneDrive - The Metropolitan Transportation Authority\Documents\Bus Vehicle Diagnostics\Zipped Files - BP - 2024-04-24 - 2024-04-30"
    #destination_path = r"C:\Users\1292786\OneDrive - The Metropolitan Transportation Authority\Documents\Bus Vehicle Diagnostics\Zipped Files - CA - 2024-04-24 - 2024-04-30"
    #destination_path = r"C:\Users\1292786\OneDrive - The Metropolitan Transportation Authority\Documents\Bus Vehicle Diagnostics\Zipped Files - EC - 2024-04-24 - 2024-04-30"
    #destination_path = r"C:\Users\1292786\OneDrive - The Metropolitan Transportation Authority\Documents\Bus Vehicle Diagnostics\Zipped Files - JG - 2024-04-24 - 2024-04-30"
    destination_path = r"C:\Users\1292786\OneDrive - The Metropolitan Transportation Authority\Documents\Bus Vehicle Diagnostics\Zipped Files - MQ - 2024-04-24 - 2024-04-30"
    if not os.path.exists(destination_path):
        os.mkdir(destination_path)
    #main()

    avn_path = r"\\10.52.25.46\AVM3BusLink_Archive\CleverWare\CleverWare_Logs"
    files = get_files(avn_path)

    #parse_avn_server(files[0])

    num_processes = 12
    pool = multiprocessing.Pool(processes=num_processes)

    with tqdm(total=len(files)) as pbar:
        # def update(*a):
        #     pbar.update()

        # pool.map_async(parse_avn_server, files, callback=update)
        for _ in pool.imap(parse_avn_server, files):
            pbar.update()

    pool.close()
    
    pool.join()