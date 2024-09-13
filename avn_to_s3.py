import os
import datetime
import regex
from alive_progress import alive_bar
import shutil
import csv

def get_buses(bus_file):
    buses = []
    with open(bus_file, 'r') as csv_file:
        reader = csv.reader(csv_file)
        next(reader, None)
        buses = set([int(line[0]) for line in reader])
    return buses

def get_files(server_path):
    bus_file = r"FleetView-Internal - Buses for XX Depot.csv" #Replace XX with bus depot abbreviation
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

def parse_avn_server(file):
    try:
        destination_path = "Enter location of output folder"
        if not os.path.exists(destination_path):
            os.mkdir(destination_path)

        #if you only want files within a specific date range
        beginning_day = datetime.datetime.strptime("240424", "%y%m%d")
        ending_day = datetime.datetime.strptime("240430", "%y%m%d")

        file_name = regex.match(r"CleverWare0{0,}(\d+)", os.path.basename(file)).group(1)
        timestamp = datetime.datetime.strptime(file_name[-6:], "%y%m%d")
        if beginning_day <= timestamp and timestamp <= ending_day:
            destination = os.path.join(destination_path, os.path.basename(file))
            shutil.copy2(file, destination)
    except Exception as e:
        print(e)

def main():
    avn_path = r"\\10.52.25.46\AVM3BusLink_Archive\CleverWare\CleverWare_Logs"
    files = get_files(avn_path)
    parse_avn_server(files)    

if __name__=='__main__':
    main()