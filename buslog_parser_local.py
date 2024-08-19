from io import StringIO, BytesIO
import pandas as pd
import numpy as np
import io
import os
import datetime
import csv
import requests
from alive_progress import alive_bar
from tqdm import tqdm
import multiprocessing
import sys

# Yard Express Url
url = 'http://yardexpress-daily.s3.amazonaws.com/yardexpress_{}.csv'

# Depot Names
abbrevation = {'Depot': ['BAISLEY PARK', 'CASTLETON', "CHARLESTON", "COLLEGE POINT", "CASEY STENGEL", "EASTCHESTER",
                         "EAST NEW YORK", "FLATBUSH",
                         "FRESH POND", "FAR ROCKAWAY", "GRAND AV", "GUN HILL", "JAMAICA", "JACKIE GLEASON", "JFK",
                         "KINGSBRIDGE", "LA GUARDIA",
                         "MEREDITH AV", "MIKE QUILL", "MANHATTANVILLE", "MOTHER HALE", "TUSKEGEE AIRMEN",
                         "QUEENS VILLAGE", "SPRING CREEK",
                         "ULMER PARK", "WEST FARMS", "YONKERS", "YUKON"],
               'Division': ["Queens South", "Staten Island", "Staten Island", "Queens North", "Queens North", "Bronx",
                            "Brooklyn",
                            "Brooklyn", "Brooklyn", "Queens South", "Brooklyn", "Bronx", "Queens North", "Brooklyn",
                            "Queens South", "Bronx",
                            "Queens North", "Staten Island", "Manhattan", "Manhattan", "Manhattan", "Manhattan",
                            "Queens North", "Brooklyn",
                            "Brooklyn", "Bronx", "Bronx", "Staten Island"]}

depot = pd.DataFrame(abbrevation,
                     index=["BP", "CA", "CH", "CP", "CS", "EC", "EN", "FB", "FP", "FR", "GA", "GH", "JA", "JG",
                            "JK", "KB", "LG", "MA", "MQ", "MV", "OF", "OH", "QV", "SC", "UP", "WF", "YO", "YU"])

#output_path = r"C:\Users\1292786\OneDrive - The Metropolitan Transportation Authority\Documents\Bus Vehicle Diagnostics\Curated Files - CS - 2024-04-24 - 2024-04-30"
#output_path = r"C:\Users\1292786\OneDrive - The Metropolitan Transportation Authority\Documents\Bus Vehicle Diagnostics\Curated Files - BP - 2024-04-24 - 2024-04-30"
#output_path = r"C:\Users\1292786\OneDrive - The Metropolitan Transportation Authority\Documents\Bus Vehicle Diagnostics\Curated Files - CA - 2024-04-24 - 2024-04-30"
#output_path = r"C:\Users\1292786\OneDrive - The Metropolitan Transportation Authority\Documents\Bus Vehicle Diagnostics\Curated Files - EC - 2024-04-24 - 2024-04-30"
#output_path = r"C:\Users\1292786\OneDrive - The Metropolitan Transportation Authority\Documents\Bus Vehicle Diagnostics\Curated Files - JG - 2024-04-24 - 2024-04-30"
#output_path = r"C:\Users\1292786\OneDrive - The Metropolitan Transportation Authority\Documents\Bus Vehicle Diagnostics\Curated Files - MQ - 2024-04-24 - 2024-04-30"

#output_path = r"C:\Users\1292786\OneDrive - The Metropolitan Transportation Authority\Documents\Bus Vehicle Diagnostics\Curated Files - CS - 2024-04-24 - 2024-04-30"
#output_path = r"C:\Users\1292786\OneDrive - The Metropolitan Transportation Authority\Documents\Bus Vehicle Diagnostics\Curated Files - BP - 2024-04-24 - 2024-04-30"
#output_path = r"C:\Users\1292786\OneDrive - The Metropolitan Transportation Authority\Documents\Bus Vehicle Diagnostics\Curated Files - CA - 2024-04-24 - 2024-04-30"
#output_path = r"C:\Users\1292786\OneDrive - The Metropolitan Transportation Authority\Documents\Bus Vehicle Diagnostics\Curated Files - EC - 2024-04-24 - 2024-04-30"
#output_path = r"C:\Users\1292786\OneDrive - The Metropolitan Transportation Authority\Documents\Bus Vehicle Diagnostics\Curated Files - JG - 2024-04-24 - 2024-04-30"
#output_path = r"C:\Users\1292786\OneDrive - The Metropolitan Transportation Authority\Documents\Bus Vehicle Diagnostics\Curated Files - MQ - 2024-04-24 - 2024-04-30"

def clean_file(file):
    file_bytes = ""
    with open(file, 'rb') as f:
        file_bytes = f.read()
    df_test = pd.DataFrame(BytesIO(file_bytes))
    cleaning_test = df_test.iloc[:, 0].astype(str).str.startswith('>')
    if True in set(cleaning_test):
        df_test = df_test[~cleaning_test]
        file_bytes = b''.join(df_test.values.flatten())
    # if file_bytes.count(b"\r\n>\r\n") > 0:
    #     file_bytes = file_bytes.replace(b"\r\n>\r\n", b"\r\n")
    reader = BytesIO(file_bytes)
    return reader

def lambda_handler(file, output_path):
#def lambda_handler(file):
    #output_path = r"C:\Users\1292786\OneDrive - The Metropolitan Transportation Authority\Documents\Bus Vehicle Diagnostics\Zipped Files - Additional - Curated"
    # Open file as CSV
    print(file)
    #df = pd.read_csv(file, sep='|', header=None, on_bad_lines='warn')
    clean_text_file = clean_file(file)
    #df = pd.read_csv(clean_text_file, sep='|', header=None)
    df = pd.read_csv(clean_text_file, sep='|', header=None, on_bad_lines='warn')    

    # Name Columns
    df.columns = ['Date', 'Time', 'Event', 'Application', 'Module', 'Lat', 'Long', 'Heading', 'Speed',
                  '0', '1', '2', '3', '4', '5', 'Status']
    # Drop Columns
    df.drop(['0', '1', '2', '3', '4', '5'], inplace=True, axis=1)
    
    # Extract Framework Module
    df_framework = df[df["Module"] == 'Framework']
    
    # Get VehicleId
    #result = df_framework[df_framework['Status'].str.contains("VehicleId=")]
    #result = result.reset_index()
    #bus_id = result.at[0, "Status"].replace('VehicleId=', '')
    bus_id = int(os.path.basename(file)[13:17].replace('.txt', ''))
    #print(f"Vehicle ID: {bus_id}")
    
    # Change time to datetime format
    x = pd.to_datetime(df_framework['Date'] + ' ' + df_framework['Time'])
    df_framework = df_framework.copy()
    df_framework['DateTime'] = x
    
    # Round time to second
    lst = []
    indexes = df_framework['DateTime'].index
    for i in range(len(indexes)):
        curr_index = indexes[i]
        ts = df_framework['DateTime'][curr_index]
        #ts = ts.round(freq='s')
        ts_temp = ts.replace(microsecond=0)
        if i!=0:
            if i < len(indexes)-1:
                if df_framework['DateTime'][indexes[i+1]].replace(microsecond=0) != ts_temp:
                    ts_temp=ts.round(freq='s')
            elif i == len(indexes)-1:
                if len(lst) >=1 and lst[i-1] == ts_temp:
                    ts_temp=ts.round(freq='s')
        ts=ts_temp
        lst.append(ts)
    df_framework['DateTime'] = lst
    
    df_framework = df_framework.drop_duplicates('DateTime')
    

    df_framework_temp = df[df["Module"] != 'Framework']

    # Change time to datetime format
    x = pd.to_datetime(df_framework_temp['Date'] + ' ' + df_framework_temp['Time'])
    df_framework_temp = df_framework_temp.copy()
    df_framework_temp['DateTime'] = x
        
    # Round time to second
    lst = []
    indexes = df_framework_temp['DateTime'].index
    for i in range(len(indexes)):
        curr_index = indexes[i]
        ts = df_framework_temp['DateTime'][curr_index]
        #ts = ts.round(freq='s')
        ts_temp = ts.replace(microsecond=0)
        if i!=0:
            if i < len(indexes)-1:
                if df_framework_temp['DateTime'][indexes[i+1]].replace(microsecond=0) != ts_temp:
                    ts_temp=ts.round(freq='s')
            elif i == len(indexes)-1:
                if len(lst) >=1 and lst[i-1] == ts_temp:
                    ts_temp=ts.round(freq='s')
        ts=ts_temp
        lst.append(ts)
    df_framework_temp['DateTime'] = lst
    
    df_framework_temp = df_framework_temp.drop_duplicates('DateTime')
    
    df_framework = df_framework.merge(df_framework_temp, how="outer", on="DateTime", indicator=True)
    df_framework['Date'] = np.where(pd.isna(df_framework['Date_x']), df_framework['Date_y'], df_framework['Date_x'])
    df_framework['Time'] = np.where(pd.isna(df_framework['Time_x']), df_framework['Time_y'], df_framework['Time_x'])
    df_framework['Event'] = np.where(pd.isna(df_framework['Event_x']), df_framework['Event_y'], df_framework['Event_x'])
    df_framework['Application'] = np.where(pd.isna(df_framework['Application_x']), df_framework['Application_y'], df_framework['Application_x'])
    df_framework['Module'] = np.where(pd.isna(df_framework['Module_x']), df_framework['Module_y'], df_framework['Module_x'])
    df_framework['Lat'] = np.where(pd.isna(df_framework['Lat_x']), df_framework['Lat_y'], df_framework['Lat_x'])
    df_framework['Long'] = np.where(pd.isna(df_framework['Long_x']), df_framework['Long_y'], df_framework['Long_x'])
    df_framework['Heading'] = np.where(pd.isna(df_framework['Heading_x']), df_framework['Heading_y'], df_framework['Heading_x'])
    df_framework['Speed'] = np.where(pd.isna(df_framework['Speed_x']), df_framework['Speed_y'], df_framework['Speed_x'])
    df_framework['Status'] = np.where(pd.isna(df_framework['Status_x']), df_framework['Status_y'], df_framework['Status_x'])    
    df_framework = df_framework.drop(["Date_x", "Date_y",
                                      "Time_x", "Time_y",
                                      "Event_x", "Event_y",
                                      "Application_x", "Application_y",
                                      "Module_x", "Module_y",
                                      "Lat_x", "Lat_y",
                                      "Long_x", "Long_y",
                                      "Heading_x", "Heading_y",
                                      "Speed_x", "Speed_y",
                                      "Status_x", "Status_y",
                                      "_merge"], axis=1)
    
    df_framework_datetime = df_framework.pop('DateTime')
    df_framework.insert(len(df_framework.columns), "DateTime", df_framework_datetime)
        
    # Get Date in Cleverware file
    date_test = df_framework['Date'].unique()
    valid_dates = [d for d in date_test if pd.to_datetime(d, errors='coerce') is not pd.NaT]
    
    # Get Bus Operator
    bus_operator = []
    data_dict = {}
    for i in valid_dates:
        #print(i)
        response = requests.get(url.format(i))
        if response.status_code != 200:
            continue
        x = response.text
        rawData = pd.read_csv(io.StringIO(x), header=0)
        #Cleaning up rawData first
        #"Unscheduled PO" and "PO Depot" have a space character at the end
        rawData = rawData.drop(rawData[rawData['Unscheduled PO '] == 'X'].index)
        # Change rows to datetime format
        rawData['Scheduled PO'] = pd.to_datetime(rawData['Scheduled PO'] + ' ' + i, format="%H:%M %Y-%m-%d")
        rawData['Actual PO'] = pd.to_datetime(rawData['Actual PO'] + ' ' + i, format="%H:%M %Y-%m-%d")
        rawData['Scheduled PI'] = pd.to_datetime(rawData['Scheduled PI'] + ' ' + i, format="%H:%M %Y-%m-%d")
        rawData.loc[rawData['Scheduled PI'] < rawData['Scheduled PO'], 'Scheduled PI'] += datetime.timedelta(days=1)
        rawData['Actual PI'] = pd.to_datetime(rawData['Actual PI'] + ' ' + i, format="%H:%M %Y-%m-%d")
        rawData.loc[rawData['Actual PI'] < rawData['Actual PO'], 'Actual PI'] += datetime.timedelta(days=1)
        # Change Bus column to string
        rawData['Bus'] = rawData['Bus'].astype("string")
        # Get rows with Bus ID
        #get_row_test = rawData[rawData['Bus'].str.match(bus_id)]
        #get_row_test = rawData[rawData['Bus'].str.match(str(bus_id))]
        get_row_test = rawData[rawData['Bus'] == str(bus_id)]
        if get_row_test.shape[0] > 0:
            # Drop asterisk
            row_test = get_row_test[~get_row_test['Operator'].str.contains('\\*')]
            # Reset Index
            index_reset = row_test.reset_index()
            # Add operator
            for index, row in index_reset.iterrows():
                operator = row['Operator']
                route = row['Route']
                if pd.isna(route):
                    continue
                if operator not in data_dict:
                    data_dict[operator] = {}
                if route not in data_dict[operator]:
                    data_dict[operator][route] = []
                data_dict[operator][route].append([row['Actual PO'], row['Actual PI'], row['UTS Depot']])
        # else:
        #     print(f"Time {i} or Bus ID {bus_id} don't match between yard-express and Cleverware log. Please check your file and try again.")
    
    curated_files_generated = False
    #i = operator
    for i in data_dict:
        #r = route
        for r in data_dict[i]:
            #t = ['Actual PO', 'Actual PI', 'UTS Depot']
            for t in data_dict[i][r]:
            # Capture time between scheduled_PI and scheduled_PO
                mask = (df_framework['DateTime'] >= t[0]) & (df_framework['DateTime'] <= t[1])
                df_filtered = df_framework.loc[mask]
                df_copy = df_filtered.copy()
                # Add operator, depot column to framework dataframe
                df_copy["Operator"] = i
                df_copy["UTS Depot"] = t[2]
                # Drop rows with empty cells
                df_copy = df_copy.dropna()
                # Speed Calculations from ft/s to mph
                df_copy['Speed'] = round(df_copy['Speed'] * 0.681818, 9)
                # Calculate time difference between rows
                df_copy['Time_Diff'] = df_copy['DateTime'].diff().dt.total_seconds()
                # Drop rows where time diff < 0
                df_copy = df_copy[df_copy['Time_Diff'] > 0]
                miles_per_second = 1/3600
                df_copy2 = df_copy.assign(
                    Acceleration=df_copy['Speed'].diff() / (df_copy['DateTime'].diff().dt.total_seconds() * (miles_per_second))
                    )
                df_copy2['Acceleration'] = round(df_copy2['Acceleration'], 9)
                                
                # Set index as UTS Depot
                df_copy2 = df_copy2.set_index("UTS Depot")
                joined_df = df_copy2.join(depot)
                joined_df.index.name = 'UTS Depot'
                joined_df['UTSDepot'] = joined_df.index
                # Drop columns
                joined_df = joined_df.drop(['Date', 'Time', 'Event', 'Application', 'Module', 'Status', 'Time_Diff'], axis=1)
                joined_df.fillna(0.0, inplace=True)
                
                joined_df_speed = joined_df.pop('Speed')
                joined_df.insert(len(joined_df.columns), "Speed", joined_df_speed)
                joined_df_acceleration = joined_df.pop('Acceleration')
                joined_df.insert(len(joined_df.columns), "Acceleration", joined_df_acceleration)

                output_file = '{}-{}-cleverwaredata.csv'.format(os.path.basename(file).replace('.txt', ''), i)

                # Drop empty dataframe
                if joined_df.shape[0] > 0:
                    curated_files_generated = True
                    joined_df['Bus_ID'] = bus_id
                    if 1527 <= int(bus_id) <= 2716:
                        joined_df = joined_df[joined_df['Speed'] <= 60]
                        joined_df.to_csv(os.path.join(output_path, output_file), index=False)
                        #print("Parsing Complete")
                        #print(f'This is an Express bus with Bus ID: {bus_id}')
                    else:
                        joined_df = joined_df[joined_df['Speed'] <= 40]
                        joined_df.to_csv(os.path.join(output_path, output_file), index=False)
                        #print("Parsing Complete")
                        #print(f'This is not an Express bus with Bus ID: {bus_id}')
                # else:
                #     print("DataFrame is empty, export to s3 skipped.")

class WarningCapture:
    def __init__(self):
        self.warnings = []
    
    def write(self, text):
        self.warnings.append(text)
    
    def flush(self):
        pass

def main():
    try:
        warnings_capture = WarningCapture()
        sys.stderr = warnings_capture
        path = sys.argv[1]
        output_path = sys.argv[2]
        #path = r"C:\Users\1292786\OneDrive - The Metropolitan Transportation Authority\Documents\Bus Vehicle Diagnostics\Unzipped Files - Test"
        #output_path = r"C:\Users\1292786\OneDrive - The Metropolitan Transportation Authority\Documents\Bus Vehicle Diagnostics\Curated Files - Test"
        
        if not os.path.exists(output_path):
            os.mkdir(output_path)
        
        files = os.listdir(path)
        for i in range(len(files)):
            files[i] = os.path.join(path, files[i])
        
        error_file = os.path.join(os.path.dirname(output_path), f"Buslog_parser_local_testing_failed_files_{os.path.basename(path)}.txt")
        warning_file = os.path.join(os.path.dirname(output_path), f"Buslog_parser_local_testing_warnings_{os.path.basename(path)}.txt")
    
        with alive_bar(len(files)) as bar:
            for file in files:
                #lambda_handler(os.path.join(path, file), output_path)
                #lambda_handler(os.path.join(path, file))
                #lambda_handler(file)
                try:
                    lambda_handler(file, output_path)
                except Exception as e:
                    if not os.path.exists(error_file):
                        with open(error_file, 'w') as text:
                            text.write("Failed Files:")
                    with open(error_file, 'a+') as text:
                        text.write('\n')
                        text.write(file)
                        text.write('\n')
                        text.write(str(e))
                        text.write('\n')
                bar()
        
        sys.stderr = sys.__stderr__
        if len(warnings_capture.warnings) > 0:
            if not os.path.exists(warning_file):
                with open(warning_file, 'w') as text:
                    text.write("Warnings:")
            with open(warning_file, 'a+') as text:
                for warning in warnings_capture.warnings:
                    text.write('\n')
                    text.write(warning.split('\n')[0])
    except Exception as e:
        print(e)
        raise e

if __name__=='__main__':
    main()
    # try:
    #     #path = r"C:\Users\1292786\OneDrive - The Metropolitan Transportation Authority\Documents\Bus Vehicle Diagnostics\Unzipped Files - CS - 2024-04-24 - 2024-04-30"
    #     #path = r"C:\Users\1292786\OneDrive - The Metropolitan Transportation Authority\Documents\Bus Vehicle Diagnostics\Unzipped Files - BP - 2024-04-24 - 2024-04-30"
    #     #path = r"C:\Users\1292786\OneDrive - The Metropolitan Transportation Authority\Documents\Bus Vehicle Diagnostics\Unzipped Files - CA - 2024-04-24 - 2024-04-30"
    #     #path = r"C:\Users\1292786\OneDrive - The Metropolitan Transportation Authority\Documents\Bus Vehicle Diagnostics\Unzipped Files - EC - 2024-04-24 - 2024-04-30"
    #     #path = r"C:\Users\1292786\OneDrive - The Metropolitan Transportation Authority\Documents\Bus Vehicle Diagnostics\Unzipped Files - JG - 2024-04-24 - 2024-04-30"
    #     #path = r"C:\Users\1292786\OneDrive - The Metropolitan Transportation Authority\Documents\Bus Vehicle Diagnostics\Unzipped Files - MQ - 2024-04-24 - 2024-04-30"
        
    #     #path = r"H:\Operator Awareness Tool\Unzipped Files - CS - 2024-04-24 - 2024-04-30"
    #     path = r"H:\Operator Awareness Tool\Unzipped Files - BP - 2024-04-24 - 2024-04-30"
    #     #path = r"H:\Operator Awareness Tool\Unzipped Files - CA - 2024-04-24 - 2024-04-30"
    #     #path = r"H:\Operator Awareness Tool\Unzipped Files - EC - 2024-04-24 - 2024-04-30"
    #     #path = r"H:\Operator Awareness Tool\Unzipped Files - JG - 2024-04-24 - 2024-04-30"
    #     #path = r"H:\Operator Awareness Tool\Unzipped Files - MQ - 2024-04-24 - 2024-04-30"
        
    #     #output_path = r"C:\Users\1292786\OneDrive - The Metropolitan Transportation Authority\Documents\Bus Vehicle Diagnostics\Unzipped Files - Test"
    #     #output_path = r"C:\Users\1292786\OneDrive - The Metropolitan Transportation Authority\Documents\Bus Vehicle Diagnostics\Zipped Files - Additional - Curated"
    #     if not os.path.exists(output_path):
    #         os.mkdir(output_path)
        
    #     files = os.listdir(path)
    #     for i in range(len(files)):
    #         files[i] = os.path.join(path, files[i])
        
    #     # lst = [r"C:\Users\1292786\OneDrive - The Metropolitan Transportation Authority\Documents\Bus Vehicle Diagnostics\Zipped Files - Additional - Unzipped\CleverWare0006016240428.txt",
    #     #        r"C:\Users\1292786\OneDrive - The Metropolitan Transportation Authority\Documents\Bus Vehicle Diagnostics\Zipped Files - Additional - Unzipped\CleverWare0006016240429.txt"]
    #     # for file in lst:
    #     #     print(file)
    #     #     lambda_handler(file)

    #     # num_processes = 4
    #     # pool = multiprocessing.Pool(processes=num_processes)

    #     # with tqdm(total=len(files)) as pbar:
    #     #     # def update(*a):
    #     #     #     pbar.update()

    #     #     # pool.map_async(lambda_handler, files, callback=update)
    #     #     #pool.map_async(print_file, files, callback=update)
    #     #     for _ in pool.imap(lambda_handler, files):
    #     #         pbar.update()

    #     # pool.close()
        
    #     # pool.join()

    #     with alive_bar(len(files)) as bar:
    #         for file in files:
    #             #lambda_handler(os.path.join(path, file), output_path)
    #             #lambda_handler(os.path.join(path, file))
    #             lambda_handler(file)
    #             bar()
    # except Exception as e:
    #     print(e)
    #     raise e