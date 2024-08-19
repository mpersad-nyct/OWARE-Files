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
import haversine #2024-07-29 FIX Sporadic spikes/drops
import vincenty #2024-08-06 FIX Sporadic spikes/drops

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

#output_path = r"H:\Operator Awareness Tool\Curated Files - CS - 2024-04-24 - 2024-04-30 - Updated"
#output_path = r"H:\Operator Awareness Tool\Curated Files - BP - 2024-04-24 - 2024-04-30 - Updated"
#output_path = r"H:\Operator Awareness Tool\Curated Files - CA - 2024-04-24 - 2024-04-30 - Updated"
#output_path = r"H:\Operator Awareness Tool\Curated Files - EC - 2024-04-24 - 2024-04-30 - Updated"
#output_path = r"H:\Operator Awareness Tool\Curated Files - JG - 2024-04-24 - 2024-04-30 - Updated"
#output_path = r"H:\Operator Awareness Tool\Curated Files - MQ - 2024-04-24 - 2024-04-30 - Updated"

#FIX: '>' character on new line
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

#10kmph to mph = (10/1.60934)
def harsh_acceleration(row):
    #return row['Speed'] > (10/1.60934) and row['G_Force_g'] > 0.15
    #return row['Speed'] > (10/1.60934) and row['G_Force_g'] > 0.3 #2024-07-03
    res = row['Speed'] > (10/1.60934) and row['G_Force_g'] > 0.3
    if res:
        res = row['GPS Speed'] > (10/1.60934) and row['GPS_G_Force_g'] > 0.3
    return res #2024-08-07

#10kmph to mph = (10/1.60934)
def harsh_braking(row):
    #return row['Speed'] > (10/1.60934) and row['G_Force_g'] < -0.2
    #return row['Speed'] > (0) and row['G_Force_g'] < -0.4 #2024-07-03
    res = row['Speed'] > (0) and row['G_Force_g'] < -0.4
    if res:
        res = row['GPS Speed'] > (0) and row['GPS_G_Force_g'] < -0.4
    return res #2024-08-07

def harsh_unique(df, column_name):
    lst = [False]
    tracker = False
    prev = df[column_name].iloc[0]
    for i in range(1, len(df[column_name])):
        curr = df[column_name].iloc[i]
        if curr == 0:
            tracker = False
            lst.append(False)
            continue
        else:
            if tracker:
                lst.append(False)
            else:
                tracker = True
                lst.append(True)
        prev = curr
    return lst

#mapping = {Column Name: (Value to search for, value to assign)}
def map_grouping(df, mapping):
    lst = []
    for i in range(len(df)):
        assigned = False
        for column_name in mapping:
            if df[column_name].iloc[i] == mapping[column_name][0]:
                assigned = True
                lst.append(mapping[column_name][1])
                break
        if not assigned:
            lst.append("None")
    return lst

def remove_incorrect_zero_speed(df):
    if len(df) == 0:
        return df
    moving = False
    s_index = 0
    e_index = 0
    false_zero = False
    all_intervals = []
    prev_index = 0
    prev_row = ""
    for index, row in df.iterrows():
        if row['Speed'] > 0:
            #speed > 0
            moving = True
            if false_zero and s_index != 0:
                e_index = index
                all_intervals.append((s_index, e_index))
                s_index = 0
                e_index = 0
                false_zero = False
            moving = True
        else:
            if index != 0:
                if false_zero:
                    if s_index == 0:
                        s_index = index
                elif abs(prev_row['Lat'] - row['Lat']) > 0.0002 or abs(prev_row['Long'] - row['Long']) > 0.0002:
                    false_zero = True
                    s_index = index
                elif abs(prev_row['Lat'] - row['Lat']) > 0 or abs(prev_row['Long'] - row['Long']) > 0 and not moving:
                    false_zero = True
            moving = False
        prev_index = index
        prev_row = row
    
    if all_intervals:
        all_intervals = sorted(all_intervals, key=lambda x: x[0], reverse=True)

        for index, item in enumerate(all_intervals):
            df = df.drop(df.index[item[0]:item[1]])

    df = df.reset_index(drop=True)

    #removes zero speed rows if it is between non 0 rows (max of 2 zero rows)
    s_index = 0
    e_index = 0
    all_intervals = []
    prev_index = 0
    prev_row = ""
    for index, row in df.iterrows():
        if prev_index == 0:
            if row['Speed'] > 0:
                prev_index = index
                prev_row = row
            continue

        if prev_row['Speed'] > 0 and row['Speed'] == 0:
            s_index = index
        elif row['Speed'] > 0 and prev_row['Speed'] == 0:
            e_index = index
            if e_index - s_index > 0 and e_index - s_index <= 2:
                all_intervals.append((s_index, e_index))
            s_index = 0
            e_index = 0
        prev_index = index
        prev_row = row
    
    if all_intervals:
        all_intervals = sorted(all_intervals, key=lambda x: x[0], reverse=True)

        for index, item in enumerate(all_intervals):
            df = df.drop(df.index[item[0]:item[1]])


    return df

def haversine_calc(df):
    lst_ft_s = []
    lst_mph = []
    prev_index = 0
    prev_row = ""
    count = 0
    for index, row in df.iterrows():
        if count == 0:
            lst_ft_s.append(0)
            lst_mph.append(0)
        else:
            lst_ft_s.append(haversine.haversine((float(row['Lat']), float(row['Long'])), (float(prev_row['Lat']), float(prev_row['Long'])), unit='ft') / (row['DateTime'] - prev_row['DateTime']).total_seconds())
            lst_mph.append((haversine.haversine((float(row['Lat']), float(row['Long'])), (float(prev_row['Lat']), float(prev_row['Long'])), unit='ft') / (row['DateTime'] - prev_row['DateTime']).total_seconds()) * (3600/5280))
        prev_index = index
        prev_row = row
        count += 1
    
    return lst_ft_s, lst_mph

def vincenty_calc(df):
    lst_ft_s = []
    lst_mph = []
    prev_index = 0
    prev_row = ""
    count = 0
    for index, row in df.iterrows():
        if count == 0:
            lst_ft_s.append(0)
            lst_mph.append(0)
        else:
            lst_ft_s.append((vincenty.vincenty((float(row['Lat']), float(row['Long'])), (float(prev_row['Lat']), float(prev_row['Long'])), miles=True) / (row['DateTime'] - prev_row['DateTime']).total_seconds()) * 5280)
            lst_mph.append((vincenty.vincenty((float(row['Lat']), float(row['Long'])), (float(prev_row['Lat']), float(prev_row['Long'])), miles=True) / (row['DateTime'] - prev_row['DateTime']).total_seconds()) * 3600)
        prev_index = index
        prev_row = row
        count += 1
    
    return lst_ft_s, lst_mph

def remove_sporadic_spikes_drops(df, override=False):
    if len(df) == 0:
        return df
    
    s_index = 0
    e_index = 0
    false_zero = False
    all_intervals = []
    prev_index = 0
    prev_row = ""
    max_rows_to_skip = 3
    row_counter = 0
    max_speed_difference = 3
    for index, row in df.iterrows():
        if prev_index != 0:
            if row_counter > max_rows_to_skip:
                s_index = 0
                row_counter = 0
            
            if override or row['Harsh Acceleration'] or row['Harsh Braking'] or s_index != 0:
                if abs(prev_row['Speed'] - row['Speed']) >= max_speed_difference:
                    if s_index == 0:
                        s_index = index
                    else:
                        if row_counter <= max_rows_to_skip:
                            e_index = index
                            all_intervals.append((s_index, e_index))
                        s_index = 0
                        e_index = 0
                        row_counter = 0
            if s_index != 0:
                row_counter += 1
        prev_index = index
        prev_row = row
    
    if all_intervals:
        all_intervals = sorted(all_intervals, key=lambda x: x[0], reverse=True)

        for index, item in enumerate(all_intervals):
            df = df.drop(df.index[item[0]:item[1]])

    df = df.reset_index(drop=True)

    return df

#2024-08-13
def remove_gps_error_speeds(df):
    if len(df) == 0:
        return df
    
    s_index = 0
    e_index = 0
    s_row = ""
    all_intervals = []
    row_counter = 1
    for index, row in df.iterrows():
        if index == 0:
            s_index = index
            s_row = row
            row_counter = 1
            continue
        
        if row['Lat'] == s_row['Lat'] and row['Long'] == s_row['Long'] and row['Heading'] == s_row['Heading']:
            row_counter += 1
        else:
            if row_counter == 2:
                e_index = index
                all_intervals.append((s_index+1, e_index))
            s_index = index
            s_row = row
            row_counter = 1
    
    if all_intervals:
        all_intervals = sorted(all_intervals, key=lambda x: x[0], reverse=True)

        for index, item in enumerate(all_intervals):
            df = df.drop(df.index[item[0]:item[1]])

    df = df.reset_index(drop=True)

    return df

#def lambda_handler(file, output_path):
#def lambda_handler(file):
def lambda_handler(file_and_output_path):
    try:
        file, output_path = file_and_output_path
        #output_path = r"C:\Users\1292786\OneDrive - The Metropolitan Transportation Authority\Documents\Bus Vehicle Diagnostics\Zipped Files - Additional - Curated"
        # Open file as CSV
        #print(file)
        #df = pd.read_csv(file, sep='|', header=None, on_bad_lines='warn')
        clean_text_file = clean_file(file)
        #df = pd.read_csv(clean_text_file, sep='|', header=None)
        # final_file = clean_text_file
        # df_test = pd.DataFrame(clean_text_file)
        # #print(df_test.shape)
        # #test = df_test[df_test.iloc[:, 0].astype(str).str.startswith('>')]
        # #print(test)
        # cleaning_test = df_test.iloc[:, 0].astype(str).str.startswith('>')
        # if True in set(cleaning_test):
        #     df_test = df_test[~cleaning_test]
        #     #df_test = df_test.drop(df_test[df_test.iloc[:, 0][0] == '>'].index)
        #     #print(df_test.shape)
        #     # final_file = b""
        #     # for x in df_test.iloc[:, 0]:
        #     #     final_file += x
        #     final_file = BytesIO(b''.join(df_test.values.flatten()))
        #clean_text_file.seek(0)
        df = pd.read_csv(clean_text_file, sep='|', header=None, on_bad_lines='warn') #FIX: Lines that aren't 16 columns (two lines were combined into one)
        #df = pd.read_csv(final_file, sep='|', header=None, on_bad_lines='warn') #FIX: Lines that aren't 16 columns (two lines were combined into one)
        
        # Name Columns
        df.columns = ['Date', 'Time', 'Event', 'Application', 'Module', 'Lat', 'Long', 'Heading', 'Speed',
                    '0', '1', '2', '3', '4', '5', 'Status']
        # Drop Columns
        df.drop(['0', '1', '2', '3', '4', '5'], inplace=True, axis=1)
        #df.drop(['1', '2', '3', '4', '5'], inplace=True, axis=1) #2024-07-29 FIX Sporadic spikes/drops
        #return
        # Extract Framework Module
        #df_framework = df[df["Module"] == 'Framework']
        df_framework = df.copy()
        
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
        #df_framework['DateTime Backup'] = x
        
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
                    # if df_framework['DateTime'][indexes[i+1]].replace(microsecond=0) != ts_temp:
                    #     ts_temp=ts.round(freq='s')
                    #only round if the next timestamp is not the next second
                    #ex: 14:12:02.966 -> 14:12:04.133
                    ts_plus_1_temp = df_framework['DateTime'][indexes[i+1]].replace(microsecond=0)
                    if ts_plus_1_temp != ts_temp and ts_plus_1_temp != ts_temp + pd.to_timedelta(1, unit='s'):
                        ts_temp=ts.round(freq='s')
                elif i == len(indexes)-1:
                    if len(lst) >=1 and lst[i-1] == ts_temp:
                        ts_temp=ts.round(freq='s')
            ts=ts_temp
            lst.append(ts)
        df_framework['DateTime'] = lst
        
        #df_framework = df_framework.drop_duplicates('DateTime')
        #df_framework = df_framework.sort_values(['DateTime', 'Speed'], ascending=[True, False])

        #2024-07-29 FIX Sporadic spikes/drops
        df_framework['Speed Temp Column'] = np.where(df_framework['Speed'] != 0, True, False)
        df_framework = df_framework.sort_values(['DateTime', 'Speed Temp Column', 'Speed'], ascending=[True, False, True])
        df_framework = df_framework.drop(['Speed Temp Column'], axis=1)
        
        df_framework = df_framework.drop_duplicates("DateTime")
        #print()

        # df_framework_temp = df[df["Module"] != 'Framework']

        # # Change time to datetime format
        # x = pd.to_datetime(df_framework_temp['Date'] + ' ' + df_framework_temp['Time'])
        # df_framework_temp = df_framework_temp.copy()
        # df_framework_temp['DateTime'] = x
            
        # # Round time to second
        # lst = []
        # indexes = df_framework_temp['DateTime'].index
        # for i in range(len(indexes)):
        #     curr_index = indexes[i]
        #     ts = df_framework_temp['DateTime'][curr_index]
        #     #ts = ts.round(freq='s')
        #     ts_temp = ts.replace(microsecond=0)
        #     if i!=0:
        #         if i < len(indexes)-1:
        #             # if df_framework_temp['DateTime'][indexes[i+1]].replace(microsecond=0) != ts_temp:
        #             #     ts_temp=ts.round(freq='s')
        #             #only round if the next timestamp is not the next second
        #             #ex: 14:12:02.966 -> 14:12:04.133
        #             ts_plus_1_temp = df_framework_temp['DateTime'][indexes[i+1]].replace(microsecond=0)
        #             if ts_plus_1_temp != ts_temp and ts_plus_1_temp != ts_temp + pd.to_timedelta(1, unit='s'):
        #                 ts_temp=ts.round(freq='s')
        #         elif i == len(indexes)-1:
        #             if len(lst) >=1 and lst[i-1] == ts_temp:
        #                 ts_temp=ts.round(freq='s')
        #     ts=ts_temp
        #     lst.append(ts)
        # df_framework_temp['DateTime'] = lst
        
        # df_framework_temp = df_framework_temp.drop_duplicates('DateTime')
        
        # df_framework = df_framework.merge(df_framework_temp, how="outer", on="DateTime", indicator=True)
        # df_framework['Date'] = np.where(pd.isna(df_framework['Date_x']), df_framework['Date_y'], df_framework['Date_x'])
        # df_framework['Time'] = np.where(pd.isna(df_framework['Time_x']), df_framework['Time_y'], df_framework['Time_x'])
        # df_framework['Event'] = np.where(pd.isna(df_framework['Event_x']), df_framework['Event_y'], df_framework['Event_x'])
        # df_framework['Application'] = np.where(pd.isna(df_framework['Application_x']), df_framework['Application_y'], df_framework['Application_x'])
        # df_framework['Module'] = np.where(pd.isna(df_framework['Module_x']), df_framework['Module_y'], df_framework['Module_x'])
        # df_framework['Lat'] = np.where(pd.isna(df_framework['Lat_x']), df_framework['Lat_y'], df_framework['Lat_x'])
        # df_framework['Long'] = np.where(pd.isna(df_framework['Long_x']), df_framework['Long_y'], df_framework['Long_x'])
        # df_framework['Heading'] = np.where(pd.isna(df_framework['Heading_x']), df_framework['Heading_y'], df_framework['Heading_x'])
        # df_framework['Speed'] = np.where(pd.isna(df_framework['Speed_x']), df_framework['Speed_y'], df_framework['Speed_x'])
        # df_framework['Status'] = np.where(pd.isna(df_framework['Status_x']), df_framework['Status_y'], df_framework['Status_x'])    
        # df_framework = df_framework.drop(["Date_x", "Date_y",
        #                                 "Time_x", "Time_y",
        #                                 "Event_x", "Event_y",
        #                                 "Application_x", "Application_y",
        #                                 "Module_x", "Module_y",
        #                                 "Lat_x", "Lat_y",
        #                                 "Long_x", "Long_y",
        #                                 "Heading_x", "Heading_y",
        #                                 "Speed_x", "Speed_y",
        #                                 "Status_x", "Status_y",
        #                                 "_merge"], axis=1)
        
        # df_framework_datetime = df_framework.pop('DateTime')
        # df_framework.insert(len(df_framework.columns), "DateTime", df_framework_datetime)
            
        # Get Date in Cleverware file
        date_test = df_framework['Date'].unique()
        valid_dates = [d for d in date_test if pd.to_datetime(d, errors='coerce') is not pd.NaT]
        
        # Get Bus Operator
        bus_operator = []
        data_dict = {}
        for i in valid_dates:
            #print(i)
            response = requests.get(url.format(i))
            if response.status_code != 200: #FIX: unknown date in file
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
        
        df_framework = df_framework.reset_index(drop=True)
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
                    # Add operator, route, depot column to framework dataframe
                    df_copy["Operator"] = i
                    df_copy["Route"] = r
                    df_copy["UTS Depot"] = t[2]
                    # Drop rows with empty cells
                    df_copy = df_copy.dropna()
                    # Speed Calculations from ft/s to mph
                    df_copy['Speed'] = round(df_copy['Speed'] * (3600/5280), 9)
                    # Calculate time difference between rows
                    df_copy['Time_Diff'] = df_copy['DateTime'].diff().dt.total_seconds()
                    # Drop rows where time diff < 0
                    df_copy = df_copy[df_copy['Time_Diff'] > 0]

                    #Clean data FIX - 2024/07/22
                    df_copy = df_copy.reset_index(drop=True)
                    df_copy = remove_incorrect_zero_speed(df_copy)
                    df_copy = df_copy.reset_index(drop=True)
                    
                    df_copy2 = df_copy.assign(
                        Acceleration=df_copy['Speed'].diff() / df_copy['DateTime'].diff().dt.total_seconds(),
                        )
                    df_copy2['Acceleration'] = round(df_copy2['Acceleration'], 9)

                    #(1609.34/3600) => CONVERTING TO M/S
                    df_copy2 = df_copy2.assign(
                        G_Force=df_copy2['Acceleration'] * (1609.34/3600),
                        G_Force_g=(df_copy2['Acceleration'] * (1609.34/3600))/9.81
                        )
                    df_copy2['G_Force'] = round(df_copy2['G_Force'], 9)
                    df_copy2['G_Force_g'] = round(df_copy2['G_Force_g'], 9)

                    #############################          GPS Speed          ############################# #2024-08-07
                    haversine_ft_s, haversine_mph = haversine_calc(df_copy2)
                    #df_copy2['Haversine FT/S'] = haversine_ft_s
                    #df_copy2['Haversine MPH'] = haversine_mph
                    df_copy2['Haversine'] = haversine_mph

                    vincenty_ft_s, vincenty_mph = vincenty_calc(df_copy2)
                    #df_copy2['Vincenty FT/S'] = vincenty_ft_s
                    #df_copy2['Vincenty MPH'] = vincenty_mph
                    df_copy2['Vincenty'] = vincenty_mph

                    df_copy2['GPS Speed'] = df_copy2[['Haversine', 'Vincenty']].mean(axis=1)

                    df_copy2 = df_copy2.assign(
                        GPS_Acceleration=df_copy2['GPS Speed'].diff() / df_copy2['DateTime'].diff().dt.total_seconds(),
                        )
                    df_copy2['GPS_Acceleration'] = round(df_copy2['GPS_Acceleration'], 9)

                    #(1609.34/3600) => CONVERTING TO M/S
                    df_copy2 = df_copy2.assign(
                        GPS_G_Force=df_copy2['GPS_Acceleration'] * (1609.34/3600),
                        GPS_G_Force_g=(df_copy2['GPS_Acceleration'] * (1609.34/3600))/9.81
                        )
                    df_copy2['GPS_G_Force'] = round(df_copy2['GPS_G_Force'], 9)
                    df_copy2['GPS_G_Force_g'] = round(df_copy2['GPS_G_Force_g'], 9)
                    #############################          GPS Speed          #############################
                                    
                    # Set index as UTS Depot
                    # df_copy2 = df_copy2.set_index("UTS Depot")
                    #joined_df = df_copy2.join(depot, on="UTS Depot")
                    # joined_df.index.name = 'UTS Depot'
                    # joined_df['UTSDepot'] = joined_df.index
                    joined_df = df_copy2
                    # Drop columns
                    joined_df = joined_df.drop(['Date', 'Time', 'Event', 'Application', 'Module', 'Status', 'Time_Diff'], axis=1)
                    #joined_df = joined_df.drop(['Date', 'Time', 'Time_Diff'], axis=1)
                    joined_df.fillna(0.0, inplace=True)
                    
                    # Drop empty dataframe
                    if joined_df.shape[0] > 0:
                        joined_df['Bus_ID'] = bus_id

                        joined_df_speed = joined_df.pop('Speed')
                        joined_df.insert(len(joined_df.columns), "Speed", joined_df_speed)
                        joined_df_acceleration = joined_df.pop('Acceleration')
                        joined_df.insert(len(joined_df.columns), "Acceleration", joined_df_acceleration)

                        joined_df_G_Force = joined_df.pop('G_Force')
                        joined_df.insert(len(joined_df.columns), "G_Force", joined_df_G_Force)
                        joined_df_G_Force_g = joined_df.pop('G_Force_g')
                        joined_df.insert(len(joined_df.columns), "G_Force_g", joined_df_G_Force_g)

                        #############################          GPS Speed          ############################# #2024-08-07
                        joined_df_haversine = joined_df.pop('Haversine')
                        joined_df.insert(len(joined_df.columns), "Haversine", joined_df_haversine)
                        joined_df_vincenty = joined_df.pop('Vincenty')
                        joined_df.insert(len(joined_df.columns), "Vincenty", joined_df_vincenty)
                        joined_df_gps_speed = joined_df.pop('GPS Speed')
                        joined_df.insert(len(joined_df.columns), "GPS Speed", joined_df_gps_speed)
                        joined_df_gps_acceleration = joined_df.pop('GPS_Acceleration')
                        joined_df.insert(len(joined_df.columns), "GPS_Acceleration", joined_df_gps_acceleration)
                        joined_df_GPS_G_Force = joined_df.pop('GPS_G_Force')
                        joined_df.insert(len(joined_df.columns), "GPS_G_Force", joined_df_GPS_G_Force)
                        joined_df_GPS_G_Force_g = joined_df.pop('GPS_G_Force_g')
                        joined_df.insert(len(joined_df.columns), "GPS_G_Force_g", joined_df_GPS_G_Force_g)
                        #############################          GPS Speed          #############################

                        output_file = '{}-{}-cleverwaredata.csv'.format(os.path.basename(file).replace('.txt', ''), i)

                        joined_df['Harsh Acceleration'] = joined_df.apply(harsh_acceleration, axis=1)
                        joined_df['Harsh Acceleration Unique'] = harsh_unique(joined_df, 'Harsh Acceleration')
                        joined_df['Harsh Braking'] = joined_df.apply(harsh_braking, axis=1)
                        joined_df['Harsh Braking Unique'] = harsh_unique(joined_df, 'Harsh Braking')

                        #First Run
                        #2024-07-29 FIX Sporadic spikes/drops
                        joined_df = joined_df.reset_index(drop=True)
                        joined_df = remove_sporadic_spikes_drops(joined_df)
                        joined_df = joined_df.reset_index(drop=True)

                        #2024-08-13 FIX GPS Anomaly
                        joined_df = joined_df.reset_index(drop=True)
                        joined_df = remove_gps_error_speeds(joined_df)
                        joined_df = joined_df.reset_index(drop=True)

                        #########################################                      Recalcuate                      #########################################
                        joined_df = joined_df.assign(
                            Acceleration=joined_df['Speed'].diff() / joined_df['DateTime'].diff().dt.total_seconds(),
                            )
                        joined_df['Acceleration'] = round(joined_df['Acceleration'], 9)

                        #(1609.34/3600) => CONVERTING TO M/S
                        joined_df = joined_df.assign(
                            G_Force=joined_df['Acceleration'] * (1609.34/3600),
                            G_Force_g=(joined_df['Acceleration'] * (1609.34/3600))/9.81
                            )
                        joined_df['G_Force'] = round(joined_df['G_Force'], 9)
                        joined_df['G_Force_g'] = round(joined_df['G_Force_g'], 9)

                        #############################          GPS Speed          ############################# #2024-08-07
                        haversine_ft_s, haversine_mph = haversine_calc(joined_df)
                        #joined_df['Haversine FT/S'] = haversine_ft_s
                        #joined_df['Haversine MPH'] = haversine_mph
                        joined_df['Haversine'] = haversine_mph

                        vincenty_ft_s, vincenty_mph = vincenty_calc(joined_df)
                        #joined_df['Vincenty FT/S'] = vincenty_ft_s
                        #joined_df['Vincenty MPH'] = vincenty_mph
                        joined_df['Vincenty'] = vincenty_mph

                        joined_df['GPS Speed'] = joined_df[['Haversine', 'Vincenty']].mean(axis=1)

                        joined_df = joined_df.assign(
                            GPS_Acceleration=joined_df['GPS Speed'].diff() / joined_df['DateTime'].diff().dt.total_seconds(),
                            )
                        joined_df['GPS_Acceleration'] = round(joined_df['GPS_Acceleration'], 9)

                        #(1609.34/3600) => CONVERTING TO M/S
                        joined_df = joined_df.assign(
                            GPS_G_Force=joined_df['GPS_Acceleration'] * (1609.34/3600),
                            GPS_G_Force_g=(joined_df['GPS_Acceleration'] * (1609.34/3600))/9.81
                            )
                        joined_df['GPS_G_Force'] = round(joined_df['GPS_G_Force'], 9)
                        joined_df['GPS_G_Force_g'] = round(joined_df['GPS_G_Force_g'], 9)
                        #############################          GPS Speed          #############################

                        joined_df['Harsh Acceleration'] = joined_df.apply(harsh_acceleration, axis=1)
                        joined_df['Harsh Acceleration Unique'] = harsh_unique(joined_df, 'Harsh Acceleration')
                        joined_df['Harsh Braking'] = joined_df.apply(harsh_braking, axis=1)
                        joined_df['Harsh Braking Unique'] = harsh_unique(joined_df, 'Harsh Braking')
                        #########################################                      Recalcuate                      #########################################
                        
                        #Second Run
                        #2024-07-29 FIX Sporadic spikes/drops
                        joined_df = joined_df.reset_index(drop=True)
                        joined_df = remove_sporadic_spikes_drops(joined_df, True)
                        joined_df = joined_df.reset_index(drop=True)

                        #2024-08-13 FIX GPS Anomaly
                        joined_df = joined_df.reset_index(drop=True)
                        joined_df = remove_gps_error_speeds(joined_df)
                        joined_df = joined_df.reset_index(drop=True)

                        #########################################                      Recalcuate                      #########################################
                        joined_df = joined_df.assign(
                            Acceleration=joined_df['Speed'].diff() / joined_df['DateTime'].diff().dt.total_seconds(),
                            )
                        joined_df['Acceleration'] = round(joined_df['Acceleration'], 9)

                        #(1609.34/3600) => CONVERTING TO M/S
                        joined_df = joined_df.assign(
                            G_Force=joined_df['Acceleration'] * (1609.34/3600),
                            G_Force_g=(joined_df['Acceleration'] * (1609.34/3600))/9.81
                            )
                        joined_df['G_Force'] = round(joined_df['G_Force'], 9)
                        joined_df['G_Force_g'] = round(joined_df['G_Force_g'], 9)

                        #############################          GPS Speed          ############################# #2024-08-07
                        haversine_ft_s, haversine_mph = haversine_calc(joined_df)
                        #joined_df['Haversine FT/S'] = haversine_ft_s
                        #joined_df['Haversine MPH'] = haversine_mph
                        joined_df['Haversine'] = haversine_mph

                        vincenty_ft_s, vincenty_mph = vincenty_calc(joined_df)
                        #joined_df['Vincenty FT/S'] = vincenty_ft_s
                        #joined_df['Vincenty MPH'] = vincenty_mph
                        joined_df['Vincenty'] = vincenty_mph

                        joined_df['GPS Speed'] = joined_df[['Haversine', 'Vincenty']].mean(axis=1)

                        joined_df = joined_df.assign(
                            GPS_Acceleration=joined_df['GPS Speed'].diff() / joined_df['DateTime'].diff().dt.total_seconds(),
                            )
                        joined_df['GPS_Acceleration'] = round(joined_df['GPS_Acceleration'], 9)

                        #(1609.34/3600) => CONVERTING TO M/S
                        joined_df = joined_df.assign(
                            GPS_G_Force=joined_df['GPS_Acceleration'] * (1609.34/3600),
                            GPS_G_Force_g=(joined_df['GPS_Acceleration'] * (1609.34/3600))/9.81
                            )
                        joined_df['GPS_G_Force'] = round(joined_df['GPS_G_Force'], 9)
                        joined_df['GPS_G_Force_g'] = round(joined_df['GPS_G_Force_g'], 9)
                        #############################          GPS Speed          #############################

                        joined_df['Harsh Acceleration'] = joined_df.apply(harsh_acceleration, axis=1)
                        joined_df['Harsh Acceleration Unique'] = harsh_unique(joined_df, 'Harsh Acceleration')
                        joined_df['Harsh Braking'] = joined_df.apply(harsh_braking, axis=1)
                        joined_df['Harsh Braking Unique'] = harsh_unique(joined_df, 'Harsh Braking')
                        #########################################                      Recalcuate                      #########################################

                        joined_df = joined_df.set_index("UTS Depot")
                        joined_df = joined_df.join(depot, on="UTS Depot")
                        joined_df.index.name = 'UTS Depot'
                        joined_df['UTSDepot'] = joined_df.index
                        
                        joined_df.fillna(0.0, inplace=True)
                        
                        route_index = joined_df.columns.get_loc("Route")
                        
                        joined_df_depot = joined_df.pop('Depot')
                        joined_df.insert(route_index+1, "Depot", joined_df_depot)
                        joined_df_division = joined_df.pop('Division')
                        joined_df.insert(route_index+2, "Division", joined_df_division)
                        joined_df_utsdepot = joined_df.pop('UTSDepot')
                        joined_df.insert(route_index+3, "UTSDepot", joined_df_utsdepot)


                        joined_df['division_bronx'] = joined_df['Division'].map(lambda x: x == "Bronx")
                        joined_df['division_brooklyn'] = joined_df['Division'].map(lambda x: x == "Brooklyn")
                        joined_df['division_manhattan'] = joined_df['Division'].map(lambda x: x == "Manhattan")
                        joined_df['division_queens_north'] = joined_df['Division'].map(lambda x: x == "Queens North")
                        joined_df['division_queens_south'] = joined_df['Division'].map(lambda x: x == "Queens South")
                        joined_df['division_staten_island'] = joined_df['Division'].map(lambda x: x == "Staten Island")

                        joined_df['datetime_hour'] = joined_df['DateTime'].map(lambda x: datetime.datetime(x.year, x.month, x.day, x.hour))
                        joined_df['Hour of the Day'] = joined_df['DateTime'].map(lambda x: x.hour)

                        joined_df['map_grouping'] = map_grouping(joined_df, {'Harsh Acceleration': (True, 'Harsh Acceleration'), 'Harsh Braking': (True, 'Harsh Braking')})
                        joined_df['map_grouping_unique'] = map_grouping(joined_df, {'Harsh Acceleration Unique': (True, 'Harsh Acceleration'), 'Harsh Braking Unique': (True, 'Harsh Braking')})

                        curated_files_generated = True
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
    except Exception as e:
        raise e

class StdoutCapture:
    def __init__(self):
        self.data = []
    
    def write(self, text):
        self.data.append(text)
    
    def flush(self):
        pass

def main():
    try:
        stdout_capture = StdoutCapture()
        sys.stderr = stdout_capture
        #path = sys.argv[1]
        #output_path = sys.argv[2]
        path = r"C:\Users\1292786\OneDrive - The Metropolitan Transportation Authority\Documents\Bus Vehicle Diagnostics\Unzipped Files - Test"
        output_path = r"C:\Users\1292786\OneDrive - The Metropolitan Transportation Authority\Documents\Bus Vehicle Diagnostics\Curated Files - Test"
        
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
                            text.write('\n')
                    with open(error_file, 'a+') as text:
                        text.write(file)
                        text.write('\n')
                        text.write(str(e))
                        text.write('\n')
                    #raise e
                bar()
        sys.stderr = sys.__stderr__
        if len(stdout_capture.data) > 0:
            if not os.path.exists(warning_file):
                with open(warning_file, 'w') as text:
                    text.write("Warnings:")
            with open(warning_file, 'a+') as text:
                for warning in stdout_capture.data:
                    text.write('\n')
                    text.write(warning.split('\n')[0])
    except Exception as e:
        print(e)
        raise e
    

if __name__=='__main__':
    #main()
    try:
        testing = False
        ####################################################################
        #Production
        if not testing:
            path = sys.argv[1]
            output_path = sys.argv[2]
            num_processes = int(sys.argv[3])
        ####################################################################

        #path = r"C:\Users\1292786\OneDrive - The Metropolitan Transportation Authority\Documents\Bus Vehicle Diagnostics\Unzipped Files - CS - 2024-04-24 - 2024-04-30"
        #path = r"C:\Users\1292786\OneDrive - The Metropolitan Transportation Authority\Documents\Bus Vehicle Diagnostics\Unzipped Files - BP - 2024-04-24 - 2024-04-30"
        #path = r"C:\Users\1292786\OneDrive - The Metropolitan Transportation Authority\Documents\Bus Vehicle Diagnostics\Unzipped Files - CA - 2024-04-24 - 2024-04-30"
        #path = r"C:\Users\1292786\OneDrive - The Metropolitan Transportation Authority\Documents\Bus Vehicle Diagnostics\Unzipped Files - EC - 2024-04-24 - 2024-04-30"
        #path = r"C:\Users\1292786\OneDrive - The Metropolitan Transportation Authority\Documents\Bus Vehicle Diagnostics\Unzipped Files - JG - 2024-04-24 - 2024-04-30"
        #path = r"C:\Users\1292786\OneDrive - The Metropolitan Transportation Authority\Documents\Bus Vehicle Diagnostics\Unzipped Files - MQ - 2024-04-24 - 2024-04-30"
        
        #path = r"H:\Operator Awareness Tool\Unzipped Files - CS - 2024-04-24 - 2024-04-30"
        #path = r"H:\Operator Awareness Tool\Unzipped Files - BP - 2024-04-24 - 2024-04-30"
        #path = r"H:\Operator Awareness Tool\Unzipped Files - CA - 2024-04-24 - 2024-04-30"
        #path = r"H:\Operator Awareness Tool\Unzipped Files - EC - 2024-04-24 - 2024-04-30"
        #path = r"H:\Operator Awareness Tool\Unzipped Files - JG - 2024-04-24 - 2024-04-30"
        #path = r"H:\Operator Awareness Tool\Unzipped Files - MQ - 2024-04-24 - 2024-04-30"
        
        if testing:
            path = r"C:\Users\1292786\OneDrive - The Metropolitan Transportation Authority\Documents\Bus Vehicle Diagnostics\Unzipped Files - Test"
            output_path = r"C:\Users\1292786\OneDrive - The Metropolitan Transportation Authority\Documents\Bus Vehicle Diagnostics\Curated Files - Test"
        
        if not os.path.exists(output_path):
            print("Creating output folder.")
            os.mkdir(output_path)
        ####################################################################
        #Production
        else:
            if not testing:
                print("Folder already exists.")
                exit(-1)
        ####################################################################
        
        files = os.listdir(path)
        for i in range(len(files)):
            files[i] = (os.path.join(path, files[i]), output_path)
        
        # lst = [r"C:\Users\1292786\OneDrive - The Metropolitan Transportation Authority\Documents\Bus Vehicle Diagnostics\Zipped Files - Additional - Unzipped\CleverWare0006016240428.txt",
        #        r"C:\Users\1292786\OneDrive - The Metropolitan Transportation Authority\Documents\Bus Vehicle Diagnostics\Zipped Files - Additional - Unzipped\CleverWare0006016240429.txt"]
        # for file in lst:
        #     print(file)
        #     lambda_handler(file)

        ####################################################################
        #Production
        #num_processes = 2
        if not testing:
            pool = multiprocessing.Pool(processes=num_processes)

            with tqdm(total=len(files)) as pbar:
                # def update(*a):
                #     pbar.update()

                # pool.map_async(lambda_handler, files, callback=update)
                #pool.map_async(print_file, files, callback=update)
                for _ in pool.imap(lambda_handler, files):
                    pbar.update()

            pool.close()
            
            pool.join()
        ####################################################################
        if testing:
            with alive_bar(len(files)) as bar:
                for file in files:
                    #lambda_handler(os.path.join(path, file), output_path)
                    #lambda_handler(os.path.join(path, file))
                    print(file)
                    lambda_handler(file)
                    bar()
    except Exception as e:
        print(e)
        raise e