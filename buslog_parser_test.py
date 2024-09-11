from io import StringIO
import zipfile
import pandas as pd
import numpy as np
import requests
import io
import os
import datetime
from tqdm import tqdm

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

def lambda_handler1():
    try:
        #with open(r"C:\Users\mjper\OneDrive\Documents\MTA\Operator Awareness Tool (OWARE)\Sample of Bus Vehicle Diagnostics and GPS log from Clever Devices IVN system\CleverWare0000323240107.txt.zip", 'rb') as f:
        #    file = f
        #obj = {"Body": file}
        #obj = {"Body": open(r"C:\Users\mjper\OneDrive\Documents\MTA\Operator Awareness Tool (OWARE)\Sample of Bus Vehicle Diagnostics and GPS log from Clever Devices IVN system\CleverWare0000323240107.txt.zip", 'rb')}
        #obj = {"Body": open(r"C:\Users\mjper\OneDrive\Documents\MTA\Operator Awareness Tool (OWARE)\Original\Sample of Bus Vehicle Diagnostics and GPS log from Clever Devices IVN system\CleverWare0000323240107.txt.zip", 'rb')}
        obj = {"Body": open(r"C:\Users\mjper\OneDrive\Documents\MTA\Operator Awareness Tool (OWARE)\Original\Sample of Bus Vehicle Diagnostics and GPS log from Clever Devices IVN system\CleverWare0007118240107.txt.zip", 'rb')}
        #obj = {"Body": open(r"D:\Documents\MTA\Bus Data\Data\CleverWare0000323240107.txt.zip", 'rb')}
        with io.BytesIO(obj["Body"].read()) as tf:
            # rewind the file
            tf.seek(0)

            # Read the file as a zipfile and process the members
            with zipfile.ZipFile(tf, mode='r') as zipf:
                for file in zipf.infolist():
                    fileName = file.filename
                    print(fileName)
                    #putFile = s3.put_object(Bucket=bucket2, Key=fileName, Body=zipf.read(file))
                    #putObjects.append(putFile)
                    #print(putFile)
                    unpacked = open('test.txt', 'w')
                    #unpacked.write(zipf.read(file).decode('utf-16'))
                    unpacked.write(zipf.read(file).decode('utf-8'))
                    unpacked.close()
                    return {'Key': fileName, 'Body': zipf.read(file)}

    except Exception as e:
        print(e)
        raise e

def lambda_handler2():
    resp = lambda_handler1()
    key=resp['Key']
    # Remove .txt from key name
    key2 = key.replace('.txt', '')
    #x = resp['Body'].decode('utf-16')
    x = resp['Body'].decode('utf-8')
    count = x.count('\r\n')+1

    df = pd.concat([chunk for chunk in tqdm(pd.read_csv(io.StringIO(x), sep='|', header=None, chunksize=1000), desc='Loading data', total=count//1000)])
    # Open file as CSV
    #df = pd.read_csv(x, sep='|', header=None)
    
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
    #df.to_csv('test.csv', index=False)
    bus_id = int(key2[13:17].replace('.txt', ''))
    print(f"Vehicle ID: {bus_id}")
    
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
        if ts > datetime.datetime(2024, 1, 6, 6, 51, 30):
            checkpoint = True
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
    
    #df_framework.to_csv('test.csv', index=False)
    
    # Get Date in Cleverware file
    date_test = df_framework['Date'].unique()
    valid_dates = [d for d in date_test if pd.to_datetime(d, errors='coerce') is not pd.NaT]
    
    # Get Bus Operator
    bus_operator = []
    data_dict = {}
    for i in valid_dates:
        #print(i)
        response = requests.get(url.format(i))
        x = response.text
        rawData = pd.read_csv(io.StringIO(x), header=0)
        #print(rawData.head(20))
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
        
        #rawData.to_csv("rawData.csv")
        # Change Bus column to string
        rawData['Bus'] = rawData['Bus'].astype("string")
        # Get rows with Bus ID
        #get_row_test = rawData[rawData['Bus'].str.match(bus_id)]
        #get_row_test = rawData[rawData['Bus'].str.match(str(bus_id))]
        get_row_test = rawData[rawData['Bus'] == str(bus_id)]
        #print(get_row_test)
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
        else:
            print(f"Time {i} or Bus ID {bus_id} don't match between yard-express and Cleverware log. Please check your file and try again.")
    
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
                #df_copy.to_csv("df_copy_before.csv", index=False)
                df_copy = df_copy.dropna()
                # Speed Calculations from ft/s to mph
                df_copy['Speed'] = round(df_copy['Speed'] * 0.681818, 9)
                #df_copy.to_csv("df_copy_after.csv", index=False)
                # Calculate time difference between rows
                df_copy['Time_Diff'] = df_copy['DateTime'].diff().dt.total_seconds()
                # Drop rows where time diff < 0
                df_copy = df_copy[df_copy['Time_Diff'] > 0]
                df_copy2 = df_copy.assign(
                    Acceleration=df_copy['Speed'].diff() / df_copy['DateTime'].diff().dt.total_seconds()
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

                # Drop empty dataframe
                if joined_df.shape[0] > 0:
                    joined_df['Bus_ID'] = bus_id
                    #joined_df.to_csv("joined_df_before.csv", index=False)
                    if 1527 <= int(bus_id) <= 2716:
                        joined_df = joined_df[joined_df['Speed'] <= 60]
                        # Upload Dataframe to s3
                        #csv_buffer = StringIO()
                        csv_buffer = f"joined_df_after_{bus_id}_{i}_{r}.csv"
                        joined_df.to_csv(csv_buffer, index=False)
                        # Change Bucket  and Key
                        # s3.put_object(Body=csv_buffer.getvalue(), Bucket='mtabuslogs-curated-rev2',
                        #               Key='{}-{}-cleverwaredata.csv'.format(key2, i))
                        print("Parsing Complete")
                        print(f'This is an Express bus with Bus ID: {bus_id}')
                    else:
                        joined_df = joined_df[joined_df['Speed'] <= 40]
                        # Upload Dataframe to s3
                        #csv_buffer = StringIO()
                        csv_buffer = f"joined_df_after_{bus_id}_{i}_{r}.csv"
                        joined_df.to_csv(csv_buffer, index=False)
                        # Change Bucket  and Key
                        # s3.put_object(Body=csv_buffer.getvalue(), Bucket='mtabuslogs-curated-rev2',
                        #               Key='{}-{}-cleverwaredata.csv'.format(key2, i))
                        print("Parsing Complete")
                        print(f'This is not an Express bus with Bus ID: {bus_id}')
                else:
                    print("DataFrame is empty, export to s3 skipped.")

if __name__=='__main__':
    lambda_handler2()