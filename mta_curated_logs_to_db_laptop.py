import os
import csv
import pandas as pd
from sqlalchemy import create_engine, types
from alive_progress import alive_bar
import sys
#from collections import defaultdict

__location__ = os.path.realpath(
    os.path.join(os.getcwd(), os.path.dirname(__file__)))

def get_mysql_credentials():
    user = "temp"
    password = "temp"
    res = []
    with open(os.path.join(__location__, "db_credentials.txt"), 'r') as f:
        res = f.read().strip().split()
    if len(res) == 2:
        user = res[0]
        password = res[1]
    return user, password

def upload_to_mysql_db(db_schema, db_table, path):
    db_user, db_password = get_mysql_credentials()
    #schema = "test_schema"
    schema = db_schema
    #table = "mta_curated_data_test"
    table = db_table
    engine = create_engine(f'mysql://{db_user}:{db_password}@localhost/{schema}') # enter your password and database names here

    header_names = [
        "Lat",
        "Long",
        "Heading",
        "DateTime",
        "Operator",
        "Route",
        "Depot",
        "Division",
        "UTSDepot",
        "Bus_ID",
        "Speed",
        "Acceleration",
        "G_Force",
        "G_Force_g",
        "Haversine",        #2024-08-07
        "Vincenty",         #2024-08-07
        "GPS Speed",        #2024-08-07
        "GPS_Acceleration", #2024-08-07
        "GPS_G_Force",      #2024-08-07
        "GPS_G_Force_g",    #2024-08-07
        "Harsh Acceleration",
        "Harsh Acceleration Unique",
        "Harsh Braking",
        "Harsh Braking Unique",
        "division_bronx",
        "division_brooklyn",
        "division_manhattan",
        "division_queens_north",
        "division_queens_south",
        "division_staten_island",
        "datetime_hour",
        "Hour of the Day",
        "map_grouping",
        "map_grouping_unique"
    ]
    dtypes = {
        "Lat": float,
        "Long": float,
        "Heading": float,
        "Operator": int,
        "Route": str,
        "Depot": str,
        "Division": str,
        "UTSDepot": str,
        "Bus_ID": int,
        "Speed": float,
        "Acceleration": float,
        "G_Force": float,
        "G_Force_g": float,
        "Haversine": float,         #2024-08-07
        "Vincenty": float,          #2024-08-07
        "GPS Speed": float,         #2024-08-07
        "GPS_Acceleration": float,  #2024-08-07
        "GPS_G_Force": float,       #2024-08-07
        "GPS_G_Force_g": float,     #2024-08-07
        "Harsh Acceleration": bool,
        "Harsh Acceleration Unique": bool,
        "Harsh Braking": bool,
        "Harsh Braking Unique": bool,
        "division_bronx": bool,
        "division_brooklyn": bool,
        "division_manhattan": bool,
        "division_queens_north": bool,
        "division_queens_south": bool,
        "division_staten_island": bool,
        "Hour of the Day": int,
        "map_grouping": str,
        "map_grouping_unique": str,
    }

    date_columns = ["DateTime", "datetime_hour"]
    
    print("Parsing...")
    total_lines = 0
    files = os.listdir(path)
    with alive_bar(len(files)) as bar:
        for file in files:
            #print(file)
            #df = pd.read_csv(os.path.join(path, file),sep=',',quotechar='\'',encoding='utf8') # Replace Excel_file_name with your excel sheet name
            df = pd.read_csv(os.path.join(path, file),sep=',',quotechar='\'',encoding='utf8', dtype=dtypes, parse_dates=date_columns, date_format="%Y-%m-%d %H:%M:%S")
            df.to_sql(table,con=engine,index=False,if_exists='append') # Replace Table_name with your sql table name
            total_lines += df.shape[0]
            bar()
    # for file in os.listdir(r"C:\Users\mjper\OneDrive\Documents\MTA\Operator Awareness Tool (OWARE)\Code\Curated_Files"):
    #     print(file)
    #     df = pd.read_csv(os.path.join(r"C:\Users\mjper\OneDrive\Documents\MTA\Operator Awareness Tool (OWARE)\Code\Curated_Files", file),sep=',',quotechar='\'',encoding='utf8') # Replace Excel_file_name with your excel sheet name
    #     df.to_sql(table,con=engine,index=False,if_exists='append') # Replace Table_name with your sql table name
    #     print("COMPLETED")
    print(f"Total Lines: {total_lines}")
    print("Program has finished...")

def main():
    #path = r"C:\Users\mjper\OneDrive\Documents\MTA\Operator Awareness Tool (OWARE)\Code\Curated_Files"
    schema = sys.argv[1]
    #schema = "test_schema"
    table = sys.argv[2]
    #table = "mta_curated_data_demo"
    #table = "mta_curated_data_demo_g_force"
    path = sys.argv[3]
    #path = r"C:\Users\mjper\OneDrive\Documents\MTA\Operator Awareness Tool (OWARE)\Curated Files - All - 2024-04-24 - 2024-04-30 - Updated"
    print(f"Schema: {schema}")
    print(f"Table: {table}")
    print(f"Path: {path}")
    upload_to_mysql_db(schema, table, path)

if __name__=='__main__':
    main()