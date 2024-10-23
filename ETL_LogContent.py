import pandas as pd
import os
import numpy as np
import json

# Processing the entered time period
def date_transform(start_date,end_date):
    from datetime import datetime, timedelta
    start_date_dt = datetime.strptime(start_date, '%Y%m%d')
    end_date_dt = datetime.strptime(end_date,'%Y%m%d')
    day_list = [(start_date_dt + timedelta(days=i)).strftime('%Y%m%d') for i in range((end_date_dt - start_date_dt).days + 1)]
    return(day_list)

# Loading data based on the request time series
def data_extract(day_list,path):
    df = pd.read_json(path + day_list[0] + '.json', lines=True)
    df['Date'] = pd.to_datetime(day_list[0], format='%Y%m%d')
    print('Processed completed {}'.format(day_list[0]))
    print('----------------------')
    for i in day_list[1:]:
        data = pd.read_json(path + i + '.json', lines =True)
        data['Date'] = pd.to_datetime(i, format='%Y%m%d')
        df = pd.concat([df,data])
        print('Processed completed {}'.format(i))
        print('----------------------')
    print('Showing data sample')
    print('----------------------')
    print(df[:100])
    print('----------------------')
    print('Showing data structure')
    print('----------------------')
    df.info()
    print('----------------------')
    return(df)

# Processing data
def data_transform(df):
    df = pd.json_normalize(df['_source']).join(df[['Date']])
    # create 'Type' column
    df['Type'] = df['AppName'].map({
    'CHANNEL': 'Television',
    'DSHD': 'Television',
    'KPLUS': 'Television',
    'KPlus': 'Television',
    'VOD': 'Feature film',
    'FIMS_RES': 'Feature film',
    'BHD_RES': 'Feature film',
    'VOD_RES': 'Feature film',
    'FIMS': 'Feature film',
    'BHD': 'Feature film',
    'DANET': 'Feature film',
    'RELAX': 'Entertainment',
    'CHILD': 'Kid',
    'SPORT': 'Sport'
    }).fillna('Error')
    print('Filter out contract = 0 and type is Error')
    print('----------------------')
    df = df[(df['Contract'] != '0') & (df['Type'] != 'Error')]
    print('Caculating the Total Duration Per Contract')
    print('----------------------')   
    df = df.groupby(['Contract','Type','Date'])['TotalDuration'].sum().reset_index(name = 'TotalDuration_PerContract')
    result = df.pivot_table(
        index=['Contract','Date'],
        columns='Type',
        values='TotalDuration_PerContract',
        aggfunc='sum',
        fill_value=0
        ).reset_index()
    print('Showing result output')
    print('----------------------') 
    print(result[:10])
    print('----------------------') 
    return(result)

# Caculate the metrics 
def the_metrics(result):
    # Caculate the most watch
    print('Caculating the most watch')
    print('----------------------')
    result['most_watch'] = result[['Entertainment', 'Feature film', 'Kid', 'Sport', 'Television']].apply(lambda row: 0 if row.eq(0).all() else row.idxmax(), axis=1)
    # Caculate the customer_taste
    print('Caculating the customer taste')
    print('----------------------')
    def concat_columns(row):
        return '-'.join([col for col in ['Entertainment', 'Feature film', 'Kid', 'Sport', 'Television'] if row[col] != 0])
    result['customer_taste'] = result.apply(concat_columns, axis=1)
    # Caculate the customer activeness
    print('Caculating the customer activeness')
    print('----------------------')
    result['customer_activeness'] = result.groupby('Contract')['Contract'].transform('count').apply(lambda x: 'high' if x > 4 else 'low')
    print('Showing result output')
    print('----------------------') 
    print(result[:10])
    print('----------------------')     
    return(result)

# Loading Data to CSV
def data_load(result,save_path):
    print('Saving result output')
    print('----------------------') 
    result.to_csv(save_path, index = False)

# Importing ETL data to MySQL
def import_to_mysql(result):
    from sqlalchemy import create_engine
    # MySQL connection details
    user = 'root'  # Replace with your MySQL username
    password = ''  # Replace with your MySQL password
    host = 'localhost'  # Your MySQL host
    port = '3306'  # Default MySQL port
    database = 'etl_data'  # The database where you want to store the data
    # Create the connection URL
    connection_url = f'mysql+pymysql://{user}:{password}@{host}:{port}/{database}'
    # Create SQLAlchemy engine
    engine = create_engine(connection_url)
    # Import the DataFrame to MySQL
    result.to_sql('LogContent_ETL_data', con=engine, if_exists='replace', index=False)
    
# ETL Process
def main_task(start_date,end_date,path,save_path):
    print('----------------------')
    print('Transforming date')
    print('----------------------')
    day_list = date_transform(start_date,end_date)
    print('Transforming date completely')
    print('----------------------')
    print('Extracting data')
    print('----------------------')
    df = data_extract(day_list,path)
    print('Extracting data completely')
    print('----------------------')
    print('Transforming data')
    print('----------------------')
    result = data_transform(df)
    print('Transforming data completely')
    print('----------------------')
    print('Caculating the metrics')
    print('----------------------')
    result = the_metrics(result)
    print('Caculating the metrics completely')
    print('----------------------')    
    print('Loading data to CSV file')
    print('----------------------')
    data_load(result,save_path)
    print('Loading data to CSV file completely')
    print('----------------------')
    print('Importing data to MySQL')
    print('----------------------')    
    import_to_mysql(result)
    print('Importing data to MySQL completely')
    print('----------------------')        
    return print('Task run successfully')

# Enter 'Path' containing data folder
path = 'C:\\Nguyễn Minh Khôi - EEC01\\study_data_DE\\Big Data\\CLass 4 - ETL Pipeline\\log_content(short)\\'
# Enter start date and end date by according to syntax day = {yyyymmdd}
start_date = '20220401'
end_date = '20220405'
# Enter 'save_path' storing data passed ETL process
save_path = 'C:\\Nguyễn Minh Khôi - EEC01\\study_data_DE\\Big Data\\CLass 4 - ETL Pipeline\\ETL_LogContent\\Clean_data.csv'
# Run to begin ETL process
main_task(start_date,end_date,path,save_path)