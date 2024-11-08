import findspark
findspark.init()
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from datetime import datetime
from datetime import date, timedelta
import os

spark = SparkSession.builder.getOrCreate()

# Processing the entered time period
def date_transform(start_date_6,end_date_6,start_date_7,end_date_7):
    start_date_6_dt = datetime.strptime(start_date_6, '%Y%m%d')
    end_date_6_dt = datetime.strptime(end_date_6,'%Y%m%d')
    day_list_6 = [(start_date_6_dt + timedelta(days=i)).strftime('%Y%m%d') for i in range((end_date_6_dt - start_date_6_dt).days + 1)]
    start_date_7_dt = datetime.strptime(start_date_7, '%Y%m%d')
    end_date_7_dt = datetime.strptime(end_date_7,'%Y%m%d')
    day_list_7 = [(start_date_7_dt + timedelta(days=i)).strftime('%Y%m%d') for i in range((end_date_7_dt - start_date_7_dt).days + 1)]
    day_list = day_list_6 + day_list_7
    return(day_list)

# Loading data based on the request time series
def data_extract(day_list,path):
    list_folder = os.listdir(path)
    folder_path = path + list_folder[0]
    parquet_files = [os.path.join(folder_path, f) for f in os.listdir(folder_path) if f.endswith('.parquet')]
    df = spark.read.parquet(parquet_files[0])
    df = df.withColumn('Date', lit(datetime.strptime(day_list[0], '%Y%m%d')))
    for i in list_folder[1:]:
        folder_path = path + i
        parquet_files = [os.path.join(folder_path, f) for f in os.listdir(folder_path) if f.endswith('.parquet')]
        data = spark.read.parquet(parquet_files[0]) 
        data = data.withColumn('Date', lit(datetime.strptime(i, '%Y%m%d')))
        df = df.union(data)
    return(df)

# Summary of the most searched keywords
def keyword_most_search(df):
    df = df.withColumn('keyword', lower(df['keyword']))
    keywork_most_search = df.groupBy('keyword','category').agg(count('keyword').alias('count'))
    keywork_most_search =  keywork_most_search.orderBy(col('count').desc())
    print('Loading data to CSV')
    print('----------------------')    
    keywork_most_search.write.csv('C:\\Nguyễn Minh Khôi - EEC01\\study_data_DE\\Big Data\\Class 7 - Final Project\\keyword_most_search')
    return(keywork_most_search)

def import_to_mysql(result):
    from pyspark.sql.types import StructType
 # Flatten struct columns if they exist
    for field in result.schema.fields:
        if isinstance(field.dataType, StructType):
            # Lấy tất cả các trường con từ struct
            for subfield in field.dataType.fields:
                column_name = f"{field.name}_{subfield.name}"
                result = result.withColumn(column_name, col(f"{field.name}.{subfield.name}"))
            # Drop cột struct gốc
            result = result.drop(field.name)
    # MySQL connection details
    user = 'root'  # Replace with your MySQL username
    password = ''  # Replace with your MySQL password
    host = 'localhost'  # Your MySQL host
    port = '3306'  # Default MySQL port
    database = 'log_search'  # The database name
    table = 'keyword_most_search'  # Table name
    # MySQL connection properties
    mysql_properties = {
        'driver': 'com.mysql.cj.jdbc.Driver',
        'user': user,
        'password': password,
        'url': f'jdbc:mysql://{host}:{port}/{database}'}
    # Write DataFrame to MySQL
    result.write \
        .mode('overwrite') \
        .format('jdbc') \
        .option('driver', mysql_properties['driver']) \
        .option('url', mysql_properties['url']) \
        .option('dbtable', table) \
        .option('user', mysql_properties['user']) \
        .option('password', mysql_properties['password']) \
        .save()
    return print('Loading data to MySQL completely')

# Main Task
def main_task(path,start_date_6,end_date_6,start_date_7,end_date_7):
    print('----------------------')
    print('Transforming date')
    day_list = date_transform(start_date_6,end_date_6,start_date_7,end_date_7)
    print('----------------------')
    print('Transforming date completely')
    print('----------------------')
    print('Extracting data')
    print('----------------------')
    df = data_extract(day_list,path)
    print('Extracting data completely')
    print('----------------------')
    print('Caculating the keyword most search')
    print('----------------------')   
    result = keyword_most_search(df)
    result.show(30)
    print('Caculating the keyword most search comletely')
    print('----------------------')
    print('Loading data to MySQL')
    print('----------------------') 
    import_to_mysql(result)
    print('----------------------')
    return print('Task run successfully')

    
# Enter the path containing data
path = 'C:\\Nguyễn Minh Khôi - EEC01\\study_data_DE\\Big Data\\Class 7 - Final Project\\log_search\\'
# Enter the start date in June
start_date_6 = '20220601'
# Enter the end date in June
end_date_6 = '20220605'
# Enter the start date in July
start_date_7 = '20220701'
# Enter the end date in July
end_date_7 = '20220705'