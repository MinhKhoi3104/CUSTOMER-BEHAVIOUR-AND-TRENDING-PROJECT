import findspark
findspark.init()
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from datetime import datetime
from datetime import date, timedelta
import os
from pyspark.sql.window import *

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

# Read the Category file by keywords
def read_category_file(category_path):
    category = spark.read.csv(category_path, header = True)
    return(category)

# Calculate and compare access information for each user in June and July
def compare_June_and_July(df, category):
    df = df.filter(col('category') == 'enter').select('user_id','keyword','Date')
    # Create a 'month' column based on the values in the 'Date' column
    df = df.withColumn('month', month('Date'))
    # Calculate the total number of access grouped by 'keyword', 'user_id', and 'month'
    df = df.groupBy('keyword','user_id','month').agg(count('keyword').alias('count'))
    df = df.join(category, on = 'keyword').drop('No')
    # Rank the keyword access by each user and month
    df = df.withColumn('rank', row_number().over(Window.partitionBy('user_id','month').orderBy(col('count').desc())))
    df = df.filter(col('rank') == '1')
    # Filter information for June
    data_month6 = df.filter(col('month') == '6').withColumnRenamed('keyword', 'most_search_month6') \
        .withColumnRenamed('Category', 'category_month6')
    data_month6 = data_month6.select('user_id', 'most_search_month6', 'category_month6')
    # Filter information for July
    data_month7 = df.filter(col('month') == '7').withColumnRenamed('keyword', 'most_search_month7') \
        .withColumnRenamed('Category', 'category_month7')
    data_month7 = data_month7.select('user_id', 'most_search_month7', 'category_month7')
    # Join 2 tables of data for June and July
    data = data_month6.join(data_month7, on = 'user_id')
    # Calculate Trending_type to see if the most searched keywords of users changed between June and July
    data = data.withColumn('Trending_type', 
                       when((col('category_month6') == col('category_month7')), 'Unchanged')
                       .otherwise('Changed'))
    # Calculate Previous to understand the changes in user access
    data = data.withColumn('Previous', 
                       when((col('category_month6') == col('category_month7')), 'Unchanged')
                       .otherwise(concat(col('category_month6'), lit('-'),col('category_month7'))))
    return(data)


def import_to_mysql(data):
    from pyspark.sql.types import StructType
 # Flatten struct columns if they exist
    for field in data.schema.fields:
        if isinstance(field.dataType, StructType):
            # Lấy tất cả các trường con từ struct
            for subfield in field.dataType.fields:
                column_name = f"{field.name}_{subfield.name}"
                data = data.withColumn(column_name, col(f"{field.name}.{subfield.name}"))
            # Drop cột struct gốc
            data = data.drop(field.name)
    # MySQL connection details
    user = 'root'  # Replace with your MySQL username
    password = ''  # Replace with your MySQL password
    host = 'localhost'  # Your MySQL host
    port = '3306'  # Default MySQL port
    database = 'log_search'  # The database name
    table = 'LogSearch_Data_Final'  # Table name
    # MySQL connection properties
    mysql_properties = {
        'driver': 'com.mysql.cj.jdbc.Driver',
        'user': user,
        'password': password,
        'url': f'jdbc:mysql://{host}:{port}/{database}'}
    # Write DataFrame to MySQL
    data.write \
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
    df.show(30)
    print('Extracting data completely')
    print('----------------------')
    print('Read the Category file by keywords')
    print('----------------------')   
    category = read_category_file(category_path)
    category.show(30)
    print('Read the Category file by keywords comletely')
    print('----------------------')
    print('Compare access information for each user in June and July')
    print('----------------------')
    data = compare_June_and_July(df, category)
    data.show(30)
    print('Compare access information for each user in June and July completely')
    print('----------------------')
    print('Loading data to MySQL')
    print('----------------------') 
    import_to_mysql(data)
    print('----------------------')
    return print('Task run successfully')

# Enter the path containing data
path = 'C:\\Nguyễn Minh Khôi - EEC01\\study_data_DE\\Big Data\\Class 7 - Final Project\\log_search\\'
# Enter the path containing category data
category_path = "C:\\Nguyễn Minh Khôi - EEC01\\study_data_DE\\Big Data\\Class 7 - Final Project\\keyword_category.csv"
# Enter the start date in June
start_date_6 = '20220601'
# Enter the end date in June
end_date_6 = '20220614'
# Enter the start date in July
start_date_7 = '20220701'
# Enter the end date in July
end_date_7 = '20220714'
