import findspark
findspark.init()
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from datetime import datetime
from datetime import date, timedelta
import os
import pandas as pd
from pyspark.sql.window import *

spark = SparkSession.builder.getOrCreate()


path = 'C:\\Nguyễn Minh Khôi - EEC01\\study_data_DE\\Big Data\\Class 7 - Final Project\\log_search\\'
category_path = "C:\\Nguyễn Minh Khôi - EEC01\\study_data_DE\\Big Data\\Class 7 - Final Project\\keyword_category.csv"
# Nhập ngày bắt đầu trong tháng 6
start_date_6 = '20220601'
# Nhập thời gian kết thúc tháng 6
end_date_6 = '20220605'
# Nhập ngày bắt đầu trong tháng 7
start_date_7 = '20220701'
# Nhập thời gian kết thúc tháng 7
end_date_7 = '20220705'
def date_transform(start_date_6,end_date_6,start_date_7,end_date_7):
    start_date_6_dt = datetime.strptime(start_date_6, '%Y%m%d')
    end_date_6_dt = datetime.strptime(end_date_6,'%Y%m%d')
    day_list_6 = [(start_date_6_dt + timedelta(days=i)).strftime('%Y%m%d') for i in range((end_date_6_dt - start_date_6_dt).days + 1)]
    start_date_7_dt = datetime.strptime(start_date_7, '%Y%m%d')
    end_date_7_dt = datetime.strptime(end_date_7,'%Y%m%d')
    day_list_7 = [(start_date_7_dt + timedelta(days=i)).strftime('%Y%m%d') for i in range((end_date_7_dt - start_date_7_dt).days + 1)]
    day_list = day_list_6 + day_list_7
    return(day_list)

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
    # keywork_most_search.write.csv('C:\\Nguyễn Minh Khôi - EEC01\\study_data_DE\\Big Data\\Class 7 - Final Project\\keyword_most_search')
    return(keywork_most_search)

#Đọc file Category theo keywords
def read_category_file(category_path):
    category = spark.read.csv(category_path, header = True)
    return(category)
#Tính toán cá nhân tháng 6 và 7
df = df.filter(col('category') == 'enter')
df = df.select('user_id','keyword','Date')
df = df.withColumn('month', month('Date'))
df = df.groupBy('keyword','user_id','month').agg(count('keyword').alias('count'))
df = df.join(category, on = 'keyword').drop('No')
df = df.withColumn('rank', row_number().over(Window.partitionBy('user_id','month').orderBy(col('count').desc())))
df = df.filter(col('rank') == '1')
data_month6 = df.filter(col('month') == '6').withColumnRenamed('keyword', 'most_search_month6') \
    .withColumnRenamed('Category', 'category_month6')
data_month6 = data_month6.select('user_id', 'most_search_month6', 'category_month6')
data_month7 = df.filter(col('month') == '7').withColumnRenamed('keyword', 'most_search_month7') \
    .withColumnRenamed('Category', 'category_month7')
data_month7 = data_month7.select('user_id', 'most_search_month7', 'category_month7')
data = data_month6.join(data_month7, on = 'user_id')
data = data.withColumn('Trending_type', 
                       when((col('most_search_month6') == col('most_search_month7')), 'Unchanged')
                       .otherwise('Changed'))
data = data.withColumn('Previous', 
                       when((col('most_search_month6') == col('most_search_month7')), 'Unchanged')
                       .otherwise(concat(col('category_month6'), lit('-'),col('category_month7'))))
sample = data.filter((col('Trending_type') == 'Unchanged') & (col('most_search_month6') != col('most_search_month7')))



#Load the keywork_category
def keyword_category(category_path):
    keyword_category = pd.read_excel(category_path, index_col=None)
    keyword_category = pd.DataFrame(keyword_category)
    return(keyword_category)

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


