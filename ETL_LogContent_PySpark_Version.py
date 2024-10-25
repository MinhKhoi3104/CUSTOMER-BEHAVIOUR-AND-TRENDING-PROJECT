import findspark
findspark.init()
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from datetime import datetime
from datetime import date, timedelta

spark = SparkSession.builder.getOrCreate()

# Processing the entered time period
def date_transform(start_date,end_date):
    start_date_dt = datetime.strptime(start_date, '%Y%m%d')
    end_date_dt = datetime.strptime(end_date,'%Y%m%d')
    day_list = [(start_date_dt + timedelta(days=i)).strftime('%Y%m%d') for i in range((end_date_dt - start_date_dt).days + 1)]
    return(day_list)

# Loading data based on the request time series
def data_extract(day_list,path):
    df = spark.read.json(path + day_list[0] + '.json')
    df = df.withColumn('Date', lit(datetime.strptime(day_list[0], '%Y%m%d')))
    print('Processed completed {}'.format(day_list[0]))
    print('----------------------')
    for i in day_list[1:]:
        data = spark.read.json(path + i + '.json')
        data = data.withColumn('Date', lit(datetime.strptime(i, '%Y%m%d')))
        df = df.union(data)
        print('Processed completed {}'.format(i))
        print('----------------------')
        print('Showing data sample')
    print('----------------------')
    print(df.show())
    print('----------------------')
    print('Showing data structure')
    print('----------------------')
    df.printSchema()
    print('----------------------')
    return(df)

# Processing data
def data_transform(df):
    df = df.select("Date", *[col("_source." + c).alias(c) for c in df.select("_source.*").columns])
    df = df.withColumn("Type",
        when((col("AppName") == 'CHANNEL') | (col("AppName") =='DSHD')| (col("AppName") =='KPLUS')| 
            (col("AppName") =='KPlus'), "Truyền Hình")
        .when((col("AppName") == 'VOD') | (col("AppName") =='FIMS_RES')| (col("AppName") =='BHD_RES')| 
            (col("AppName") =='VOD_RES')| (col("AppName") =='FIMS')| (col("AppName") =='BHD')| 
            (col("AppName") =='DANET'), "Phim Truyện")
        .when((col("AppName") == 'RELAX'), "Giải Trí")
        .when((col("AppName") == 'CHILD'), "Thiếu Nhi")
        .when((col("AppName") == 'SPORT'), "Thể Thao")
        .otherwise("Error"))
    df = df.select('Contract','Date','Type','TotalDuration')
    df = df.filter(df.Contract != '0' )
    df = df.filter(df.Type != 'Error')
    print('Filter out contract = 0 and type is Error')
    print('----------------------')
    

# Enter 'Path' containing data folder
path = 'C:\\Nguyễn Minh Khôi - EEC01\\study_data_DE\\Big Data\\CLass 4 - ETL Pipeline\\log_content(short)\\'
# Enter start date and end date by according to syntax day = {yyyymmdd}
start_date = '20220401'
end_date = '20220405'
# Enter 'save_path' storing data passed ETL process
save_path = 'C:\\Nguyễn Minh Khôi - EEC01\\study_data_DE\\Big Data\\CLass 4 - ETL Pipeline\\ETL_LogContent\\Clean_data.csv'


