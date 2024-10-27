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
    print(df.show(20))
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
            (col("AppName") =='KPlus'), "Television")
        .when((col("AppName") == 'VOD') | (col("AppName") =='FIMS_RES')| (col("AppName") =='BHD_RES')| 
            (col("AppName") =='VOD_RES')| (col("AppName") =='FIMS')| (col("AppName") =='BHD')| 
            (col("AppName") =='DANET'), "Feature film")
        .when((col("AppName") == 'RELAX'), "Entertainment")
        .when((col("AppName") == 'CHILD'), "Kid")
        .when((col("AppName") == 'SPORT'), "Sport")
        .otherwise("Error"))
    df = df.select('Contract','Date','Type','TotalDuration')
    df = df.filter(df.Contract != '0')
    df = df.filter(df.Type != 'Error')
    print('Filter out contract = 0 and type is Error')
    print('----------------------')
    df = df.groupBy('Contract','Type', 'Date').sum('TotalDuration').withColumnRenamed('sum(TotalDuration)','TotalDuration')
    print('Sum TotalDuration according Contract and Type')
    print('----------------------')
    result = df.groupBy('Contract').pivot('Type').sum('TotalDuration').fillna(0)
    print('Pivot Type')
    print('----------------------')
    # Caculate the most watch
    columns_to_compare = ['Television', 'Feature film', 'Entertainment','Kid','Sport']
    result = result.withColumn('most_watch', greatest(*columns_to_compare))
    conditions = [when(col('most_watch') == col(c), c) for c in columns_to_compare]
    result = result.withColumn('most_watch', coalesce(*conditions))
    print('Caculating the most watch')
    print('----------------------')
    # Caculate the customer_taste
    conditions = [
        when(col('Entertainment') != 0, 'Entertainment'),
        when(col('Feature film') != 0, 'Feature film'),
        when(col('Kid') != 0, 'Kid'),
        when(col('Sport') != 0, 'Sport'),
        when(col('Television') != 0, 'Television')]
    result = result.withColumn('customer_taste', concat_ws('-', *conditions))
    print('Caculating the customer taste')
    print('----------------------')
    # Caculate the customer activeness
    contract_counts = df.groupBy('Contract').agg(count('Contract').alias('Contract_count'))
    result = result.join(contract_counts, 'Contract', 'left') \
    .withColumn('customer_activeness', when(col('Contract_count') > 4, 'high').otherwise('low'))
    return(result)

# Loading Data to CSV
def data_load(result,save_path):
    print('Saving result output')
    print('----------------------') 
    result.write.csv(save_path,header = True)

# Importing ETL data to MySQL


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
    print('Showing data sample')
    print('----------------------')  
    result.show(20)
    print('----------------------') 
    print('Loading data to CSV file')
    print('----------------------')
    data_load(result,save_path)
    print('Loading data to CSV file completely')
    print('----------------------')    
    import_to_mysql(result)
    print('Importing data to MySQL completely')
    print('----------------------')        
    return print('Task run successfully')

# Enter 'Path' containing data folder
path = 'C:\\Nguyễn Minh Khôi - EEC01\\study_data_DE\\Big Data\\CLass 4 - ETL Pipeline\\log_content(short)\\'
# Enter start date and end date by according to syntax day = {yyyymmdd}
start_date = '20220401'
end_date = '20220410'
# Enter 'save_path' storing data passed ETL process
save_path = 'C:\\Nguyễn Minh Khôi - EEC01\\study_data_DE\\Big Data\\CLass 4 - ETL Pipeline\\ETL_LogContent\\Clean_data.csv'


