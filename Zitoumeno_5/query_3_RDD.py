# Query 3 - Solution (RDD)

# Imports
from pyspark import SparkConf
from pyspark.sql import SparkSession

def extract_number(s):
    li = s.split(",")
    res = li[0][1:] + li[1]
    return res   # string


def person_income_per_year(file_name_1, file_name_2):
    '''Find mean personal income per year in LA.'''
    conf = SparkConf().setAppName("FindIncomePerYear") \
        .set("spark.executor.memory", "2g") \
        .set("spark.driver.memory", "2g")
    ss = SparkSession.builder.config(conf=conf).getOrCreate()
    # Read parquet files -> Convert to RDD -> Define Transformations
    rdd_1 = ss.read.parquet("hdfs://hdfs-namenode:9000/user/magdalinipavlou/data/parquet/" + file_name_1).rdd \
        .map(lambda x: (x["Zip Code"], x["Average Household Size"]))
    rdd_2 = ss.read.parquet("hdfs://hdfs-namenode:9000/user/magdalinipavlou/data/parquet/" + file_name_2).rdd \
        .map(lambda x: (x["Zip Code"], extract_number(x["Estimated Median Income"])))
    result = rdd_2.join(rdd_1) \
        .map(lambda x: (x[0], float(x[1][0])/float(x[1][1]))) \
        .sortBy(lambda x: x[0])
    return result



if __name__ == '__main__':
    # Excecute Transformations
    file_name_1 = "part-00000-19793775-2445-425c-8c28-1e03c674d6be-c000.snappy.parquet"
    file_name_2 = "part-00000-ccff6091-7fb3-44da-890c-97c8e350492d-c000.snappy.parquet"
    find_income = person_income_per_year(file_name_1, file_name_2)
    # Apply Transformations
    my_rdd_list = find_income.take(20)
    for i in my_rdd_list:
        print(i)