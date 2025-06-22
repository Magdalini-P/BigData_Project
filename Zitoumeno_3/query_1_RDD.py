# Query 1 - Solution (RDD)

# Imports
from pyspark import SparkConf
from pyspark.sql import SparkSession

def get_age_groups(age):
    '''Fuction to map age -> age-group.'''
    age = int(age)
    if age < 18:
        return "Kids"
    elif age >= 18 and age <= 24:
        return "Young Adults"
    elif age >= 25 and age <= 64:
        return "Adults"
    else:
        return "Elderly"

def combine_parquets(ss, file1, file2, file3, file4, file5):
    '''Return Union of RDDs'''
    rdd = ss.read.parquet("hdfs://hdfs-namenode:9000/user/magdalinipavlou/data/parquet/" + file1).rdd \
        .union(ss.read.parquet("hdfs://hdfs-namenode:9000/user/magdalinipavlou/data/parquet/" + file2).rdd) \
        .union(ss.read.parquet("hdfs://hdfs-namenode:9000/user/magdalinipavlou/data/parquet/" + file3).rdd) \
        .union(ss.read.parquet("hdfs://hdfs-namenode:9000/user/magdalinipavlou/data/parquet/" + file4).rdd) \
        .union(ss.read.parquet("hdfs://hdfs-namenode:9000/user/magdalinipavlou/data/parquet/" + file5).rdd)
    return rdd

def group_by_age_rdd(file10, file11, file12, file13, file14, file20, file21, file22, file23, file24):
    '''Group Data By Age-Groups'''
    # Define Transformations
    conf = SparkConf().setAppName("GroupVictsByAge") \
        .set("spark.executor.memory", "2g") \
        .set("spark.driver.memory", "2g")
    ss = SparkSession.builder.config(conf=conf).getOrCreate()
    # Read parquet files -> Convert to RDD -> Define Transformations
    rdd_1 = combine_parquets(ss, file10, file11, file12, file13, file14)
    rdd_2 = combine_parquets(ss, file20, file21, file22, file23, file24)
    group_victs = rdd_1.union(rdd_2) \
        .filter(lambda x: "AGGRAVATED ASSAULT" in x["Crm Cd Desc"]) \
        .map(lambda x: x["Vict Age"]) \
        .map(lambda x: get_age_groups(x)) \
        .map(lambda x: (x, 1)) \
        .reduceByKey(lambda x,y : x+y) \
        .sortBy(lambda x: x[1], ascending=False)
    return group_victs


if __name__ == '__main__':
    # Excecute Transformations
    # Part1
    file_name_10 = "part-00000-f2c0a77c-7c54-4dbe-9261-005a7c16cd57-c000.snappy.parquet"
    file_name_11 = "part-00001-f2c0a77c-7c54-4dbe-9261-005a7c16cd57-c000.snappy.parquet"
    file_name_12 = "part-00002-f2c0a77c-7c54-4dbe-9261-005a7c16cd57-c000.snappy.parquet"
    file_name_13 = "part-00003-f2c0a77c-7c54-4dbe-9261-005a7c16cd57-c000.snappy.parquet"
    file_name_14 = "part-00004-f2c0a77c-7c54-4dbe-9261-005a7c16cd57-c000.snappy.parquet"
    # Part2
    file_name_20 = "part-00000-b387cd04-f8a5-412b-b6be-7ab6f8d23d98-c000.snappy.parquet"
    file_name_21 = "part-00001-b387cd04-f8a5-412b-b6be-7ab6f8d23d98-c000.snappy.parquet"
    file_name_22 = "part-00002-b387cd04-f8a5-412b-b6be-7ab6f8d23d98-c000.snappy.parquet"
    file_name_23 = "part-00003-b387cd04-f8a5-412b-b6be-7ab6f8d23d98-c000.snappy.parquet"
    file_name_24 = "part-00004-b387cd04-f8a5-412b-b6be-7ab6f8d23d98-c000.snappy.parquet"
    group_victs = group_by_age_rdd(file_name_10, file_name_11, file_name_12, file_name_13, file_name_14, file_name_20, file_name_21, file_name_22, file_name_23, file_name_24)
    li = group_victs.collect()
    for i in li:
        print(i)
