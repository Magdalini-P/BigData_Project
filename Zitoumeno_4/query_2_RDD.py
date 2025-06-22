# Query 2 - Solution (RDD)

# Imports
from pyspark import SparkConf
from pyspark.sql import SparkSession


def combine_parquets(ss, file1, file2, file3, file4, file5):
    '''Return Union of RDDs'''
    rdd = ss.read.parquet("hdfs://hdfs-namenode:9000/user/magdalinipavlou/data/parquet/" + file1).rdd \
        .union(ss.read.parquet("hdfs://hdfs-namenode:9000/user/magdalinipavlou/data/parquet/" + file2).rdd) \
        .union(ss.read.parquet("hdfs://hdfs-namenode:9000/user/magdalinipavlou/data/parquet/" + file3).rdd) \
        .union(ss.read.parquet("hdfs://hdfs-namenode:9000/user/magdalinipavlou/data/parquet/" + file4).rdd) \
        .union(ss.read.parquet("hdfs://hdfs-namenode:9000/user/magdalinipavlou/data/parquet/" + file5).rdd)
    return rdd

def extract_year(init_str):
    d = init_str.split(" ")[0]
    return int(d.split("/")[2])

def sort_values(li):
    '''Sort List of Tuples by the element in the 1st position'''
    res = sorted(li, key=lambda x: x[1], reverse=True)
    return res


def find_prec_per_year(file10, file11, file12, file13, file14, file20, file21, file22, file23, file24):
    '''Find 3 Top Precincts per Year'''
    conf = SparkConf().setAppName("FindPrecincts") \
        .set("spark.executor.memory", "2g") \
        .set("spark.driver.memory", "2g")
    ss = SparkSession.builder.config(conf=conf).getOrCreate()
    # Read parquet files -> Convert to RDD -> Define Transformations
    rdd_1 = combine_parquets(ss, file10, file11, file12, file13, file14)
    rdd_2 = combine_parquets(ss, file20, file21, file22, file23, file24)
    process_1 = rdd_1.union(rdd_2) \
        .map(lambda x: (extract_year(x["Date Rptd"]), x["AREA NAME"], x["Status Desc"])) \
        .filter(lambda x: ("UNK" not in x[2]) and ("Invest Cont" not in x[2])) \
        .map(lambda x: ((x[0], x[1]),1)) \
        .reduceByKey(lambda x,y: x+y)
    process_2 = rdd_1.union(rdd_2) \
        .map(lambda x: (extract_year(x["Date Rptd"]), x["AREA NAME"], x["Status Desc"])) \
        .map(lambda x: ((x[0], x[1]),1)) \
        .reduceByKey(lambda x,y: x+y)
    final_process = process_1.join(process_2) \
        .map(lambda x: (x[0][0], (x[0][1], x[1][0]*100/x[1][1]))) \
        .groupByKey() \
        .map(lambda x: (x[0], list(x[1]))) \
        .map(lambda x: (x[0], sort_values(x[1]))) \
        .flatMap(lambda x: [(x[0], x[1][0], 1), (x[0], x[1][1], 2), (x[0], x[1][2], 3)]) \
        .sortBy(lambda x: (x[0], x[2]))
    return final_process



if __name__ == "__main__":
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
    find_precs = find_prec_per_year(file_name_10, file_name_11, file_name_12, file_name_13, file_name_14, file_name_20, file_name_21, file_name_22, file_name_23, file_name_24)
    # Apply Transformations
    my_rdd_list = find_precs.collect()
    for i in my_rdd_list:
        print(i)