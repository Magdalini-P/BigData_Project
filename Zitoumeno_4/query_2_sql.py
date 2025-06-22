# Query 2 - Solution (SQL)

# Imports
from pyspark import SparkConf
from pyspark.sql import SparkSession
from pyspark.sql.functions import udf
from pyspark.sql.types import StringType


def combine_parquets(ss, file1, file2, file3, file4, file5):
    '''Return Union of DFs'''
    df = ss.read.parquet("hdfs://hdfs-namenode:9000/user/magdalinipavlou/data/parquet/" + file1) \
        .union(ss.read.parquet("hdfs://hdfs-namenode:9000/user/magdalinipavlou/data/parquet/" + file2)) \
        .union(ss.read.parquet("hdfs://hdfs-namenode:9000/user/magdalinipavlou/data/parquet/" + file3)) \
        .union(ss.read.parquet("hdfs://hdfs-namenode:9000/user/magdalinipavlou/data/parquet/" + file4)) \
        .union(ss.read.parquet("hdfs://hdfs-namenode:9000/user/magdalinipavlou/data/parquet/" + file5))
    return df


def extract_year(s):
    '''Extract year from Date Rptd'''
    s = s.split("/")[2]
    s = s.split(" ")[0]
    return s


def query_2_sql(file10, file11, file12, file13, file14, file20, file21, file22, file23, file24, query):
    '''Find 3 Top Precincts per Year'''
    conf = SparkConf().setAppName("FindPrecinctsSQL") \
        .set("spark.executor.memory", "2g") \
        .set("spark.driver.memory", "2g")
    ss = SparkSession.builder.config(conf=conf).getOrCreate()
    ss.udf.register("extract_year", extract_year, StringType())
    # Read parquet files -> Convert to RDD -> Define Transformations
    df_1 = combine_parquets(ss, file10, file11, file12, file13, file14)
    df_2 = combine_parquets(ss, file20, file21, file22, file23, file24)
    df = df_1.union(df_2)
    # Use SQL
    df.createOrReplaceTempView("Crimes")
    result = ss.sql(query)
    return result

# Query
query = """
    WITH table_2 AS (
    SELECT extract_year(`Date Rptd`) AS year, `AREA NAME` AS precinct, COUNT(`AREA NAME`) AS case_count
    FROM Crimes
    GROUP BY year, precinct),
    final_table AS (
    SELECT *, ROW_NUMBER() OVER (PARTITION BY year ORDER BY closed_case_rate DESC) AS row_number
    FROM (
    SELECT table_1.year, table_1.precinct, closed_case_count*100/case_count AS closed_case_rate
    FROM (
    SELECT extract_year(`Date Rptd`) AS year, `AREA NAME` AS precinct, COUNT(`AREA NAME`) AS closed_case_count
    FROM Crimes
    WHERE `Status Desc` NOT IN ("UNK", "Invest Cont")
    GROUP BY year, precinct
    ORDER BY year, closed_case_count DESC) table_1 join table_2 ON table_1.year = table_2.year AND table_1.precinct = table_2.precinct))
    
    SELECT *
    FROM final_table
    WHERE row_number <= 3
"""

if __name__ == "__main__":
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
    result = query_2_sql(file_name_10, file_name_11, file_name_12, file_name_13, file_name_14, file_name_20, file_name_21, file_name_22, file_name_23, file_name_24, query)
    result.show()
