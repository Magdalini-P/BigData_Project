from pyspark import SparkConf
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, count, udf, row_number
from pyspark.sql.types import StringType
from pyspark.sql.window import Window
import time

# UDF for extraction of the year of column "Date Rptd"
def extract_year(s):
    return s.split("/")[2].split(" ")[0]

# Main function
def query_2_df(file_name_1_0, file_name_1_1, file_name_1_2, file_name_1_3, file_name_1_4, file_name_2_0, file_name_2_1, file_name_2_2, file_name_2_3, file_name_2_4):
    conf = SparkConf().setAppName("FindPrecinctsDF") \
        .set("spark.executor.memory", "2g") \
        .set("spark.driver.memory", "2g")
    ss = SparkSession.builder.config(conf=conf).getOrCreate()

    # Register UDF
    extract_year_udf = udf(extract_year, StringType())

    # Load data
    df1 = ss.read.parquet("hdfs://hdfs-namenode:9000/user/katerinagratsia/data/parquet/" + file_name_1_0,
                          "hdfs://hdfs-namenode:9000/user/katerinagratsia/data/parquet/" + file_name_1_1,
                          "hdfs://hdfs-namenode:9000/user/katerinagratsia/data/parquet/" + file_name_1_2,
                          "hdfs://hdfs-namenode:9000/user/katerinagratsia/data/parquet/" + file_name_1_3,
                          "hdfs://hdfs-namenode:9000/user/katerinagratsia/data/parquet/" + file_name_1_4)
    df2 = ss.read.parquet("hdfs://hdfs-namenode:9000/user/katerinagratsia/data/parquet/" + file_name_2_0,
                          "hdfs://hdfs-namenode:9000/user/katerinagratsia/data/parquet/" + file_name_2_1,
                          "hdfs://hdfs-namenode:9000/user/katerinagratsia/data/parquet/" + file_name_2_2,
                          "hdfs://hdfs-namenode:9000/user/katerinagratsia/data/parquet/" + file_name_2_3,
                          "hdfs://hdfs-namenode:9000/user/katerinagratsia/data/parquet/" + file_name_2_4)
    df = df1.union(df2)

    # Add year column
    df = df.withColumn("year", extract_year_udf(col("Date Rptd")))

    # Calculate total cases for each year and precinct
    total_cases = df.groupBy("year", "AREA NAME") \
        .agg(count("AREA NAME").alias("case_count"))

    # Calculate closed cases
    closed_cases = df.filter(~col("Status Desc").isin("UNK", "Invest Cont")) \
        .groupBy("year", "AREA NAME") \
        .agg(count("AREA NAME").alias("closed_case_count"))

    # Join of two tables for calculation of the percentage of closed cases
    joined = closed_cases.join(total_cases,
                               on=["year", "AREA NAME"]) \
        .withColumn("closed_case_rate",
                    (col("closed_case_count") * 100.0) / col("case_count"))

    # Top 3 each year
    window_spec = Window.partitionBy("year").orderBy(col("closed_case_rate").desc())

    # Add row number
    result = joined.withColumn("row_number", row_number().over(window_spec)) \
                   .filter(col("row_number") <= 3)

    return result

# Execution
if __name__ == "__main__":

    start = time.time()

    file_name_1_0 = "part-00000-ecba35dd-23ff-4ca6-9ceb-7577b8b15406-c000.snappy.parquet"
    file_name_1_1 = "part-00001-ecba35dd-23ff-4ca6-9ceb-7577b8b15406-c000.snappy.parquet"
    file_name_1_2 = "part-00002-ecba35dd-23ff-4ca6-9ceb-7577b8b15406-c000.snappy.parquet"
    file_name_1_3 = "part-00003-ecba35dd-23ff-4ca6-9ceb-7577b8b15406-c000.snappy.parquet"
    file_name_1_4 = "part-00004-ecba35dd-23ff-4ca6-9ceb-7577b8b15406-c000.snappy.parquet"
    file_name_2_0 = "part-00000-5a77e7c2-056a-4f7f-9169-ae4c48ff27bf-c000.snappy.parquet"
    file_name_2_1 = "part-00001-5a77e7c2-056a-4f7f-9169-ae4c48ff27bf-c000.snappy.parquet"
    file_name_2_2 = "part-00002-5a77e7c2-056a-4f7f-9169-ae4c48ff27bf-c000.snappy.parquet"
    file_name_2_3 = "part-00003-5a77e7c2-056a-4f7f-9169-ae4c48ff27bf-c000.snappy.parquet"
    file_name_2_4 = "part-00004-5a77e7c2-056a-4f7f-9169-ae4c48ff27bf-c000.snappy.parquet"
    result = query_2_df(file_name_1_0, file_name_1_1, file_name_1_2, file_name_1_3, file_name_1_4, file_name_2_0, file_name_2_1, file_name_2_2, file_name_2_3, file_name_2_4)
    result.show()

    end = time.time()

    print(f"Execution time: {(end - start):.2f} seconds")