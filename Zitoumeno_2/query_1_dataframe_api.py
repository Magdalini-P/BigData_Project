from pyspark import SparkConf
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, when, lower
from pyspark.sql.types import IntegerType
import time

def group_by_age_df(file_name_1_0, file_name_1_1, file_name_1_2, file_name_1_3, file_name_1_4, file_name_2_0, file_name_2_1, file_name_2_2, file_name_2_3, file_name_2_4):
    '''Group Data By Age-Groups using DataFrame API'''
    # Define Spark session
    conf = SparkConf().setAppName("GroupVictsByAge") \
        .set("spark.executor.memory", "2g") \
        .set("spark.driver.memory", "2g")
    ss = SparkSession.builder.config(conf=conf).getOrCreate()

    # Load parquet files
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

    # Union datasets
    combined_df = df1.union(df2)

    # Filter rows for "AGGRAVATED ASSAULT"
    filtered_df = combined_df.filter(lower(col("Crm Cd Desc")).contains("aggravated assault"))

    # Ensure Vict Age is integer
    filtered_df = filtered_df.withColumn("Vict Age", col("Vict Age").cast(IntegerType()))

    # Create age groups using when-otherwise logic
    grouped_df = filtered_df.withColumn(
        "Age Group",
        when(col("Vict Age") < 18, "Kids")
        .when((col("Vict Age") >= 18) & (col("Vict Age") <= 24), "Young Adults")
        .when((col("Vict Age") >= 25) & (col("Vict Age") <= 64), "Adults")
        .otherwise("Elderly")
    )

    # Group by Age Group and count occurrences
    result_df = grouped_df.groupBy("Age Group").count().orderBy(col("count").desc())

    return result_df

if __name__ == '__main__':

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
    result_df = group_by_age_df(file_name_1_0, file_name_1_1, file_name_1_2, file_name_1_3, file_name_1_4, file_name_2_0, file_name_2_1, file_name_2_2, file_name_2_3, file_name_2_4)
    result_df.show(truncate=False)

    end = time.time()

    print(f"Execution time: {(end - start):.2f} seconds")