from pyspark import SparkConf
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, lower, udf
from pyspark.sql.types import IntegerType, StringType
import time

# UDF for matching age with the age group
def get_age_group(age):

    if age is None:
        return "Unknown"
    if age < 18:
        return "Kids"
    elif 18 <= age <= 24:
        return "Young Adults"
    elif 25 <= age <= 64:
        return "Adults"
    else:
        return "Elderly"

# Create UDF
age_group_udf = udf(get_age_group, StringType())

def group_by_age_df(file_name_1_0, file_name_1_1, file_name_1_2, file_name_1_3, file_name_1_4, file_name_2_0, file_name_2_1, file_name_2_2, file_name_2_3, file_name_2_4):
    '''Group Data By Age-Groups using DataFrame API with UDF'''
    conf = SparkConf().setAppName("GroupVictsByAgeUDF") \
        .set("spark.executor.memory", "2g") \
        .set("spark.driver.memory", "2g")
    ss = SparkSession.builder.config(conf=conf).getOrCreate()

    # Read parquet files
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

    # Combine DataFrames
    combined_df = df1.union(df2)

    # Filter for "AGGRAVATED ASSAULT"
    filtered_df = combined_df.filter(lower(col("Crm Cd Desc")).contains("aggravated assault"))

    # Transform age in integer
    filtered_df = filtered_df.withColumn("Vict Age", col("Vict Age").cast(IntegerType()))

    # Add column with age group
    grouped_df = filtered_df.withColumn("Age Group", age_group_udf(col("Vict Age")))

    # Grouping and counting
    result_df = grouped_df.groupBy("Age Group").count().orderBy(col("count").desc())

    return result_df

# Execution
#if __name__ == '__main__':

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
result_df.show()

end = time.time()

print(f"Execution time: {(end - start):.2f} seconds")
