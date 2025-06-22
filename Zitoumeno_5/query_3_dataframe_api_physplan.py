from pyspark import SparkConf
from pyspark.sql import SparkSession
from pyspark.sql.functions import udf, col
from pyspark.sql.types import FloatType, StringType
import time

# In order to extract the number of the column income which is string type
def extract_number(s):
    parts = s.split(",")
    return parts[0][1:] + parts[1]

# Main function
def person_income_per_year_df(file_name_1, file_name_2):
    conf = SparkConf().setAppName("FindIncomePerYearDF") \
        .set("spark.executor.memory", "2g") \
        .set("spark.driver.memory", "2g")
    ss = SparkSession.builder.config(conf=conf).getOrCreate()

    # UDF for income extraction
    extract_income_udf = udf(extract_number, StringType())

    # Read files
    df1 = ss.read.parquet("hdfs://hdfs-namenode:9000/user/katerinagratsia/data/parquet/" + file_name_1)
    df2 = ss.read.parquet("hdfs://hdfs-namenode:9000/user/katerinagratsia/data/parquet/" + file_name_2)

    # Calculate income and convert to float
    df2 = df2.withColumn("clean_income",
                         extract_income_udf(col("Estimated Median Income")).cast(FloatType()))

    # Join based on Zip Code
    joined = df1.join(df2, on="Zip Code", how="inner")

    # Calculate personal income
    result = joined.withColumn("person_income",
                               col("clean_income") / col("Average Household Size"))

    # Select results
    result = result.select("Zip Code", "person_income")

    return result

# Execution
if __name__ == '__main__':

    file_name_1 = "part-00000-e639ed5b-ca3f-4420-90b0-c7c0c4d44233-c000.snappy.parquet"
    file_name_2 = "part-00000-b7a87fec-0554-4aea-ba67-184f8975739e-c000.snappy.parquet"
    result_df = person_income_per_year_df(file_name_1, file_name_2)
    start = time.time()
    result_df.show(20)
    end = time.time()
    result_df.explain()     # Print Physical Plan
    print(f"Execution time: {(end - start):.2f} seconds")
