from pyspark import SparkConf
from pyspark.sql import SparkSession
from pyspark.sql.functions import udf, col
from pyspark.sql.types import FloatType, StringType
import time

# Extract income
def extract_number(s):
    parts = s.split(",")
    return parts[0][1:] + parts[1]

def person_income_per_year_df(file_name_1, file_name_2):
    conf = SparkConf().setAppName("FindIncomePerYearCSV") \
        .set("spark.executor.memory", "2g") \
        .set("spark.driver.memory", "2g")
    ss = SparkSession.builder.config(conf=conf).getOrCreate()

    # UDF for income
    extract_income_udf = udf(extract_number, StringType())

    # Load CSV files with heades and inferred schema
    df1 = ss.read.option("header", "true").option("inferSchema", "true") \
        .csv("hdfs://hdfs-namenode:9000/user/katerinagratsia/" + file_name_1)

    df2 = ss.read.option("header", "true").option("inferSchema", "true") \
        .csv("hdfs://hdfs-namenode:9000/user/katerinagratsia/" + file_name_2)

    # Calculate income and convert to float
    df2 = df2.withColumn("clean_income",
                         extract_income_udf(col("Estimated Median Income")).cast(FloatType()))

    # Join based on Zip Code
    joined = df1.join(df2, on="Zip Code", how="inner")

    # Calculate personal income
    result = joined.withColumn("person_income",
                               col("clean_income") / col("Average Household Size"))

    # Select output
    result = result.select("Zip Code", "person_income")

    return result

# Execution
if __name__ == '__main__':

    start = time.time()

    file_name_1 = "part-00000-e639ed5b-ca3f-4420-90b0-c7c0c4d44233-c000.csv"
    file_name_2 = "part-00000-b7a87fec-0554-4aea-ba67-184f8975739e-c000.csv"
    result_df = person_income_per_year_df(file_name_1, file_name_2)
    result_df.show(20)

    end = time.time()

    print(f"Execution time: {(end - start):.2f} seconds")

