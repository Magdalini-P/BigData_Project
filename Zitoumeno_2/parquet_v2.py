from pyspark.sql import SparkSession
from pyspark.sql.functions import split, col
import os

# Δημιουργία SparkSession
spark = SparkSession.builder \
    .appName("CSVΤoParquet") \
    .getOrCreate()

# Διαδρομή datasets στο HDFS
dataset_LA_Crime_Data_10_19 = "hdfs://hdfs-namenode:9000/user/root/data/LA_Crime_Data_2010_2019.csv"
dataset_LA_Crime_Data_20 = "hdfs://hdfs-namenode:9000/user/root/data/LA_Crime_Data_2020_2025.csv"
dataset_LA_Police_Stations = "hdfs://hdfs-namenode:9000/user/root/data/LA_Police_Stations.csv"
dataset_Median_Income = "hdfs://hdfs-namenode:9000/user/root/data/LA_income_2015.csv"
dataset_2010_Census_Populations = "hdfs://hdfs-namenode:9000/user/root/data/2010_Census_Populations_by_Zip_Code.csv"
dataset_MO_Codes = "hdfs://hdfs-namenode:9000/user/root/data/MO_codes.txt"

# Ανάγνωση αρχείου CSV απο ΗDFS
df_LA_Crime_Data_10_19 = spark.read.csv(dataset_LA_Crime_Data_10_19, header = True, inferSchema = True)
df_LA_Crime_Data_20 = spark.read.csv(dataset_LA_Crime_Data_20, header = True, inferSchema = True)
df_LA_Police_Stations = spark.read.csv(dataset_LA_Police_Stations, header = True, inferSchema = True)
df_Median_Income = spark.read.csv(dataset_Median_Income, header = True, inferSchema = True)
df_2010_Census_Populations = spark.read.csv(dataset_2010_Census_Populations, header = True, inferSchema = True)

# Διαβάζουμε το .txt αρχείο ως dataframe με μία στήλη - value
df_MO_Codes = spark.read.text(dataset_MO_Codes)

# Διαχωρισμός της γραμμής σε "code" και "description" στο πρωτο κενό
df_MO_Codes_split = df_MO_Codes.withColumn("code", split(col("value"), " ", 2)[0]) \
                 .withColumn("description", split(col("value"), " ", 2)[1]) \
                 .drop("value")

output_path = "hdfs://hdfs-namenode:9000/user/magdalinipavlou/data/parquet"
# Ensure that the directory exists
os.makedirs(os.path.dirname(output_path), exist_ok=True)

# Αποθήκευση σε μορφή Parquet
df_LA_Crime_Data_10_19.write.mode("append").parquet(output_path)
df_LA_Crime_Data_20.write.mode("append").parquet(output_path)
df_LA_Police_Stations.write.mode("append").parquet(output_path)
df_Median_Income.write.mode("append").parquet(output_path)
df_2010_Census_Populations.write.mode("append").parquet(output_path)
df_MO_Codes_split.write.mode("append").parquet(output_path)