from pyspark.sql import SparkSession
from pyspark.sql.functions import col, lower, broadcast, row_number, avg, count, sqrt, pow
from pyspark.sql.window import Window
import time

# Count time from here
start_time = time.time()

# Create Spark session
spark = SparkSession.builder \
    .appName("WeaponCrimeDistanceByStation") \
    .config("spark.executor.memory", "2g") \
    .config("spark.driver.memory", "2g") \
    .getOrCreate()

# Read parquet files
crime_df1 = spark.read.parquet("hdfs://hdfs-namenode:9000/user/katerinagratsia/data/parquet/part-00000-5a77e7c2-056a-4f7f-9169-ae4c48ff27bf-c000.snappy.parquet",
                               "hdfs://hdfs-namenode:9000/user/katerinagratsia/data/parquet/part-00001-5a77e7c2-056a-4f7f-9169-ae4c48ff27bf-c000.snappy.parquet",
                               "hdfs://hdfs-namenode:9000/user/katerinagratsia/data/parquet/part-00002-5a77e7c2-056a-4f7f-9169-ae4c48ff27bf-c000.snappy.parquet",
                               "hdfs://hdfs-namenode:9000/user/katerinagratsia/data/parquet/part-00003-5a77e7c2-056a-4f7f-9169-ae4c48ff27bf-c000.snappy.parquet",
                               "hdfs://hdfs-namenode:9000/user/katerinagratsia/data/parquet/part-00004-5a77e7c2-056a-4f7f-9169-ae4c48ff27bf-c000.snappy.parquet")

crime_df2 = spark.read.parquet("hdfs://hdfs-namenode:9000/user/katerinagratsia/data/parquet/part-00000-ecba35dd-23ff-4ca6-9ceb-7577b8b15406-c000.snappy.parquet",
                               "hdfs://hdfs-namenode:9000/user/katerinagratsia/data/parquet/part-00001-ecba35dd-23ff-4ca6-9ceb-7577b8b15406-c000.snappy.parquet",
                               "hdfs://hdfs-namenode:9000/user/katerinagratsia/data/parquet/part-00002-ecba35dd-23ff-4ca6-9ceb-7577b8b15406-c000.snappy.parquet",
                               "hdfs://hdfs-namenode:9000/user/katerinagratsia/data/parquet/part-00003-ecba35dd-23ff-4ca6-9ceb-7577b8b15406-c000.snappy.parquet",
                               "hdfs://hdfs-namenode:9000/user/katerinagratsia/data/parquet/part-00004-ecba35dd-23ff-4ca6-9ceb-7577b8b15406-c000.snappy.parquet")

weapon_codes_df = spark.read.parquet("hdfs://hdfs-namenode:9000/user/katerinagratsia/data/parquet/part-00000-346ae1d2-4150-4b4e-99ce-203228b3e0d5-c000.snappy.parquet")

stations_df = spark.read.parquet("hdfs://hdfs-namenode:9000/user/katerinagratsia/data/parquet/part-00000-4c396d41-e83c-4b53-a68e-bad7f6e76d43-c000.snappy.parquet")

# Filter weapon codes
weapon_codes_filtered = weapon_codes_df.filter(
    lower(col("description")).contains("gun") | lower(col("description")).contains("weapon")
).select(col("code").alias("WeaponCode"))

# Union of all cases
crime_df = crime_df1.union(crime_df2)

# Filter crimes
filtered_crimes = crime_df.join(
    weapon_codes_filtered,
    crime_df["Mocodes"] == weapon_codes_filtered["WeaponCode"],
    how="inner"
).filter(
    (lower(col("Crm Cd Desc")).contains("aggravated assault")) &
    ((lower(col("Status Desc")) == "unk") | (lower(col("Status Desc")) == "invest cont")) &
    (col("LAT") != 0) & (col("LON") != 0)
)

# Cross join with all stations for distance
stations = stations_df.select(
    col("DIVISION").alias("station_name"),
    col("Y").alias("station_lat"),
    col("X").alias("station_lon")
)

joined = filtered_crimes.crossJoin(stations)

# Calculate euclidean distance
joined = joined.withColumn(
    "distance_km",
    sqrt(
        pow((col("LAT") - col("station_lat")) * 111, 2) +
        pow((col("LON") - col("station_lon")) * 85, 2)
    )
)

# Keep the closest station in each case
window_spec = Window.partitionBy("DR_NO").orderBy("distance_km")
closest = joined.withColumn("rank", row_number().over(window_spec)).filter(col("rank") == 1)

# Group by station
result = closest.groupBy("station_name").agg(
    count("*").alias("num_crimes"),
    avg("distance_km").alias("avg_distance_km")
).orderBy(col("num_crimes").desc())

# show results
result.show(20, truncate=False)

# Print Physical Plan
result.explain()

# End time and print
end_time = time.time()
print(f"\nTotal execution time: {end_time - start_time:.2f} seconds")
