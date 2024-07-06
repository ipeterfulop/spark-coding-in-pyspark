"""
Given the backups below:
+-----------+-----------+-----------+-----------+
|backup_date|device_name|backup_type|backup_size|
+-----------+-----------+-----------+-----------+
|2024-01-01 |device #1  |Full       |1000       |
|2024-01-02 |device #1  |Incremental|100        |
|2024-01-03 |device #1  |Full       |1100       |
|2024-01-04 |device #1  |Incremental|100        |
|2024-01-01 |device #2  |Full       |2000       |
|2024-01-02 |device #2  |Incremental|100        |
|2024-01-03 |device #2  |Full       |2100       |
|2024-01-04 |device #2  |Incremental|100        |
|2024-01-05 |device #2  |Full       |2200       |
|2024-01-06 |device #2  |Incremental|100        |
+-----------+-----------+-----------+-----------+
Find average size of last full backups.

**Expected output**:

+-----------------------------+
|avg_size_of_last_full_backups|
+-----------------------------+
|1650                         |
+-----------------------------+
"""
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, DateType, LongType
from pyspark.sql.window import Window
from pyspark.sql.functions import col, rank, avg, round
import pyspark.sql.functions as F

# Initialize Spark session
spark = SparkSession.builder.appName("Backups").getOrCreate()

# Define the schema
schema = StructType([
    StructField("str_backup_date", StringType(), nullable=False),
    StructField("device_name", StringType(), nullable=False),
    StructField("backup_type", StringType(), nullable=False),
    StructField("backup_size", LongType(), nullable=False)
])

# Create a list of tuples containing the data
data = [
    ('2024-01-01', 'device #1', 'Full', 1000),
    ('2024-01-02', 'device #1', 'Incremental', 100),
    ('2024-01-03', 'device #1', 'Full', 1100),
    ('2024-01-04', 'device #1', 'Incremental', 100),
    ('2024-01-01', 'device #2', 'Full', 2000),
    ('2024-01-02', 'device #2', 'Incremental', 100),
    ('2024-01-03', 'device #2', 'Full', 2100),
    ('2024-01-04', 'device #2', 'Incremental', 100),
    ('2024-01-05', 'device #2', 'Full', 2200),
    ('2024-01-06', 'device #2', 'Incremental', 100)
]

# Create DataFrame
df_backups = spark.createDataFrame(data, schema)
df_backups = df_backups.withColumn("backup_date", F.to_date(F.col("str_backup_date"), "yyyy-MM-dd"))
df_backups.drop("str_backup_date")

# Show the DataFrame
df_backups.show()

# Define the window specification
windowSpec = Window.partitionBy("device_name").orderBy(col("backup_date").desc())

# Add a rank column to identify the most recent full backups
df_full_backups_with_rank = df_backups \
    .filter(col("backup_type") == "Full") \
    .withColumn("brank", rank().over(windowSpec))

# Filter to get only the most recent full backups
df_recent_full_backups = df_full_backups_with_rank.filter(col("brank") == 1)

# Calculate the average size of the last full backups
df_avg_size_of_last_full_backups = df_recent_full_backups \
    .agg(round(avg(col("backup_size")), 3).alias("avg_size_of_last_full_backups"))

# Show the result
df_avg_size_of_last_full_backups.show()
