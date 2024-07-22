"""
In the table below is given the seat reservation of a cinema for a given movie session.
Find the longest consecutive sequence of free seats. (take seat_id as seat number)
Consecutive seats are the seats that are next to each in the order of seat_ids and are at least 2 seats next to each other.
Input:
+-------+----+
|seat_id|free|
+-------+----+
|      1|   1|
|      2|   0|
|      3|   1|
|      4|   1|
|      5|   1|
|      6|   0|
|      7|   1|
|      8|   1|
|      9|   0|
|     10|   1|
|     11|   0|
|     12|   1|
|     13|   0|
|     14|   1|
|     15|   1|
|     16|   0|
|     17|   1|
|     18|   1|
|     19|   0|
|     20|   1|
+-------+----+

Expected output:
+-------+
|seat_id|
+-------+
|      3|
|      4|
|      5|
|      7|
|      8|
|     14|
|     15|
|     17|
|     18|
+-------+


"""

from pyspark.sql import SparkSession, Window
from pyspark.sql.types import IntegerType, StructType, StructField
import pyspark.sql.functions as f

if __name__ == "__main__":
    # Initialize Spark session
    spark = SparkSession.builder.appName("CinemaSeats").getOrCreate()

    # Define the schema
    schema = StructType([
        StructField("seat_id", IntegerType(), nullable=False),
        StructField("is_free", IntegerType(), nullable=False)
    ])

    # Create a list of tuples containing the data
    data = [
        (1, 1), (2, 0), (3, 1), (4, 1), (5, 1),
        (6, 0), (7, 1), (8, 1), (9, 0), (10, 1),
        (11, 0), (12, 1), (13, 0), (14, 1), (15, 1),
        (16, 0), (17, 1), (18, 1), (19, 0), (20, 1)
    ]

    cinema_df = spark.createDataFrame(data, schema)

    # Filter free seats
    free_seats_df = cinema_df.filter(f.col("is_free") == 1)

    # Define the window specification
    windowSpec = Window.orderBy("seat_id")

    # Add lag and lead columns
    free_seat_stat_df = free_seats_df.withColumn("prev_free", f.lag("seat_id", 1, -1).over(windowSpec)) \
        .withColumn("next_free", f.lead("seat_id", 1, -1).over(windowSpec))

    # Filter the result
    result_df = free_seat_stat_df.filter(
        (f.col("seat_id") == f.col("prev_free") + 1) | (f.col("seat_id") == f.col("next_free") - 1))

    # Show the result
    result_df.select("seat_id").show()