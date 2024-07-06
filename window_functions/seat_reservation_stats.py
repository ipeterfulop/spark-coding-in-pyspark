from pyspark.sql import SparkSession, Window
from pyspark.sql.functions import col, lag, lead, sum, count, when, coalesce
from pyspark.sql.types import IntegerType, StructType, StructField

# Initialize Spark session
spark = SparkSession.builder.appName("SeatReservationStats").getOrCreate()

# Define the schema
schema = StructType([
    StructField("seat_id", IntegerType(), nullable=False),
    StructField("is_free", IntegerType(), nullable=False)
])

# Create a list of tuples containing the data
seat_reservation_data = [
    (1, 1), (2, 0), (3, 1), (4, 1), (5, 1),
    (6, 0), (7, 1), (8, 1), (9, 0), (10, 1),
    (11, 0), (12, 1), (13, 0), (14, 1), (15, 1),
    (16, 1), (17, 1), (18, 1), (19, 0), (20, 1), (21, 1)
]

df_seat_reservation = spark.createDataFrame(seat_reservation_data, schema)

# Define the window specification for lag and lead functions
windowSpec = Window.orderBy("seat_id")

# Step 1: Create free_seat_stat DataFrame
# that for each free seat contains the previous and next free seat number(seat_id)
free_seat_stat = df_seat_reservation \
    .filter(col("is_free") == 1) \
    .withColumn("prev_free", lag("seat_id", 1).over(windowSpec)) \
    .withColumn("next_free", lead("seat_id", 1).over(windowSpec))

# Step 2: Create consecutive_seats DataFrame
# a "gap" column is added to prepare the creation of a group number
consecutive_seats = free_seat_stat \
    .withColumn("gap", col("seat_id") - coalesce(col("prev_free"), col("seat_id")) - 1) \
    .filter((col("seat_id") == col("prev_free") + 1) | (col("seat_id") == col("next_free") - 1))
consecutive_seats.show()

# Step 3: Create consecutive_seat_group_numbers DataFrame: The window for the SUM function expands as we move from one
# row to the next. Each row's group_number is computed based on all rows from the start up to and including the
# current row, ordered by seat_id. This cumulative sum helps to determine the group number for each row based on the
# presence of gaps. By using `SUM with OVER (ORDER BY seat_id)`, we ensure that each row's calculation considers all
# previous rows in the order specified, thus creating a cumulative running total up to the current row.
windowSpecGroup = Window.orderBy("seat_id")
consecutive_seat_group_numbers = consecutive_seats \
    .withColumn("group_number", sum(when(col("gap") > 0, 1).otherwise(0)).over(windowSpecGroup))

# Step 4: Create group_sizes DataFrame
# simple aggregation to count the number of seats in each group
group_sizes = consecutive_seat_group_numbers.groupBy("group_number").agg(count("*").alias("group_size"))

# Step 5: Calculate the number of free seat groups by group_size
result = group_sizes.groupBy("group_size").agg(count("*").alias("nr_of_free_seatgroups"))

# Show the result
result.orderBy("group_size").show()
