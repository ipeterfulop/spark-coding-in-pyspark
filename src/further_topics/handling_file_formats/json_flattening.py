from pyspark.sql import SparkSession
from pyspark.sql.functions import explode, col

# Initialize Spark session
spark = SparkSession.builder.appName("FlattenJSON").getOrCreate()

# Sample data
data = [
    (
        "101",
        {
            "first_name": "George",
            "last_name": "Washington",
            "age": 57,
            "address": {
                "country_ISO2_code": "IR",
                "street": "Flowers road",
                "house_number": 38,
                "City": "Dublin",
            },
            "emails": ({"type": "work", "email": "georgew@workplace.com"}, {"type": "personal", "email": "georgew@gmail.com"})
        },
    ),
    (
        "102",
        {
            "first_name": "John",
            "last_name": "Adams",
            "age": 61,
            "address": {
                "country_ISO2_code": "US",
                "street": "Main street",
                "house_number": 12,
                "City": "New York",
            },
            "emails": ({"type": "personal", "email": "john.adams@gmail.com"},)
        },
    ),
    (
        "103",
        {
            "first_name": "Thomas",
            "last_name": "Jefferson",
            "age": 83,
            "address": {
                "country_ISO2_code": "US",
                "street": "Broadway",
                "house_number": 4,
                "City": "Chicago",
            },
            "emails": ({"type": "work", "email": "thomas.jefferson@thefirm.com"},{"type": "personal", "email": "thomas@thejeffersos.com"})
        },
    ),
]

# Create a DataFrame
df = spark.createDataFrame(data, ["id", "info"])

# Flatten the DataFrame
flattened_df = df.select(
    col("id"),
    col("info.first_name").alias("first_name"),
    col("info.last_name").alias("last_name"),
    col("info.age").alias("age"),
    col("info.address.country_ISO2_code").alias("country_ISO2_code"),
    col("info.address.street").alias("street"),
    col("info.address.house_number").alias("house_number"),
    col("info.address.City").alias("city"),
    explode("info.emails").alias("email")
).select(
    col("id"),
    col("first_name"),
    col("last_name"),
    col("age"),
    col("country_ISO2_code"),
    col("street"),
    col("house_number"),
    col("city"),
    col("email.type").alias("email_type"),
    col("email.email").alias("email_address")
)

# Show the flattened DataFrame
flattened_df.show(truncate=False)

# Stop the Spark session
spark.stop()

