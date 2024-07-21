import os
from pyspark.sql.types import IntegerType, StructType, StructField, FloatType, DateType, StringType


def load_movies_from_json_file(spark):
    path_to_json_file = os.path.join(
        os.path.dirname(os.path.abspath(__file__)),
        'movie90s.json'
    )
    schema_movies = StructType([
        StructField("movie_id", IntegerType(), nullable=False),
        StructField("title", StringType(), nullable=False),
        StructField("release_date", DateType(), nullable=False),
        StructField("budget", IntegerType(), nullable=False),
        StructField("revenue", IntegerType(), nullable=False),
        StructField("popularity", FloatType(), nullable=False),
        StructField("vote_average", FloatType(), nullable=False),
        StructField("vote_count", IntegerType(), nullable=False),
        StructField("runtime", IntegerType(), nullable=False)
    ])
    return spark.read.schema(schema_movies).option("multiLine", "true").json(path_to_json_file)


def load_genres_from_json_file(spark):
    path_to_json_file = os.path.join(
        os.path.dirname(os.path.abspath(__file__)),
        'genre90s.json'
    )
    schema_genres = StructType([
        StructField("genre_id", IntegerType(), nullable=False),
        StructField("genre_name", StringType(), nullable=False)
    ])
    return spark.read.schema(schema_genres).option("multiLine", "true").json(path_to_json_file)


def load_movie_genres_from_json_file(spark):
    path_to_json_file = os.path.join(
        os.path.dirname(os.path.abspath(__file__)),
        'movie_genre_90s.json'
    )
    schema_movie_genres = StructType([
        StructField("movie_id", IntegerType(), nullable=False),
        StructField("genre_id", IntegerType(), nullable=False)
    ])
    return spark.read.schema(schema_movie_genres).option("multiLine", "true").json(path_to_json_file)
