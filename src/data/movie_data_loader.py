"""
The current file contains a set of helper functions to load movie data from JSON files.
The data is used as input in different challenging tasks throughout the repository.
The movies database is part of my "SQL Essentials" course:
https://github.com/ipeterfulop/sql-essentials-course/tree/main/practice/movies
"""

import os
import typing
from pyspark.sql.types import IntegerType, StructType, StructField, FloatType, DateType, StringType
from pyspark.sql.dataframe import DataFrame
from pyspark.sql import SparkSession


def load_movies_from_json_file(spark: SparkSession, json_file_path: str = 'movies.json') -> DataFrame:
    path_to_json_file = os.path.join(
        os.path.dirname(os.path.abspath(__file__)),
        json_file_path
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


def load_genres_from_json_file(spark: SparkSession, json_file_path: str = 'genres.json') -> DataFrame:
    path_to_json_file = os.path.join(
        os.path.dirname(os.path.abspath(__file__)),
        json_file_path
    )
    schema_genres = StructType([
        StructField("genre_id", IntegerType(), nullable=False),
        StructField("genre_name", StringType(), nullable=False)
    ])
    return spark.read.schema(schema_genres).option("multiLine", "true").json(path_to_json_file)


def load_movie_genres_from_json_file(spark: SparkSession, json_file_path: str = 'movie_genre.json') -> DataFrame:
    path_to_json_file = os.path.join(
        os.path.dirname(os.path.abspath(__file__)),
        json_file_path
    )
    schema_movie_genres = StructType([
        StructField("movie_id", IntegerType(), nullable=False),
        StructField("genre_id", IntegerType(), nullable=False)
    ])
    return spark.read.schema(schema_movie_genres).option("multiLine", "true").json(path_to_json_file)


def load_actors_from_json_file(spark: SparkSession, json_file_path='actors.json') -> DataFrame:
    path_to_json_file = os.path.join(
        os.path.dirname(os.path.abspath(__file__)),
        json_file_path
    )
    schema_actors = StructType([
        StructField("person_id", IntegerType(), nullable=False),
        StructField("person_name", StringType(), nullable=False)
    ])
    return spark.read.schema(schema_actors).option("multiLine", "true").json(path_to_json_file)


def load_movie_actor_from_json_file(spark: SparkSession, json_file_path: str = 'movie_actor.json') -> DataFrame:
    path_to_json_file = os.path.join(
        os.path.dirname(os.path.abspath(__file__)),
        json_file_path
    )
    schema_movie_actors = StructType([
        StructField("movie_id", IntegerType(), nullable=False),
        StructField("person_id", IntegerType(), nullable=False),
        StructField("character_name", StringType(), nullable=False),
        StructField("gender_id", IntegerType(), nullable=False),
        StructField("cast_order", IntegerType(), nullable=False)
    ])
    return spark.read.schema(schema_movie_actors).option("multiLine", "true").json(path_to_json_file)
