"""
The current file contains a set of helper functions to load movie data from JSON files.
The data is used as input in different challenging tasks throughout the repository.
The movies database is part of my "SQL Essentials" course:
https://github.com/ipeterfulop/sql-essentials-course/tree/main/practice/movies
"""

import os
import typing
import requests
from pyspark.sql.types import IntegerType, StructType, StructField, FloatType, DateType, StringType
from pyspark.sql.dataframe import DataFrame
from pyspark.sql import SparkSession


def load_movies_from_json_file(spark: SparkSession,
                               data_folder_url: str,
                               json_file_name: str = 'movies.json'
                               ) -> DataFrame:
    response = requests.get(data_folder_url + json_file_name)
    response.raise_for_status()

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

    spark_df = spark.read.schema(schema_movies).json(spark.sparkContext.parallelize([response.text]))
    return spark_df


def load_genres_from_json_file(spark: SparkSession,
                               data_folder_url: str,
                               json_file_name: str = 'genres.json'
                               ) -> DataFrame:
    response = requests.get(data_folder_url + json_file_name)
    response.raise_for_status()

    schema_genres = StructType([
        StructField("genre_id", IntegerType(), nullable=False),
        StructField("genre_name", StringType(), nullable=False)
    ])
    spark_df = spark.read.schema(schema_genres).json(spark.sparkContext.parallelize([response.text]))
    return spark_df


def load_movie_genres_from_json_file(spark: SparkSession,
                                     data_folder_url: str,
                                     json_file_name: str = 'movie_genre.json',
                                     ) -> DataFrame:
    response = requests.get(data_folder_url + json_file_name)
    response.raise_for_status()

    schema_movie_genres = StructType([
        StructField("movie_id", IntegerType(), nullable=False),
        StructField("genre_id", IntegerType(), nullable=False)
    ])
    spark_df = spark.read.schema(schema_movie_genres).json(spark.sparkContext.parallelize([response.text]))
    return spark_df


def load_actors_from_json_file(spark: SparkSession,
                               data_folder_url: str,
                               json_file_name: str = 'actors.json',
                               ) -> DataFrame:
    response = requests.get(data_folder_url + json_file_name)
    response.raise_for_status()

    schema_actors = StructType([
        StructField("person_id", IntegerType(), nullable=False),
        StructField("person_name", StringType(), nullable=False)
    ])
    spark_df = spark.read.schema(schema_actors).json(spark.sparkContext.parallelize([response.text]))
    return spark_df


def load_movie_actor_from_json_file(spark: SparkSession,
                                    data_folder_url: str,
                                    json_file_name: str = 'movie_actor.json',
                                    ) -> DataFrame:
    response = requests.get(data_folder_url + json_file_name)
    response.raise_for_status()

    schema_movie_actor = StructType([
        StructField("movie_id", IntegerType(), nullable=False),
        StructField("person_id", IntegerType(), nullable=False),
        StructField("character_name", StringType(), nullable=False),
        StructField("gender_id", IntegerType(), nullable=False),
        StructField("cast_order", IntegerType(), nullable=False)
    ])
    spark_df = spark.read.schema(schema_movie_actor).json(spark.sparkContext.parallelize([response.text]))
    return spark_df
