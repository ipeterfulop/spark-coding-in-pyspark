"""
A list of movies released in the 1990s is available in a JSON file. Additionally, a list of genres and movie-genre pairs
are also provided as JSON files.
This script reads the data into DataFrames with appropriate schemas and lists the movies in ascending order,
specifying the list of genres. Each movie appears only once in the list.

Steps:
1. Read the JSON files into DataFrames.
2. Perform joins to combine movies with their respective genres.
3. Aggregate the genres for each movie.
4. List the movies in ascending order with the associated genres. Make the genres available as a list of strings and
a concatenated string as well.
"""
import sys
import os
from pyspark.sql import SparkSession
import pyspark.sql.functions as f

data_path = os.path.abspath(os.path.join(os.path.dirname(__file__), '..', 'data'))
sys.path.insert(0, data_path)
from movie_data_loader import *

spark = SparkSession.builder \
    .appName("FindingMovieGenres") \
    .getOrCreate()
data_folder_url = 'https://raw.githubusercontent.com/ipeterfulop/spark-coding-in-pyspark/main/src/data/'

df_movies = load_movies_from_json_file(spark, data_folder_url, 'movie90s.json')
df_genres = load_genres_from_json_file(spark, data_folder_url)
df_movie_genre = load_movie_genres_from_json_file(spark, data_folder_url, 'movie_genre_90s.json')

join_type = "inner"
join_expression = df_movies["movie_id"] == df_movie_genre["movie_id"]

df_movies_with_genres = (df_movies.alias('movies')
                         .join(df_movie_genre.alias('movie_genre'), join_expression, join_type)
                         .join(df_genres.alias('genres'), (df_movie_genre["genre_id"] == df_genres["genre_id"]),
                               "inner")
                         .select(f.col("movies.*"),
                                 f.col("genres.genre_id").alias("genre_id"),
                                 f.col("genres.genre_name").alias("genre_name")
                                 )
                         )

df_movies_with_genres_concatenated = (df_movies_with_genres
                                      .groupBy("movie_id", "title", "release_date", "budget", "revenue", "popularity")
                                      .agg(f.collect_list("genre_name").alias("genre_list"))
                                      ).withColumn("genres", f.concat_ws(", ", f.col("genre_list")))

df_movies_with_genres_concatenated.orderBy("title").show(10)
