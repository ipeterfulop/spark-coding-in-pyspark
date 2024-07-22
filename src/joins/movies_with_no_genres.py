"""
A list of movies released in the 1990s is available in a JSON file.
Additionally,  a list of genres and movie-genre pairs are also provided as JSON files.
Find the movies with no genres associated with them.
"""
import os
import sys
from pyspark.sql import SparkSession
import pyspark.sql.functions as f

data_path = os.path.abspath(os.path.join(os.path.dirname(__file__), '..', 'data'))
sys.path.insert(0, data_path)
from movie_data_loader import *

spark = SparkSession.builder.appName("FindingMoviesWithNoGenre").getOrCreate()

df_movies = load_movies_from_json_file(spark)
df_movie_genre = load_movie_genres_from_json_file(spark)

join_expression = df_movies["movie_id"] == df_movie_genre["movie_id"]

df_movies_with_no_genres = (df_movies.alias('movies')
                            .join(df_movie_genre.alias('movie_genre'), join_expression, "left_anti")
                            .select(f.col("movies.*"))
                            )

df_movies_with_no_genres.show(10)
