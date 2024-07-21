"""
Given a list of movies released in the 90s:
- Each movie can belong to zero or multiple genres.
- Each movie can feature zero or more actors.

Create a customized list using PySpark DataFrame:
- First column: title - movie titles in alphabetical order.
- Second column: genre - genres of each movie, listed alphabetically.
- Third column: actor_name - actors in each movie, listed alphabetically.

Note that the columns may vary in length.

For a subset of movies with IDs 177 and 184, the output would look as follows:
+---------------+----------+----------------------+
|title          |genre     |actor_name            |
+---------------+----------+----------------------+
|Jackie Brown   |Comedy    |Adam Bryant           |
|The Fisher King|Crime     |Aimee Graham          |
|null           |Drama     |Amanda Plummer        |
|null           |Romance   |Bradley Gregg         |
|null           |null      |Hattie Winston        |
|null           |null      |Jeffrey Deedrick      |
|null           |null      |John de Lancie        |
|null           |null      |John Ottavino         |
|null           |null      |David Hyde Pierce     |
|null           |null      |Michael Bowen         |
|null           |null      |Gary Mann             |
|null           |null      |Lisa Blades           |
|null           |null      |Warren Olney          |
|null           |null      |Lara Harris           |
|null           |null      |Lisa Gay Hamilton     |
|null           |null      |Diana Uribe           |
|null           |null      |Dan Futterman         |
|null           |null      |Glendon Rich          |
|null           |null      |Robin Williams        |
|null           |null      |Robert Forster        |
|null           |null      |Ellis E. Williams     |
|null           |null      |Vanessia Valentino    |
|null           |null      |Michael Jeter         |
|null           |null      |Diane Robin           |
|null           |null      |Herbert Hans Wilmsen  |
|null           |null      |Bridget Fonda         |
|null           |null      |Harry Shearer         |
|null           |null      |Ted Ross              |
|null           |null      |Melinda Culea         |
|null           |null      |Laura Lovelace        |
|null           |null      |Jeff Bridges          |
|null           |null      |Tom Waits             |
|null           |null      |Mercedes Ruehl        |
|null           |null      |Chris Tucker          |
|null           |null      |Roy Nesvold           |
|null           |null      |Julia Ervin           |
|null           |null      |Kathy Najimy          |
|null           |null      |Sid Haig              |
|null           |null      |Tangie Ambrose        |
|null           |null      |Michelle Berube       |
|null           |null      |Candice Briese        |
|null           |null      |Juliet Long           |
|null           |null      |Michael Keaton        |
|null           |null      |Frazer Smith          |
|null           |null      |Tom Lister Jr.        |
|null           |null      |Mary Ann Schmidt      |
|null           |null      |T'Keyah Crystal Keymah|
|null           |null      |Jayce Bartok          |
|null           |null      |Paul Lombardi         |
|null           |null      |Samuel L. Jackson     |
|null           |null      |James Remini          |
|null           |null      |Robert De Niro        |
|null           |null      |Pam Grier             |
|null           |null      |Elizabeth McInerney   |
|null           |null      |Mark Bowden           |
|null           |null      |Colleen Mayne         |
|null           |null      |Christine Lydon       |
+---------------+----------+----------------------+
"""

import sys
import os
from pyspark.sql import SparkSession
import pyspark.sql.functions as f
from pyspark.sql.window import Window

data_path = os.path.abspath(os.path.join(os.path.dirname(__file__), '..', 'data'))
sys.path.insert(0, data_path)
from movie_data_loader import *

spark = SparkSession.builder.appName("MoviesCustomList").getOrCreate()

df_movies = load_movies_from_json_file(spark, 'movie90s.json')
df_genres = load_genres_from_json_file(spark)
df_movie_genre = load_movie_genres_from_json_file(spark, 'movie_genre_90s.json')
df_actors = load_actors_from_json_file(spark)
df_movie_actor = load_movie_actor_from_json_file(spark, json_file_path='movie_actor_90s.json')

selected_movie_ids = [177, 184]

df_movies_selected = (df_movies
                      .filter(f.col("movie_id").isin(selected_movie_ids))
                      .select("title")
                      .withColumn("movie_rank", f.rank().over(Window.orderBy("title")))
                      )

df_related_genres = (df_movie_genre
                     .filter(f.col("movie_id").isin(selected_movie_ids))
                     .select("genre_id")
                     .dropDuplicates()
                     .join(df_genres, "genre_id")
                     .select("genre_name")
                     .withColumn("genre_rank", f.rank().over(Window.orderBy("genre_name")))
                     )

df_related_actors = (df_movie_actor
                     .filter(f.col("movie_id").isin(selected_movie_ids))
                     .select("person_id")
                     .dropDuplicates()
                     .join(df_actors, "person_id")
                     .select("person_name")
                     .withColumn("actor_rank", f.rank().over(Window.orderBy("person_name")))
                     )
max_rank = max(df_movies_selected.count(), df_related_genres.count(), df_related_actors.count())
df_result = (df_movies_selected
             .join(df_related_genres, df_movies_selected["movie_rank"] == df_related_genres["genre_rank"], "full_outer")
             .join(df_related_actors, df_related_genres["genre_rank"] == df_related_actors["actor_rank"], "full_outer")
             .orderBy(f.coalesce(f.col("movie_rank"), f.lit(max_rank + 1)),
                      f.coalesce(f.col("genre_rank"), f.lit(max_rank + 1)),
                      f.coalesce(f.col("actor_rank"), f.lit(max_rank + 1)))
             .select("title", "genre_name", "person_name")
             )

df_result.show(100)
