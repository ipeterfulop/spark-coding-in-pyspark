import sys
import os
from pyspark.sql import SparkSession
import pyspark.sql.functions as f


def get_full_path(file_name: str, folder_with_absolute_path: str) -> str:
    return os.path.join(folder_with_absolute_path, file_name)


data_path = os.path.abspath(os.path.join(os.getcwd(),
                                         '../..',
                                         'data'))
sys.path.insert(0, data_path)
from movie_data_loader import *

spark = SparkSession.builder \
    .appName("FindingMovieGenres") \
    .getOrCreate()
data_folder_url = 'https://raw.githubusercontent.com/ipeterfulop/spark-coding-in-pyspark/main/src/data/'

# set the general strategy for overwriting data files
overwrite_data_files = True

# create a dictionary with the names of the JSON files
json_files = {
    'movies': 'movies.json',
    'genres': 'genres.json',
    'movie_genre': 'movie_genre_90s.json'
}

df_movies = load_movies_from_json_file(spark,
                                       data_folder_url,
                                       get_full_path(json_files['movies'], data_path),
                                       load_remotely=False)
parquet_file_path = get_full_path(json_files['movies'], data_path)[:-5] + '.parquet'

if overwrite_data_files \
        and os.path.isfile(full_path := get_full_path(json_files['movies'], data_path)):
    # save parquet to the same folder as the JSON file, replacing the JSON extension with parquet
    df_movies.write.mode('overwrite').parquet(parquet_file_path)

# Let us reload the data from the parquet file
# print the schema of the dataframe
# and check if the data is the same as the original data

df_movies_from_parquet = spark.read.parquet(parquet_file_path)
print(f"Schema of the dataframe read from the <{parquet_file_path}> file:")
df_movies_from_parquet.printSchema()
