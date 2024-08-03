import sys
import os
from pyspark.sql import SparkSession
import pyspark.sql.functions as f
import shutil
import glob


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

# set the general strategy for overwriting data files
overwrite_data_files = True
create_single_file = True
parquet_dir_suffix = '_dir.parquet'

# create a dictionary with the names of the JSON files
json_files = {
    'movies': 'movies.json',
    'genres': 'genres.json',
    'movie_genre': 'movie_genre.json'
}

# load the movies from the local JSON file
df_movies = load_movies_from_json_file(spark,
                                       data_folder_url=None,
                                       json_file_name=get_full_path(json_files['movies'], data_path),
                                       load_remotely=False)

parquet_folder_path = get_full_path(json_files['movies'], data_path)[:-5] + '_dir.parquet'
output_parquet_file = parquet_folder_path
if overwrite_data_files \
        and os.path.isfile(full_path := get_full_path(json_files['movies'], data_path)):
    if create_single_file:
        # save the dataframe as a single parquet file by asking Spark to not parallelize the writing
        # by repartitioning/coalescing the dataframe to a single partition
        df_movies = df_movies.repartition(1)
        df_movies.write.mode('overwrite').parquet(parquet_folder_path)

        part_file = glob.glob(parquet_folder_path + "/part-*.parquet")[0]
        output_parquet_file = parquet_folder_path[:-len(parquet_dir_suffix)] + '.parquet'
        shutil.move(part_file, output_parquet_file)
        shutil.rmtree(parquet_folder_path)

    # save parquet to the same folder as the JSON file, replacing the JSON extension with parquet
    df_movies.write.mode('overwrite').parquet(parquet_folder_path)

# Let us reload the data from the parquet file
# print the schema of the dataframe
# and check if the data is the same as the original data
df_movies_from_parquet = spark.read.parquet(output_parquet_file)
print(f"Schema of the dataframe read from the <{output_parquet_file}> file:")
df_movies_from_parquet.printSchema()
