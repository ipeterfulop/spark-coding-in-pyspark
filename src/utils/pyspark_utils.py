from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.types import StructType, StructField, StringType
from pyspark.sql.functions import count, countDistinct
from typing import List, Any, Optional, Dict


class PySparkUtils:

    @staticmethod
    def get_common_columns(*dataframes: DataFrame) -> List[str]:
        """Finds the common columns across multiple DataFrames.

        Parameters:
        :param dataframes: Variable number of DataFrame arguments.
        :type dataframes: DataFrame

        :return: A list of strings containing the names of columns that are common across all provided DataFrames.
        :rtype: List[str]
        """

        if not dataframes:
            return []

        common_columns = set(dataframes[0].columns)
        for df in dataframes[1:]:
            common_columns.intersection_update(set(df.columns))

        return list(common_columns)

    @staticmethod
    def has_duplicates(df: DataFrame, *columns: List[str]) -> Dict[str, bool]:
        duplicates_dict = {}

        for col_name in columns:
            df_agg = df.select(col_name).agg(count(col_name).alias('total'), countDistinct(col_name).alias('distinct'))
            # Fetch the results as a local dictionary
            result = df_agg.collect()[0].asDict()

            # Determine if there are duplicates
            duplicates_dict[col_name] = result['total'] > result['distinct']

        return duplicates_dict

    @staticmethod
    def covert_list_to_dataframe(list_of_objects: List[Any], context: Any) -> DataFrame:
        """
        Converts a list of objects into a Spark DataFrame using the given Spark context.

        :param list_of_objects: A list containing objects that will form the rows of the DataFrame.
        :type list_of_objects: List[Any]
        :param context: The Spark context used to create the DataFrame.
        :type context: SparkSession
        :return: A DataFrame where each object from the list becomes a row.
        :rtype: DataFrame
        :raises TypeError: If the input is not a list.

        """

        if not isinstance(list_of_objects, List):
            raise TypeError("list_of_objects must be a list")
        schema = StructType([StructField(f"element_{i}", StringType(), True)
                             for i in range(len(list_of_objects))])
        data = [tuple([str(element) for element in list_of_objects])]

        return context.createDataFrame(data, schema)

    @staticmethod
    def convert_dict_to_dataframe(dict_of_objects: Dict[str, Any], context: Any) -> DataFrame:
        columns = sorted(dict_of_objects.keys())
        schema = StructType([StructField(col, StringType(), True)
                             for col in columns])
        data = [tuple(str(dict_of_objects[col]) for col in columns)]

        return context.createDataFrame(data, schema)

    @staticmethod
    def are_dataframes_equal(df1: DataFrame, df2: DataFrame) -> bool:
        return df1.schema == df2.schema \
            and df1.count() == df2.count() \
            and (diff_df := df1.subtract(df2).union(df2.subtract(df1))).count() == 0


class ExtendedDataFrame(DataFrame):

    def __init__(self,
                 dataframe: DataFrame,
                 context: Optional[Any] = None):
        self.df = dataframe
        self.context = context

    def has_column(self, column_name: str) -> bool:
        """
         Check if the DataFrame has a column with the given name.

         Parameters:
         column_name (str): The name of the column to check.

         Returns:
         bool: True if the DataFrame has the column, False otherwise.
         """
        return column_name in self.columns

    def get_common_columns(self, *dataframes) -> List[str]:
        return PySparkUtils.get_common_columns(self.df, *dataframes)
