import sys
import os
from pyspark.sql import SparkSession
import pyspark.sql.functions as f

data_path = os.path.abspath(os.path.join(os.path.dirname(__file__), '..', 'data'))
sys.path.insert(0, data_path)
from employees_data_provider import EmployeesDataProvider

spark = SparkSession.builder \
    .appName("Working with employees") \
    .getOrCreate()

df_employees = EmployeesDataProvider.get_employees_dataframe(spark)

df_employees.show(10)
