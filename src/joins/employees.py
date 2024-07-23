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

df_team_leads_with_number_of_subordinates = (df_employees.alias("tld")
                                             .filter(f.col("tld.lead_employee_id").isNotNull())
                                             .groupBy("tld.lead_employee_id")
                                             .agg(f.count("*").alias("number_of_subordinates")))
df_team_leads_with_number_of_subordinates.show()
