import sys
import os
from pyspark.sql import SparkSession
import pyspark.sql.functions as f

data_path = os.path.abspath(os.path.join(os.path.dirname(__file__), '..', 'data'))
sys.path.insert(0, data_path)
from employees_data_provider import EmployeesDataProvider

spark = SparkSession.builder \
    .appName("EmployeesRelatedAssignments") \
    .getOrCreate()

# Create a dataframe with the team leads' names and the number of subordinates they have.
# Order the dataframe by the number of subordinates in descending order.


df_employees = EmployeesDataProvider.get_employees_dataframe(spark)

df_team_leads_with_number_of_subordinates = (df_employees.alias("tld")
                                             .filter(f.col("tld.lead_employee_id").isNotNull())
                                             .groupBy("tld.lead_employee_id")
                                             .agg(f.count("*").alias("number_of_subordinates")))

df_team_leads_with_number_of_subordinates = (df_team_leads_with_number_of_subordinates.alias("tld")
                                             .join(df_employees.alias("emp"),
                                                   f.col("tld.lead_employee_id") == f.col("emp.id"),
                                                   "inner")
                                             .select(f.concat(f.col("emp.first_name"),
                                                              f.lit(" "),
                                                              f.col("emp.last_name")).alias("employee_name"),
                                                     f.col("tld.number_of_subordinates"))
                                             .orderBy(f.col("tld.number_of_subordinates").desc()))

df_team_leads_with_number_of_subordinates.show()

# Create a dataframe with the employees name with their business unit names.
# Order the dataframe by the business unit name in ascending order and by the employee name in descending order.

df_business_units = EmployeesDataProvider.get_business_units_dataframe(spark)

df_employees_with_business_units = (df_employees.alias("emp")
                                    .join(df_business_units.alias("bu"),
                                          f.col("emp.business_unit_id") == f.col("bu.id"))
                                    .select(f.concat(f.col("emp.first_name"), f.lit(" "), f.col("emp.last_name"))
                                            .alias("employee_name"),
                                            f.col("bu.name").alias("business_unit_name"))
                                    .orderBy(f.col("business_unit_name").asc(),
                                             f.col("last_name").asc(),
                                             f.col("first_name").asc()))
df_employees_with_business_units.show()

df = spark.parallelize([("a", ["x", "y", "z"]), ("b", ["p", "r"])])

