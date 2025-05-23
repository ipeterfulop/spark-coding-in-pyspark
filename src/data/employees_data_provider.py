from typing import List, Dict, Tuple
from pyspark.sql.types import IntegerType, StructType, StructField, FloatType, DateType, StringType
from pyspark.sql.dataframe import DataFrame
from pyspark.sql import SparkSession
import pyspark.sql.functions as f


class EmployeesDataProvider:

    @staticmethod
    def get_employees_dataframe(spark: SparkSession) -> DataFrame:
        df = spark.createDataFrame(EmployeesDataProvider.get_employees_list(),
                                   EmployeesDataProvider.get_employees_schema())
        df = df \
            .withColumn("hire_date", f.to_date("hire_date", "yyyy-MM-dd")) \
            .withColumn("birth_date", f.to_date("birth_date", "yyyy-MM-dd")) \
            .withColumn("last_salary_review_date", f.to_date("last_salary_review_date", "yyyy-MM-dd")) \
            .withColumn("date_of_last_medical_analysis", f.to_date("date_of_last_medical_analysis", "yyyy-MM-dd"))

        return df

    @staticmethod
    def get_business_units_dataframe(spark: SparkSession) -> DataFrame:
        df = spark.createDataFrame(EmployeesDataProvider.get_business_units_list(),
                                   EmployeesDataProvider.get_business_units_schema())
        df = df.withColumn("activity_start_date", f.to_date("activity_start_date", "yyyy-MM-dd"))

        return df

    @staticmethod
    def get_employees_schema() -> StructType:
        return StructType([
            StructField("id", IntegerType(), nullable=True),
            StructField("first_name", StringType(), nullable=True),
            StructField("middle_name", StringType(), nullable=True),
            StructField("last_name", StringType(), nullable=True),
            StructField("hire_date", StringType(), nullable=True),
            StructField("birth_date", StringType(), nullable=True),
            StructField("business_unit_id", IntegerType(), nullable=True),
            StructField("lead_employee_id", IntegerType(), nullable=True),
            StructField("job_title_id", IntegerType(), nullable=True),
            StructField("band_id", IntegerType(), nullable=True),
            StructField("last_salary_review_date", StringType(), nullable=True),
            StructField("salary", StringType(), nullable=True),
            StructField("email_address", StringType(), nullable=True),
            StructField("gender", StringType(), nullable=True),
            StructField("yearly_leave_days", IntegerType(), nullable=True),
            StructField("personal_id_number", StringType(), nullable=True),
            StructField("date_of_last_medical_analysis", StringType(), nullable=True),
            StructField("residence_country_id", StringType(), nullable=True),
            StructField("native_language_id", StringType(), nullable=True),
            StructField("preferred_language_id", StringType(), nullable=True)
        ])

    @staticmethod
    def get_business_units_schema() -> StructType:
        return StructType([
            StructField("id", IntegerType(), nullable=False),
            StructField("name", StringType(), nullable=False),
            StructField("activity_start_date", StringType(), nullable=False)
        ])

    @staticmethod
    def get_employees_list() -> List[Tuple]:
        list_of_dicts = [
            {
                "id": 1,
                "first_name": "Gabor",
                "middle_name": None,
                "last_name": "Nagy",
                "hire_date": "2022-02-15",
                "birth_date": "1990-08-10",
                "business_unit_id": 1,
                "lead_employee_id": 10,
                "job_title_id": 1,
                "band_id": 1,
                "last_salary_review_date": "2023-03-25",
                "salary": 2500,
                "email_address": "gabor@hcl.com",
                "gender": "M",
                "yearly_leave_days": 25,
                "personal_id_number": "123456AB",
                "date_of_last_medical_analysis": "2022-10-05",
                "residence_country_id": "HUN",
                "native_language_id": 367,
                "preferred_language_id": 369
            },
            {
                "id": 2,
                "first_name": "Istvan",
                "middle_name": "Peter",
                "last_name": "Kiss",
                "hire_date": "2022-03-01",
                "birth_date": "1985-05-20",
                "business_unit_id": 2,
                "lead_employee_id": 10,
                "job_title_id": 2,
                "band_id": 2,
                "last_salary_review_date": None,
                "salary": 3500,
                "email_address": "istvan@hcl.com",
                "gender": "M",
                "yearly_leave_days": 30,
                "personal_id_number": "654321CD",
                "date_of_last_medical_analysis": "2019-07-12",
                "residence_country_id": "ROM",
                "native_language_id": 367,
                "preferred_language_id": 369
            },
            {
                "id": 3,
                "first_name": "Zoltan",
                "middle_name": None,
                "last_name": "Toth",
                "hire_date": "2022-03-15",
                "birth_date": "1995-12-02",
                "business_unit_id": 3,
                "lead_employee_id": 10,
                "job_title_id": 3,
                "band_id": 3,
                "last_salary_review_date": None,
                "salary": 2000,
                "email_address": "zoltan@hcl.com",
                "gender": "M",
                "yearly_leave_days": 20,
                "personal_id_number": "789012EF",
                "date_of_last_medical_analysis": "2022-11-30",
                "residence_country_id": "USA",
                "native_language_id": 367,
                "preferred_language_id": 369
            },
            {
                "id": 4,
                "first_name": "Eva",
                "middle_name": None,
                "last_name": "Horvath",
                "hire_date": "2022-04-01",
                "birth_date": "1992-03-18",
                "business_unit_id": 4,
                "lead_employee_id": 10,
                "job_title_id": 4,
                "band_id": 4,
                "last_salary_review_date": "2022-09-10",
                "salary": 4500,
                "email_address": "eva@hcl.com",
                "gender": "F",
                "yearly_leave_days": 28,
                "personal_id_number": "987654FG",
                "date_of_last_medical_analysis": "2021-06-25",
                "residence_country_id": "HUN",
                "native_language_id": 893,
                "preferred_language_id": 894
            },
            {
                "id": 5,
                "first_name": "Katalin",
                "middle_name": "Anna",
                "last_name": "Szabo",
                "hire_date": "2022-05-01",
                "birth_date": "1998-07-05",
                "business_unit_id": 5,
                "lead_employee_id": 10,
                "job_title_id": 5,
                "band_id": 5,
                "last_salary_review_date": "2023-01-20",
                "salary": 2800,
                "email_address": "katalin@hcl.com",
                "gender": "F",
                "yearly_leave_days": 35,
                "personal_id_number": "567890GH",
                "date_of_last_medical_analysis": "2019-03-15",
                "residence_country_id": "ROM",
                "native_language_id": 893,
                "preferred_language_id": 894
            },
            {
                "id": 6,
                "first_name": "Andras",
                "middle_name": "Balazs",
                "last_name": "Kovacs",
                "hire_date": "2022-06-15",
                "birth_date": "1991-10-30",
                "business_unit_id": 2,
                "lead_employee_id": 10,
                "job_title_id": 1,
                "band_id": 6,
                "last_salary_review_date": None,
                "salary": 2600,
                "email_address": "andras@hcl.com",
                "gender": "M",
                "yearly_leave_days": 32,
                "personal_id_number": "234567IJ",
                "date_of_last_medical_analysis": "2023-04-15",
                "residence_country_id": "HUN",
                "native_language_id": 367,
                "preferred_language_id": 369
            },
            {
                "id": 7,
                "first_name": "Anita",
                "middle_name": None,
                "last_name": "Molnar",
                "hire_date": "2022-07-01",
                "birth_date": "1997-09-12",
                "business_unit_id": 3,
                "lead_employee_id": 10,
                "job_title_id": 2,
                "band_id": 7,
                "last_salary_review_date": "2023-04-30",
                "salary": 3800,
                "email_address": "anita@hcl.com",
                "gender": "F",
                "yearly_leave_days": 27,
                "personal_id_number": "456789JK",
                "date_of_last_medical_analysis": "2022-05-10",
                "residence_country_id": "USA",
                "native_language_id": 367,
                "preferred_language_id": 369
            },
            {
                "id": 8,
                "first_name": "Ferenc",
                "middle_name": "Peter",
                "last_name": "Kovacs",
                "hire_date": "2022-08-15",
                "birth_date": "1996-11-25",
                "business_unit_id": 4,
                "lead_employee_id": 10,
                "job_title_id": 3,
                "band_id": 7,
                "last_salary_review_date": None,
                "salary": 2900,
                "email_address": "ferenc@hcl.com",
                "gender": "M",
                "yearly_leave_days": 22,
                "personal_id_number": "345678KL",
                "date_of_last_medical_analysis": None,
                "residence_country_id": "HUN",
                "native_language_id": 893,
                "preferred_language_id": 894
            },
            {
                "id": 9,
                "first_name": "Lilla",
                "middle_name": None,
                "last_name": "Toth",
                "hire_date": "2022-09-01",
                "birth_date": "1989-04-15",
                "business_unit_id": None,
                "lead_employee_id": 10,
                "job_title_id": 4,
                "band_id": 5,
                "last_salary_review_date": "2022-08-10",
                "salary": 4100,
                "email_address": "lilla@hcl.com",
                "gender": "F",
                "yearly_leave_days": 33,
                "personal_id_number": "234567MN",
                "date_of_last_medical_analysis": None,
                "residence_country_id": "ROM",
                "native_language_id": 367,
                "preferred_language_id": None
            },
            {
                "id": 10,
                "first_name": "Laszlo",
                "middle_name": "Gabor",
                "last_name": "Nagy",
                "hire_date": "2022-10-01",
                "birth_date": "1993-06-28",
                "business_unit_id": 5,
                "lead_employee_id": None,
                "job_title_id": 5,
                "band_id": None,
                "last_salary_review_date": "2023-02-18",
                "salary": 3200,
                "email_address": "laszlo@hcl.com",
                "gender": "M",
                "yearly_leave_days": 24,
                "personal_id_number": "123456OP",
                "date_of_last_medical_analysis": "2020-04-20",
                "residence_country_id": "USA",
                "native_language_id": 894,
                "preferred_language_id": None
            },
            {
                "id": 11,
                "first_name": "Anna",
                "middle_name": None,
                "last_name": "Kovacs",
                "hire_date": "2022-05-10",
                "birth_date": "1990-03-15",
                "business_unit_id": 1,
                "lead_employee_id": 1,
                "job_title_id": 1,
                "band_id": 1,
                "last_salary_review_date": "2022-06-15",
                "salary": 3500,
                "email_address": "anna.kovacs@hcl.com",
                "gender": "F",
                "yearly_leave_days": 25,
                "personal_id_number": "900315AB",
                "date_of_last_medical_analysis": "2022-04-01",
                "residence_country_id": "HUN",
                "native_language_id": 367,
                "preferred_language_id": None
            },
            {
                "id": 12,
                "first_name": "Balazs",
                "middle_name": None,
                "last_name": "Toth",
                "hire_date": "2021-08-20",
                "birth_date": "1988-12-10",
                "business_unit_id": 2,
                "lead_employee_id": 2,
                "job_title_id": 2,
                "band_id": 2,
                "last_salary_review_date": "2023-02-15",
                "salary": 4000,
                "email_address": "balazs.toth@hcl.com",
                "gender": "M",
                "yearly_leave_days": 30,
                "personal_id_number": "881210CD",
                "date_of_last_medical_analysis": None,
                "residence_country_id": "HUN",
                "native_language_id": 369,
                "preferred_language_id": None
            },
            {
                "id": 13,
                "first_name": "Gabor",
                "middle_name": None,
                "last_name": "Nagy",
                "hire_date": "2023-01-05",
                "birth_date": "1995-07-03",
                "business_unit_id": 3,
                "lead_employee_id": 3,
                "job_title_id": 3,
                "band_id": 3,
                "last_salary_review_date": None,
                "salary": 2500,
                "email_address": "gabor.nagy@hcl.com",
                "gender": "M",
                "yearly_leave_days": 28,
                "personal_id_number": "950703EF",
                "date_of_last_medical_analysis": "2022-09-20",
                "residence_country_id": "ROM",
                "native_language_id": 367,
                "preferred_language_id": None
            },
            {
                "id": 14,
                "first_name": "Katalin",
                "middle_name": None,
                "last_name": "Szabo",
                "hire_date": "2020-04-15",
                "birth_date": "1992-10-24",
                "business_unit_id": 4,
                "lead_employee_id": 4,
                "job_title_id": 4,
                "band_id": 4,
                "last_salary_review_date": None,
                "salary": 3000,
                "email_address": "katalin.szabo@hcl.com",
                "gender": "F",
                "yearly_leave_days": 35,
                "personal_id_number": "921024GH",
                "date_of_last_medical_analysis": "2023-04-15",
                "residence_country_id": "USA",
                "native_language_id": 369,
                "preferred_language_id": 893
            },
            {
                "id": 15,
                "first_name": "Istvan",
                "middle_name": None,
                "last_name": "Horvath",
                "hire_date": "2019-11-30",
                "birth_date": "1985-06-08",
                "business_unit_id": 5,
                "lead_employee_id": 5,
                "job_title_id": 5,
                "band_id": 5,
                "last_salary_review_date": "2021-07-25",
                "salary": 4500,
                "email_address": "istvan.horvath@hcl.com",
                "gender": "M",
                "yearly_leave_days": 20,
                "personal_id_number": "850608IJ",
                "date_of_last_medical_analysis": "2021-10-10",
                "residence_country_id": "HUN",
                "native_language_id": 894,
                "preferred_language_id": 367
            },
            {
                "id": 16,
                "first_name": "Eva",
                "middle_name": None,
                "last_name": "Kiss",
                "hire_date": "2021-06-10",
                "birth_date": "1989-09-12",
                "business_unit_id": None,
                "lead_employee_id": None,
                "job_title_id": None,
                "band_id": 6,
                "last_salary_review_date": None,
                "salary": 2800,
                "email_address": "eva.kiss@hcl.com",
                "gender": "F",
                "yearly_leave_days": 22,
                "personal_id_number": "890912KL",
                "date_of_last_medical_analysis": None,
                "residence_country_id": "ROM",
                "native_language_id": 367,
                "preferred_language_id": 893
            },
            {
                "id": 17,
                "first_name": "Tamas",
                "middle_name": None,
                "last_name": "Molnar",
                "hire_date": "2019-04-03",
                "birth_date": "1980-11-19",
                "business_unit_id": None,
                "lead_employee_id": None,
                "job_title_id": None,
                "band_id": 7,
                "last_salary_review_date": None,
                "salary": 2000,
                "email_address": "tamas.molnar@hcl.com",
                "gender": "M",
                "yearly_leave_days": 23,
                "personal_id_number": "801119MN",
                "date_of_last_medical_analysis": None,
                "residence_country_id": "USA",
                "native_language_id": 894,
                "preferred_language_id": 367
            },
            {
                "id": 18,
                "first_name": "Zsuzsa",
                "middle_name": None,
                "last_name": "Papp",
                "hire_date": "2022-09-05",
                "birth_date": "1993-04-28",
                "business_unit_id": None,
                "lead_employee_id": None,
                "job_title_id": None,
                "band_id": 1,
                "last_salary_review_date": "2022-11-15",
                "salary": 3200,
                "email_address": "zsuzsa.papp@hcl.com",
                "gender": "F",
                "yearly_leave_days": 33,
                "personal_id_number": "930428OP",
                "date_of_last_medical_analysis": "2022-10-20",
                "residence_country_id": "HUN",
                "native_language_id": 367,
                "preferred_language_id": 369
            },
            {
                "id": 19,
                "first_name": "Laszlo",
                "middle_name": None,
                "last_name": "Varga",
                "hire_date": "2020-11-10",
                "birth_date": "1987-08-17",
                "business_unit_id": None,
                "lead_employee_id": None,
                "job_title_id": None,
                "band_id": 2,
                "last_salary_review_date": "2021-01-20",
                "salary": 3800,
                "email_address": "laszlo.varga@hcl.com",
                "gender": "M",
                "yearly_leave_days": 24,
                "personal_id_number": "870817QR",
                "date_of_last_medical_analysis": "2020-12-15",
                "residence_country_id": "ROM",
                "native_language_id": 369,
                "preferred_language_id": 894
            },
            {
                "id": 20,
                "first_name": "Noemi",
                "middle_name": None,
                "last_name": "Biro",
                "hire_date": "2021-07-22",
                "birth_date": "1994-03-05",
                "business_unit_id": None,
                "lead_employee_id": None,
                "job_title_id": None,
                "band_id": 3,
                "last_salary_review_date": None,
                "salary": 2700,
                "email_address": "noemi.biro@hcl.com",
                "gender": "F",
                "yearly_leave_days": 21,
                "personal_id_number": "940305ST",
                "date_of_last_medical_analysis": "2021-10-01",
                "residence_country_id": "USA",
                "native_language_id": 894,
                "preferred_language_id": 367
            },
            {
                "id": 21,
                "first_name": "Gabor",
                "middle_name": None,
                "last_name": "Kovacs",
                "hire_date": "2022-03-15",
                "birth_date": "1990-05-10",
                "business_unit_id": 2,
                "lead_employee_id": 6,
                "job_title_id": 2,
                "band_id": 3,
                "last_salary_review_date": None,
                "salary": 3500,
                "email_address": "gabor.kovacs@hcl.com",
                "gender": "M",
                "yearly_leave_days": 25,
                "personal_id_number": "123456AB",
                "date_of_last_medical_analysis": "2020-12-15",
                "residence_country_id": "HUN",
                "native_language_id": 367,
                "preferred_language_id": 367
            },
            {
                "id": 22,
                "first_name": "Anna",
                "middle_name": None,
                "last_name": "Nagy",
                "hire_date": "2020-08-20",
                "birth_date": "1991-11-22",
                "business_unit_id": 5,
                "lead_employee_id": 1,
                "job_title_id": 4,
                "band_id": 6,
                "last_salary_review_date": "2022-03-12",
                "salary": 3800,
                "email_address": "anna.nagy@hcl.com",
                "gender": "F",
                "yearly_leave_days": 30,
                "personal_id_number": "654321CD",
                "date_of_last_medical_analysis": None,
                "residence_country_id": "ROM",
                "native_language_id": 369,
                "preferred_language_id": 367
            },
            {
                "id": 23,
                "first_name": "Ferenc",
                "middle_name": None,
                "last_name": "Kiss",
                "hire_date": "2019-06-10",
                "birth_date": "1995-09-18",
                "business_unit_id": 4,
                "lead_employee_id": 10,
                "job_title_id": 1,
                "band_id": 2,
                "last_salary_review_date": "2023-01-20",
                "salary": 2900,
                "email_address": "ferenc.kiss@hcl.com",
                "gender": "M",
                "yearly_leave_days": 22,
                "personal_id_number": "123789GH",
                "date_of_last_medical_analysis": "2022-02-05",
                "residence_country_id": "HUN",
                "native_language_id": 894,
                "preferred_language_id": 367
            },
            {
                "id": 24,
                "first_name": "Katalin",
                "middle_name": None,
                "last_name": "Varga",
                "hire_date": "2019-11-05",
                "birth_date": "1994-07-28",
                "business_unit_id": 1,
                "lead_employee_id": 8,
                "job_title_id": 5,
                "band_id": 5,
                "last_salary_review_date": None,
                "salary": 2600,
                "email_address": "katalin.varga@hcl.com",
                "gender": "F",
                "yearly_leave_days": 26,
                "personal_id_number": "456123KL",
                "date_of_last_medical_analysis": None,
                "residence_country_id": "USA",
                "native_language_id": 893,
                "preferred_language_id": 369
            },
            {
                "id": 25,
                "first_name": "Miklos",
                "middle_name": None,
                "last_name": "Kovacs",
                "hire_date": "2021-04-08",
                "birth_date": "1992-03-01",
                "business_unit_id": 3,
                "lead_employee_id": 9,
                "job_title_id": 3,
                "band_id": 4,
                "last_salary_review_date": None,
                "salary": 3100,
                "email_address": "miklos.kovacs@hcl.com",
                "gender": "M",
                "yearly_leave_days": 28,
                "personal_id_number": "987654MN",
                "date_of_last_medical_analysis": "2023-04-15",
                "residence_country_id": "ROM",
                "native_language_id": 367,
                "preferred_language_id": 367
            },
            {
                "id": 26,
                "first_name": "Eva",
                "middle_name": None,
                "last_name": "Szabo",
                "hire_date": "2023-02-22",
                "birth_date": "1993-08-12",
                "business_unit_id": 2,
                "lead_employee_id": 7,
                "job_title_id": 2,
                "band_id": 3,
                "last_salary_review_date": None,
                "salary": 2800,
                "email_address": "eva.szabo@hcl.com",
                "gender": "F",
                "yearly_leave_days": 20,
                "personal_id_number": "543219OP",
                "date_of_last_medical_analysis": "2022-06-30",
                "residence_country_id": "HUN",
                "native_language_id": 894,
                "preferred_language_id": 894
            },
            {
                "id": 27,
                "first_name": "Andras",
                "middle_name": None,
                "last_name": "Farkas",
                "hire_date": "2019-08-17",
                "birth_date": "1996-04-14",
                "business_unit_id": 5,
                "lead_employee_id": 1,
                "job_title_id": 4,
                "band_id": 6,
                "last_salary_review_date": "2022-05-25",
                "salary": 3500,
                "email_address": "andras.farkas@hcl.com",
                "gender": "M",
                "yearly_leave_days": 33,
                "personal_id_number": "785621QR",
                "date_of_last_medical_analysis": None,
                "residence_country_id": "USA",
                "native_language_id": 367,
                "preferred_language_id": 369
            },
            {
                "id": 28,
                "first_name": "Krisztina",
                "middle_name": None,
                "last_name": "Toth",
                "hire_date": "2022-01-05",
                "birth_date": "1997-12-03",
                "business_unit_id": 2,
                "lead_employee_id": 6,
                "job_title_id": 2,
                "band_id": 3,
                "last_salary_review_date": None,
                "salary": 2900,
                "email_address": "krisztina.toth@hcl.com",
                "gender": "F",
                "yearly_leave_days": 24,
                "personal_id_number": "896513XY",
                "date_of_last_medical_analysis": "2021-09-10",
                "residence_country_id": "HUN",
                "native_language_id": 367,
                "preferred_language_id": 367
            },
            {
                "id": 29,
                "first_name": "Bela",
                "middle_name": None,
                "last_name": "Nagy",
                "hire_date": "2020-12-03",
                "birth_date": "1998-10-06",
                "business_unit_id": 4,
                "lead_employee_id": 10,
                "job_title_id": 1,
                "band_id": 2,
                "last_salary_review_date": "2020-11-30",
                "salary": 3100,
                "email_address": "bela.nagy@hcl.com",
                "gender": "M",
                "yearly_leave_days": 21,
                "personal_id_number": "986123WZ",
                "date_of_last_medical_analysis": None,
                "residence_country_id": "ROM",
                "native_language_id": 367,
                "preferred_language_id": 369
            }
        ]
        return EmployeesDataProvider.convert_list_of_dicts_to_list_of_tuples(list_of_dicts)

    @staticmethod
    def get_business_units_list() -> List[Dict]:
        return [
            {
                "id": 1,
                "name": "Consulting 1",
                "activity_start_date": "2010-01-15"
            },
            {
                "id": 2,
                "name": "Consulting 2",
                "activity_start_date": "2011-02-20"
            },
            {
                "id": 3,
                "name": "Managed services",
                "activity_start_date": "2012-03-10"
            },
            {
                "id": 4,
                "name": "Team augmentation 1",
                "activity_start_date": "2011-04-05"
            },
            {
                "id": 5,
                "name": "Team Augmentation 2",
                "activity_start_date": "2011-05-12"
            },
            {
                "id": 6,
                "name": "Generative AI",
                "activity_start_date": "2023-09-09"
            }
        ]

    @staticmethod
    def convert_list_of_dicts_to_list_of_tuples(list_of_dicts: List[Dict]) -> List[Tuple]:
        """
        Convert a list of dictionaries to a list of tuples.

        :param list_of_dicts: List of dictionaries
        :return: List of tuples
        """
        if not list_of_dicts:
            return []

        # Get the keys from the first dictionary to maintain the order
        keys = list_of_dicts[0].keys()

        # Convert each dictionary to a tuple
        list_of_tuples = [tuple(d[key] for key in keys) for d in list_of_dicts]

        return list_of_tuples
