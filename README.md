# Spark coding in PySpark

<!-- TOC -->
* [Spark coding in PySpark](#spark-coding-in-pyspark)
  * [1. Introduction](#1-introduction)
  * [2. Currently covered topics](#2-currently-covered-topics)
    * [Window Functions](#window-functions)
  * [3. Running the Solutions](#3-running-the-solutions)
    * [3.1 Using Databricks Community Edition](#31-using-databricks-community-edition)
    * [3.2 Creating a local `conda` environment](#32-creating-a-local-conda-environment)
    * [3.3 Using Apache Spark Docker containers to run pyspark programs using spark-submit](#33-using-apache-spark-docker-containers-to-run-pyspark-programs-using-spark-submit)
      * [3.3.1 Prerequisites](#331-prerequisites)
      * [3.3.2 The Docker container](#332-the-docker-container)
      * [3.3.4 Building and running the Docker containers](#334-building-and-running-the-docker-containers)
      * [3.3.5 Verify that the containers are running](#335-verify-that-the-containers-are-running)
      * [3.3.6 Verify that the containers contains the exercise files](#336-verify-that-the-containers-contains-the-exercise-files)
      * [3.3.7 Get the address where our spark master container is running](#337-get-the-address-where-our-spark-master-container-is-running)
      * [3.3.8 Finding the service name of the master container](#338-finding-the-service-name-of-the-master-container)
      * [3.3.9 Submitting a pyspark program using spark-submit command](#339-submitting-a-pyspark-program-using-spark-submit-command)
  * [4. Miscellaneous](#4-miscellaneous)
    * [4.1 Databricks Certified Associate Developer for Apache Spark](#41-databricks-certified-associate-developer-for-apache-spark)
      * [4.1.1 Exam Details](#411-exam-details)
      * [4.1.2 Minimally Qualified Candidate](#412-minimally-qualified-candidate)
      * [4.1.3 Duration](#413-duration)
      * [4.1.4 Questions](#414-questions)
<!-- TOC -->

## 1. Introduction

This repository contains a collection of assignments with solutions in PySpark. Some assignments are challenges I
encountered during my daily work as a Data Engineer, while others are part of my preparation for
the [Databricks Apache Spark Developer Associate](https://www.databricks.com/learn/certification/apache-spark-developer-associate)
certification.

## 2. Currently covered topics

### Window Functions
  The [src/window_functions](src/window_functions) directory contains assignments and solutions
  that demonstrate how to use window functions in PySpark.  
  Window functions are a powerful tool for data analysis and manipulation in PySpark. They allow you to perform 
  calculations across a set of rows related to the current row.  
  Window functions are similar to aggregate functions, but they do not reduce the number of rows. Instead, they add a
  new column to the DataFrame. Window functions are used to calculate running totals, moving averages, and other
  calculations that require access to multiple rows in a DataFrame.  
  For more information check the 
  [Window Functions chapter](https://spark.apache.org/docs/latest/sql-ref-syntax-qry-select-window.html) of the 
  official documentation. 

## 3. Running the Solutions

### 3.1 Using Databricks Community Edition

All assignments and solutions are provided as Databricks Workbooks (files with .dbc extension).

### 3.2 Creating a local `conda` environment

### 3.3 Using Apache Spark Docker containers to run pyspark programs using spark-submit

Containerizing Apache Spark enhances the deployment and management of Spark clusters by improving resource utilization
and scalability. It also boosts security through isolation. Containerization ensures a consistent and portable runtime
environment for Spark applications, resulting in more efficient development and deployment processes.

#### 3.3.1 Prerequisites

* Docker installed and running on your system.
* Basic knowledge of Apache Spark and Docker containers.
* Familiarity with docker-compose files.

#### 3.3.2 The Docker container

Here is the [docker-compose](docker-compose.yaml) yaml file that we will use to run the Apache Spark cluster.  
This setup utilizes the `bitnami/spark` image. You can scale by adding more workers and adjust the `SPARK_WORKER_CORES`
and `SPARK_WORKER_MEMORY` environment variables to match your system’s specifications.

#### 3.3.4 Building and running the Docker containers

```bash
docker-compose up -d
```

#### 3.3.5 Verify that the containers are running

```bash
docker compose ps
```

#### 3.3.6 Verify that the containers contains the exercise files

```bash
docker-compose exec -it spark-master ls -al /opt/bitnami/spark/src/data

```

#### 3.3.7 Get the address where our spark master container is running

```bash
docker logs spark-coding-in-pyspark-spark-master-1
```

Find the line that says `Starting Spark master at spark:// ...` in our case it is `spark://172.19.0.2:7077`

```
24/07/22 15:38:57 INFO Utils: Successfully started service 'sparkMaster' on port 7077.
24/07/22 15:38:57 INFO Master: Starting Spark master at spark://172.19.0.2:7077
24/07/22 15:38:57 INFO Master: Running Spark version 3.5.1
24/07/22 15:38:57 INFO JettyUtils: Start Jetty 0.0.0.0:8080 for MasterUI

```

#### 3.3.8 Finding the service name of the master container

You will need the service name of the master container to run the spark-submit command.
You can get the service name by running the following command:

```bash
docker compose ps
```

In the services column you will see the service name of the master container, `spark-master` in this case.

```
NAME                                       IMAGE                  COMMAND                  SERVICE          CREATED          STATUS          PORTS
spark-coding-in-pyspark-spark-master-1     bitnami/spark:latest   "/opt/bitnami/script…"   spark-master     19 minutes ago   Up 19 minutes   0.0.0.0:7077->7077/tcp, 0.0.0.0:9090->8080/tcp
spark-coding-in-pyspark-spark-worker-1-1   bitnami/spark:latest   "/opt/bitnami/script…"   spark-worker-1   19 minutes ago   Up 19 minutes   
spark-coding-in-pyspark-spark-worker-2-1   bitnami/spark:latest   "/opt/bitnami/script…"   spark-worker-2   19 minutes ago   Up 19 minutes   

```

#### 3.3.9 Submitting a pyspark program using spark-submit command

```bash
docker-compose exec  spark-master spark-submit --master  spark://172.19.0.2:7077 src/window_functions/backups.py
```

## 4. Miscellaneous

### 4.1 Databricks Certified Associate Developer for Apache Spark

URL: [https://www.databricks.com/learn/certification/apache-spark-developer-associate](https://www.databricks.com/learn/certification/apache-spark-developer-associate)  
The Databricks Certified Associate Developer for Apache Spark certification exam assesses the understanding of the Spark
DataFrame API and the ability to apply the Spark DataFrame API to complete basic data manipulation tasks within a Spark
session. These tasks include selecting, renaming and manipulating columns; filtering, dropping, sorting, and aggregating
rows; handling missing data; combining, reading, writing and partitioning DataFrames with schemas; and working with UDFs
and Spark SQL functions. In addition, the exam will assess the basics of the Spark architecture like
execution/deployment modes, the execution hierarchy, fault tolerance, garbage collection, and broadcasting. Individuals
who pass this certification exam can be expected to complete basic Spark DataFrame tasks using Python or Scala.

#### 4.1.1 Exam Details

Key details about the certification exam are provided below.

#### 4.1.2 Minimally Qualified Candidate

The minimally qualified candidate should be able to:

* Understanding the basics of the Spark architecture, including Adaptive Query Execution
* Apply the Spark DataFrame API to complete individual data manipulation task, including:

* selecting, renaming and manipulating columns
* filtering, dropping, sorting, and aggregating rows
* joining, reading, writing and partitioning DataFrames
* working with UDFs and Spark SQL functions

While it will not be explicitly tested, the candidate must have a working knowledge of either Python or Scala. The exam
is available in both languages.

#### 4.1.3 Duration

Testers will have 120 minutes to complete the certification exam.

#### 4.1.4 Questions

There are 60 multiple-choice questions on the certification exam. The questions will be distributed by high-level topic
in the following way:

* Apache Spark Architecture Concepts – 17% (10/60)
* Apache Spark Architecture Applications – 11% (7/60)
* Apache Spark DataFrame API Applications – 72% (43/60)
