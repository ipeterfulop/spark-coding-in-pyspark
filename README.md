# Using Apache Spark Docker containers to run pyspark programs using spark-submit

Containerizing Apache Spark enhances the deployment and management of Spark clusters by improving resource utilization
and scalability. It also boosts security through isolation. Containerization ensures a consistent and portable runtime
environment for Spark applications, resulting in more efficient development and deployment processes.

## Prerequisites

* Docker installed and running on your system.
* Basic knowledge of Apache Spark and Docker containers.
* Familiarity with docker-compose files.

## The Docker container

Here is the [docker-compose](docker-compose.yaml) yaml file that we will use to run the Apache Spark cluster.  
This setup utilizes the `bitnami/spark` image. You can scale by adding more workers and adjust the `SPARK_WORKER_CORES`
and `SPARK_WORKER_MEMORY` environment variables to match your system’s specifications.

## Building and running the Docker containers

```bash
docker-compose up -d
```

## Verify that the containers are running

```bash
docker compose ps
```

## Verify that the containers contains the exercise files

```bash
docker-compose exec -it spark-master ls -al /opt/bitnami/spark/src/data

```

## get the address where our spark master container is running

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

## Finding the service name of the master container

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

## Submitting a pyspark program using spark-submit command

```bash
docker-compose exec  spark-master spark-submit --master  spark://172.19.0.2:7077 src/window_functions/backups.py
```