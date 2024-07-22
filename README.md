# Using Apache Spark Docker containers to run pyspark programs using spark-submit

```bash
docker cp src/. spark-coding-in-pyspark-spark-master-1:/opt/bitnami/spark/src

```

```bash
docker exec -it spark-coding-in-pyspark-spark-master-1 ls /opt/bitnami/spark/src
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
docker-compose exec  spark-master spark-submit --master  spark://172.19.0.2:7077 src/joins/movies_with_no_genres.py
```

```bash
```