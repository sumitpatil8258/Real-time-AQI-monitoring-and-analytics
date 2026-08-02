#!/bin/bash

docker exec -it spark-master \
/opt/spark/bin/spark-submit \
--master spark://spark-master:7077 \
--conf spark.jars.ivy=/tmp/.ivy2 \
--jars /opt/spark/jars-extra/mysql-connector-j-9.0.0.jar \
--packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.2 \
/opt/spark-apps/spark_streaming.py
