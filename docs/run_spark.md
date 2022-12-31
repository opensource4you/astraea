#TODO astraea#1365 撰寫start_spark.sh中文文件

```bash
# Run Spark Master
SPARK_PORT=8080 SPARK_UI_PORT=7077 docker/start_spark.sh \
folder=/home/kafka/spark2kafkaTest/ImportcsvTest/source:/home/kafka/spark2kafkaTest/ImportcsvTest/source
```

```bash
# Run Spark Worker
SPARK_PORT=8081 SPARK_UI_PORT=7078 docker/start_spark.sh \
master=spark://192.168.103.189:8080 \
folder=/home/kafka/spark2kafkaTest/ImportcsvTest/source:/home/kafka/spark2kafkaTest/ImportcsvTest/source
```