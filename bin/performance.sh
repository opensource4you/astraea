#!/bin/bash

base_dir=$(dirname $0)
KAFKA_VERSION=2.8.0
classpath="${base_dir}/classpath.txt"

# TODO: 試著不要下載kafka，或許可以使用gradle來執行
# 檢查kafka是否存在，若不存在則從網路上下載
if [[ ! -d "kafka_2.13-2.8.0" ]]; then
    echo "No kafka found. Downloading it."
    wget https://archive.apache.org/dist/kafka/${KAFKA_VERSION}/kafka_2.13-${KAFKA_VERSION}.tgz
    tar -zxvf kafka_2.13-${KAFKA_VERSION}.tgz
    echo "Remove kafka's zip file"
    rm kafka_2.13-${KAFKA_VERSION}.tgz
fi

# 檢查有否參數
if [[ $# -lt 1 ]]; then
    echo "Nothing to run."
    exit 2
fi

# 製造java所需的路徑
echo "Creating java classpath file..."
echo ".:app/build/libs/app-0.0.1-SNAPSHOT.jar:"`ls kafka_2.13-2.8.0/libs/*.jar` | tr " " : >> ${classpath}

# 執行效能評測
java -cp "@$classpath" org.astraea.performance.Performance "$@"

# 刪除中間檔案
echo "Removing java classpath file..."
rm $classpath
