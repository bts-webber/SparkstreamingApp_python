#!/bin/bash
LIBRARY_PATH="/data/sdb1/python/lib"
SCHEMA_PATH="/data/sdb1/python/schema"
PYTHON_LIB_PATH="/data/sdb1/python/pylib"


spark-submit \
--master yarn-client \
--deploy-mode client \
--name sparkstreamingApp_python \
--driver-memory 1g \
--num-executors 10 \
--executor-cores 6 \
--executor-memory 3g \
--py-files ${PYTHON_LIB_PATH}/kafka.zip,\
${PYTHON_LIB_PATH}/fastavro.zip,${PYTHON_LIB_PATH}/elasticsearch.zip,${PYTHON_LIB_PATH}/avro.zip,\
${PYTHON_LIB_PATH}/urllib3.zip,./Serialization.py,./es.py,./FromKafkaToEsJob.py,./FromKafkaToKafkaJob.py,./get_ssc.py \
--files ${SCHEMA_PATH}/netflow_schema.json,${SCHEMA_PATH}/netflow_enrich_schema.json,./AppConfig.json \
--jars \
${LIBRARY_PATH}/zkclient-0.3.jar,${LIBRARY_PATH}/spark-streaming-kafka_2.10-1.4.0.jar,\
${LIBRARY_PATH}/metrics-core-2.2.0.jar,\
${LIBRARY_PATH}/kafka_2.10-0.8.1.2.2.4.2-2.jar,${LIBRARY_PATH}/kafka-clients-0.8.2.1.jar,\
${LIBRARY_PATH}/kafka-spark-consumer-1.0.4.jar,${LIBRARY_PATH}/kafka_2.10-0.8.2.1.jar \
./main.py