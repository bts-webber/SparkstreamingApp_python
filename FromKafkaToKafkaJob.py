from __future__ import print_function
from get_ssc import get_ssc
from Serialization import pyAvro as Avro
from Serialization import utf8_decoder
from pyspark.streaming.kafka import KafkaUtils
from kafka import KafkaProducer
class FromKafkaToKafkaJob(object):
    def __init__(self,conf):
        self.conf=conf
        self.app_conf = conf["App"]["FromKafkaToKafkaJob"]
        self.schema=Avro.getSchema(self.app_conf["schema_file"])
    def startJob(self):
        print("Start Job!")
        ssc=get_ssc(self.app_conf)
        zookeeper=self.conf["global"]["zookeeper"]
        in_topic=self.app_conf["in_topic"]
        in_topic_partitions=self.app_conf["in_topic_partitions"]
        topic={in_topic:in_topic_partitions}
        kvs=KafkaUtils.createStream(ssc,zookeeper,self.app_conf["app_name"],topic,keyDecoder=utf8_decoder,valueDecoder=lambda v :Avro.AvroToJson(v,self.schema))
        kvs.foreachRDD(
           lambda rdd: rdd.foreachPartition(self.Send_to_kafka)
        )
        ssc.start()
        ssc.awaitTermination()
    def Send_to_kafka(self,iter):
        producer =KafkaProducer(bootstrap_servers=self.app_conf["kafka_producer"])
        out_topic=self.app_conf["out_topic"]
        for record in iter:
            recordJson=record[1]
            recordAvro=Avro.JsonToAvro(recordJson,self.schema)
            producer.send(out_topic,recordAvro)