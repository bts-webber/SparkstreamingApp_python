from get_ssc import get_ssc
from avro import pyAvro as Avro
from avro import utf8_decoder
from pyspark.streaming.kafka import KafkaUtils
from kafka import KafkaProducer
class FromKafkaToKafkaJob(object):
    def __init__(self,conf):
        self.conf=conf
        self.app_conf = conf["App"]["FromKafkaToKafkaJob"]
    def startJob(self):
        ssc=get_ssc(self.app_conf)
        zookeeper=self.conf["global"]["zookeeper"]
        in_topic=self.app_conf["in_topic"]
        in_topic_partitions=self.app_conf["in_topic_partitions"]
        topic={in_topic:in_topic_partitions}
        kvs=KafkaUtils.createStream(ssc,zookeeper,self.app_conf["app_name"],topic,keyDecoder=utf8_decoder,valueDecoder=utf8_decoder)
        kvs.foreachRDD(
           lambda rdd: rdd.foreachPartition(self.Send_to_kafka)
        )
        ssc.start()
        ssc.awaitTermination()
    def Send_to_kafka(self,iter):
        producer =KafkaProducer(bootstrap_servers=self.app_conf["kafka_producer"])
        avro_instance=Avro()
        schema=avro_instance.getSchema(self.app_conf["schema_file"])
        out_topic=self.app_conf["out_topic"]
        for record in iter:
            recordJson=avro_instance.AvroToJson(record[1],schema)
            recordAvro=avro_instance.JsonToAvro(recordJson,schema)
            producer.send(out_topic,recordAvro)