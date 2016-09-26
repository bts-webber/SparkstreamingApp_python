from __future__ import print_function
from get_ssc import get_ssc
from Serialization import pyAvro as Avro
from Serialization import utf8_decoder
from pyspark.streaming.kafka import KafkaUtils
from es import ES
class FromKafkaToEsJob(object):
    def __init__(self,conf):
        self.conf=conf
        self.app_conf=conf["App"]["FromKafkaToEsJob"]
        self.schema = Avro.getSchema(self.app_conf["schema_file"])
    def startJob(self):
        print("Start Job!")
        ssc=get_ssc(self.app_conf)
        zookeeper=self.conf["global"]["zookeeper"]
        in_topic=self.app_conf["in_topic"]
        in_topic_partitions=self.app_conf["in_topic_partitions"]
        topic={in_topic:in_topic_partitions}
        kvs=KafkaUtils.createStream(ssc,zookeeper,self.app_conf["app_name"],topic,keyDecoder=utf8_decoder,valueDecoder=self.avro_decoder)
        kvs.foreachRDD(
           lambda rdd: rdd.foreachPartition(self.Send_to_Es)
        )
        ssc.start()
        ssc.awaitTermination()
    def Send_to_Es(self,iter):
        schema=Avro.getSchema(self.app_conf["schema_file"])
        es = ES(self.app_conf["elasticsearch"])
        index_name=self.app_conf["index_name"]
        type_name=self.app_conf["type_name"]
        for record in iter:
            recordJson = ES.pop_null(record[1])
            es.write_to_es(index_name, type_name, recordJson)
    def avro_decoder(self, s):
        """Decode the avro as Json"""
        if s is None:
            return None
        return Avro.AvroToJson(s, self.schema)