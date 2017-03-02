#coding=utf-8
from __future__ import print_function
from Utils import get_ssc,utf8_decoder,ES
from pyspark.streaming.kafka import KafkaUtils
import urlparse,urllib,json,pickle
from Utils import XssVector,SqlVector,Get_Sql_ValueFromFile,\
    Get_Xss_ValueFromFile,Merge_Sql_data,Merge_Xss_data,Train_Test,TestError
import numpy as np
from sklearn import svm
class SqliDetectionJob(object):
    def __init__(self,conf):
        self.conf=conf
        self.app_conf = conf["App"]["SqliDetectionJob"]
    #    self.schema=Avro.getSchema(self.app_conf["schema_file"])
    def startJob(self):
        with open("ali_sql_model.txt") as file:
            sqli_model=pickle.load(file)
        print("获取模型成功！")
        ssc=get_ssc(self.app_conf)
        sqli_model_b = ssc._sc.broadcast(sqli_model)
        zookeeper=self.conf["global"]["zookeeper"]
        in_topic=self.app_conf["in_topic"]
        in_topic_partitions=self.app_conf["in_topic_partitions"]
        topic={in_topic:in_topic_partitions}
        #kvs=KafkaUtils.createStream(ssc,zookeeper,self.app_conf["app_name"],topic,keyDecoder=utf8_decoder,valueDecoder=lambda v :Avro.AvroToJson(v,self.schema))
        kvs = KafkaUtils.createStream(ssc, zookeeper, self.app_conf["app_name"], topic,keyDecoder=utf8_decoder,valueDecoder=lambda data:data)
        #获取外网访问内网的Get方式的数据
        print ("Get data with 'Request'")
        kvs_get=kvs.filter(self.filter_test)
        kvs_get.pprint(100)
        print("Data number is :",kvs_get.count().pprint())
        #计算数据的特征向量，并添加到vector字段
        print("Get Vector......")
        sql_vector_data=kvs_get.map(lambda data:self.add_vector_test(data,SqlVector))
        sql_vector_data.pprint(100)
        print("Start Predict and Write result in Es......")
        sql_vector_data.foreachRDD(
           lambda rdd: rdd.foreachPartition(
               lambda iter:self.Predict(iter,sqli_model_b)
           )
        )
        ssc.start()
        ssc.awaitTermination()
    def filter_get(self,data):
        if data[1]["http_method"]=="GET":
            if data[1]["src_ip"][0:3] in ("10.","11."):
                return False
            uri=data[1]["cs_uri"]
            url_parse=urlparse.urlparse(uri)
            payload=url_parse.params+url_parse.query+url_parse.fragment
            if len(payload.strip())>60:
                return True
            else:return False
        else :return False
    def filter_test(self,data):
        #return Request log
        data=json.loads(data[1])
        if data["http_type"]=="Request":
            if data["method"]=="GET":
                p=urllib.unquote(data["uri"])
                p=urllib.unquote(p)
                url_parse=urlparse.urlparse(p)
                p = url_parse.params + url_parse.query + url_parse.fragment
                p=p.strip()
            elif data["method"]=="POST":
                p=urllib.unquote(data["data"])
                p=p.strip()
            else :return False
            if len(p)==0:
                return False
            else :return True
        else :return False
    def add_vector(self,data,vector_f):
        data=data[1]
        p=data["cs_uri"]
        url_parse=urlparse.urlparse(p)
        p=url_parse.params+url_parse.query+url_parse.fragment
        p=urllib.unquote(p)
        p=p.encode("raw_unicode_escape")
        data["vector"]=vector_f(p)
        return data
    def add_vector_test(self,data,vector_f):
        data = json.loads(data[1])
        if data["method"]=="GET":
            p=urllib.unquote(data["uri"])
            p=urllib.unquote(p)
            url_parse = urlparse.urlparse(p)
            p = url_parse.params + url_parse.query + url_parse.fragment
        else:
            p=urllib.unquote(data["data"])
            p = urllib.unquote(p)
        p=p.encode("raw_unicode_escape")
        data["vector"]=vector_f(p)
        return data
    def Predict(self, iter,model_pickle):
        es = ES(self.app_conf["elasticsearch"])
        index_name = self.app_conf["index_name"]
        type_name = self.app_conf["type_name"]
        model=model_pickle.value
        for record in iter:
            result=model.predict(np.array(record["vector"]))
            if result=="1":
                recordJson=ES.pop_null(record)
                recordJson.pop("vector")
                recordJson["alarm_type"]="SqlInjection"
                es.write_to_es(index_name, type_name, recordJson)