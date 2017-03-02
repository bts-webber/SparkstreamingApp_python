#coding=utf-8
from __future__ import print_function
from Utils import get_ssc,utf8_decoder,ES
from Utils import fastAvro as Avro
from pyspark.streaming.kafka import KafkaUtils
import urlparse,urllib,json
from Utils import XssVector,SqlVector,Get_Sql_ValueFromFile,\
    Get_Xss_ValueFromFile,Merge_Sql_data,Merge_Xss_data,Train_Test,TestError
import numpy as np
from sklearn import svm
class WebDetectionJob(object):
    def __init__(self,conf):
        self.conf=conf
        self.app_conf = conf["App"]["WebDetectionJob"]
    #    self.schema=Avro.getSchema(self.app_conf["schema_file"])
    def startJob(self):
        print("Started Job......")
        sql_exploit_db = Get_Sql_ValueFromFile("sql_injection_exploit_db_long.csv")
        xss_xssed = Get_Xss_ValueFromFile("xss_xssed.csv")
        tianyan_web = Get_Xss_ValueFromFile("tianyan_web.csv")
        tianyan_web_long=Get_Sql_ValueFromFile("tianyan_web_long.csv")
        print("Get Train File Successful!")
        #合并数据，并生成label列表
        sql_data,sql_label=Merge_Sql_data(sql_exploit_db,tianyan_web_long)
        xss_data,xss_label=Merge_Xss_data(xss_xssed,tianyan_web)
        #生成训练数据集和测试数据集及其label列表
        sql_train_data,sql_train_label,sql_test_data,sql_test_label=Train_Test(sql_data,sql_label)
        xss_train_data,xss_train_label,xss_test_data,xss_test_label=Train_Test(xss_data,xss_label)
        sql_train_data=np.matrix(sql_train_data)
        sql_test_data=np.matrix(sql_test_data)
        xss_train_data=np.matrix(xss_train_data)
        xss_test_data=np.matrix(xss_test_data)
        #训练模型
        print("Trainning......")
        sql_model=svm.SVC(kernel="poly",C=10000)
        xss_model=svm.SVC(kernel="poly",C=10000)
        sql_model.fit(sql_train_data,sql_train_label)
        xss_model.fit(xss_train_data,xss_train_label)
        print("Model Trained！")
        #评价模型，使用测试数据检验模型
        sql_error_num,sql_error_num_sql,drop_num,sql_normal_error_num=TestError(sql_model,sql_test_data,sql_test_label)
        xss_error_num,drop_num,xss_error_num_xss,xss_normal_error_num=TestError(xss_model,xss_test_data,xss_test_label)
        print("sql注入模型预测错误率：", float(sql_error_num) / len(sql_test_data))
        print("SqlInjection检出率：",
              (sql_test_label.count("SqlInjection") - float(sql_error_num_sql)) / sql_test_label.count("SqlInjection"))
        print("sql注入误报率：", float(sql_normal_error_num) / sql_test_label.count("Normal"))
        print("xss模型预测错误率：", float(xss_error_num) / len(xss_test_data))
        print("Xss检出率：",
              (xss_test_label.count("Xss") - float(xss_error_num_xss)) / xss_test_label.count("Xss"))
        print("xss误报率：", float(xss_normal_error_num) / xss_test_label.count("Normal"))
        ssc=get_ssc(self.app_conf)
        # 将训练好的模型广播到excutor
        sql_model_b = ssc._sc.broadcast(sql_model)
        xss_model_b = ssc._sc.broadcast(xss_model)
        zookeeper=self.conf["global"]["zookeeper"]
        in_topic=self.app_conf["in_topic"]
        in_topic_partitions=self.app_conf["in_topic_partitions"]
        topic={in_topic:in_topic_partitions}
        #kvs=KafkaUtils.createStream(ssc,zookeeper,self.app_conf["app_name"],topic,keyDecoder=utf8_decoder,valueDecoder=lambda v :Avro.AvroToJson(v,self.schema))
        kvs = KafkaUtils.createStream(ssc, zookeeper, self.app_conf["app_name"], topic,keyDecoder=utf8_decoder,valueDecoder=lambda data:data)
        #获取外网访问内网的Get方式的数据
        #print("Get data from outer with 'Get' method......")
        print ("Get data with 'Request'")
        kvs_get=kvs.filter(self.filter_test)
        kvs_get.pprint(100)
        print("Data number is :",kvs_get.count().pprint())
        #计算数据的特征向量，并添加到vector字段
        print("Get Vector......")
        sql_vector_data=kvs_get.map(lambda data:self.add_vector_test(data,SqlVector))
        xss_vector_data=kvs_get.map(lambda data:self.add_vector_test(data,XssVector))
        sql_vector_data.pprint(100)
        xss_vector_data.pprint(100)
        print("Start Predict and Write result in Es......")
        sql_vector_data.foreachRDD(
           lambda rdd: rdd.foreachPartition(
               lambda iter:self.Predict(iter,sql_model_b)
           )
        )
        xss_vector_data.foreachRDD(
            lambda rdd: rdd.foreachPartition(
                lambda iter: self.Predict(iter, xss_model_b)
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
            url_parse = urlparse.urlparse(p)
            p = url_parse.params + url_parse.query + url_parse.fragment
        else:
            p=urllib.unquote(data["data"])
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
            if result=="SqlInjection":
                recordJson=ES.pop_null(record)
                recordJson.pop("vector")
                recordJson["alarm_type"]="SqlInjection"
                es.write_to_es(index_name, type_name, recordJson)
            if result=="Xss":
                recordJson = ES.pop_null(record)
                recordJson.pop("vector")
                recordJson["alarm_type"] = "Xss"
                es.write_to_es(index_name, type_name, recordJson)