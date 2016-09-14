from elasticsearch import Elasticsearch
class ES(object):
    def __init__(self,es_conf):
        '''init Elastisearch connection'''
        self.conf=es_conf
        self.es_connect=Elasticsearch(hosts=es_conf["connect"]["hosts"],
                                      maxsize=es_conf["connect"]["maxsize"])
    def create_indices(self,index_name):
        '''create a new index'''
        body = {
            "settings": {
                "index": self.conf["index"]
            }
        }
        self.es_connect.indices.create(index_name,body)
    def write_to_es(self,index_name,type_name,record):
        '''create a new document '''
        body=record
        self.es_connect.create(index_name,type_name,body)
    def exits_index(self, index_name):
        return self.es_connect.indices.exists(index_name)
    @staticmethod
    def pop_null(record):
        for key in record.keys():
            if not record[key]:
                record.pop(key)
        return record