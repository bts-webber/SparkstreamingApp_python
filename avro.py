from fastavro import dump
from fastavro.reader import read_data
import pyavroc
import json,io
def utf8_decoder(s):
    """ Decode the unicode as UTF-8 """
    if s is None:
        return None
    return s.decode('utf-8','ignore')
class fastAvro(object):
    ''''Serialization and Deserialization record using fastavro.
    <https://github.com/tebeka/fastavro>'''
    def getSchema(self,path):
        with open(path) as schemaFile:
            schema = json.loads(schemaFile.read())
            return schema
    def AvroToJson(self,record, schema):
        buf = io.BytesIO()
        buf.write(record.encode("utf-8"))
        buf.seek(0)
        return read_data(buf, schema)

    def JsonToAvro(self,record, schema):
        buf = io.BytesIO()
        dump(buf, record, schema)
        avroRecord = buf.getvalue()
        return avroRecord.decode("utf-8")
class pyAvro(object):
    ''''Serialization and Deserialization record using pyavroc.
        <https://github.com/Byhiras/pyavroc'''
    def getSchema(self,path):
        with open(path) as schemaFile:
            return schemaFile.read()
    def AvroToJson(self,record, schema):
        deserializer = pyavroc.AvroDeserializer(schema)
        return deserializer.deserialize(record.encode("utf-8"))
    def JsonToAvro(self,record, schema):
        serializer = pyavroc.AvroSerializer(schema)
        avroRecoed = serializer.serialize(record)
        return avroRecoed
