from fastavro import dump
from fastavro.reader import read_data
#import pyavroc
import json,io,avro.schema,avro.io
def utf8_decoder(s):
    """ Decode the unicode as UTF-8 """
    if s is None:
        return None
    return s.decode('utf-8','ignore')
class apacheAvro(object):
    ''''Serialization and Deserialization record using apache avro.'''
    @staticmethod
    def getSchema(path):
        with open(path) as schemaFile:
            schema = avro.schema.parse(schemaFile.read())
            return schema
    @staticmethod
    def AvroToJson(record, schema):
        bytes_reader = io.BytesIO(record)
        decoder = avro.io.BinaryDecoder(bytes_reader)
        reader = avro.io.DatumReader(schema)
        jsonRecord = reader.read(decoder)
        return jsonRecord
    @staticmethod
    def JsonToAvro(record, schema):
        writer = avro.io.DatumWriter(schema)
        bytes_writer = io.BytesIO()
        encoder = avro.io.BinaryEncoder(bytes_writer)
        writer.write(record, encoder)
        avroRecord = bytes_writer.getvalue()
        return avroRecord
class fastAvro(object):
    ''''Serialization and Deserialization record using fastavro.
    <https://github.com/tebeka/fastavro>'''
    @staticmethod
    def getSchema(path):
        with open(path) as schemaFile:
            schema = json.loads(schemaFile.read())
            return schema
    @staticmethod
    def AvroToJson(record, schema):
        buf = io.BytesIO()
        buf.write(record)
        buf.seek(0)
        return read_data(buf, schema)
    @staticmethod
    def JsonToAvro(record, schema):
        buf = io.BytesIO()
        dump(buf, record, schema)
        avroRecord = buf.getvalue()
        return avroRecord
#class pyAvro(object):
#    ''''Serialization and Deserialization record using pyavroc.
#       <https://github.com/Byhiras/pyavroc'''
 #   @staticmethod
 #   def getSchema(path):
 #       with open(path) as schemaFile:
 #           return schemaFile.read()
 #   @staticmethod
 #   def AvroToJson(record, schema):
 #       deserializer = pyavroc.AvroDeserializer(schema)
 #       return deserializer.deserialize(record)
 #   @staticmethod
 #   def JsonToAvro(record, schema):
 #       serializer = pyavroc.AvroSerializer(schema)
 #       avroRecoed = serializer.serialize(record)
 #       return avroRecoed
