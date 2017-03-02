from pyspark import SparkContext,SparkConf
from pyspark.streaming import StreamingContext
def get_ssc(AppConfig):
    conf=SparkConf().setAppName(AppConfig["app_name"])
    sc = SparkContext(conf=conf)
    ssc = StreamingContext(sc, 20)
    return ssc