from pyspark.sql import SparkSession
from pyspark.streaming import StreamingContext
# from pyspark.sql import Row
# import json

def save_to_mongo(rdd):
    if not rdd.isEmpty():
        df = spark.createDataFrame(rdd)
        df.write.format("com.mongodb.spark.sql.DefaultSource").mode("append").save()

# Verificar si la ip ha cambiado en la maquina
spark = SparkSession.builder \
    .appName("TwitterStream") \
    .config("spark.mongodb.input.uri", "mongodb://192.168.23.32/reto2.tweets") \
    .config("spark.mongodb.output.uri", "mongodb://192.168.23.32/reto2.tweets") \
    .getOrCreate()

# nc -lk 9999 desde la BBDD para probar el streaming
ssc = StreamingContext(spark.sparkContext, 1)

lines = ssc.socketTextStream("192.168.23.32", 9999)
lines.pprint()

# parsed_lines = lines.map(lambda line: Row(**json.loads(line)))
# parsed_lines.foreachRDD(save_to_mongo)

ssc.start()
ssc.awaitTermination()
