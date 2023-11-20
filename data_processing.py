import pymongo
import json
import Floading
from pyspark.sql import SparkSession
from pyspark.streaming import StreamingContext
from pymongo import MongoClient

ip = '192.168.23.32'
port = 27017
bbdd = 'reto2'
table = 'tweets'

def main():
    test = True
    mensaje = 'Realizando un test de conexión a mongo...'
    test = Floading.loading(test_mongo,mensaje)    
    if not test:
        mensaje = 'Abriendo streaming...'
        ssc = Floading.loading(abrir_streaming,mensaje)
        lines = ssc.socketTextStream(ip, 9999)
        lines.pprint()
        lines.foreachRDD(save_rdd_to_mongo)
        ssc.start()
        mensaje = 'Escuchando y exportando datos...'
        Floading.loading(lambda: ssc.awaitTermination(),mensaje)
    else:
        print('Se ha cancelado el proceso')

def test_mongo():
    client = MongoClient(f'mongodb://{ip}:{port}/')
    db = client.admin
    error = False
    try:
        collections = db.list_collection_names()
        print(collections)
    except Exception as e:
        print(f"Error conectando a MongoDB: {e}")
        error = True    
    finally:
        client.close()
        return error  

def abrir_streaming(segundos=10):
    spark = SparkSession.builder \
        .appName("TwitterStream") \
        .getOrCreate()
    return StreamingContext(spark.sparkContext, segundos)

def save_rdd_to_mongo(rdd):
    if not rdd.isEmpty():
        local_data = rdd.collect()
        client = pymongo.MongoClient(f"mongodb://{ip}:{port}/")
        db = client[bbdd]
        collection = db[table]
        for tweet_data in local_data:
            try:
                tweet_json = json.loads(tweet_data)
                collection.insert_one(tweet_json)
            except json.JSONDecodeError as e:
                print("Error decoding JSON:", e)
        client.close()
