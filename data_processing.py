import pymongo
import json
import Floading, Fgenerales
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

def main_napa():
    test = True
    mensaje = 'Realizando un test de conexión a mongo...'
    test = Floading.loading(test_mongo,mensaje)    
    if not test:
        Floading.loading(save_rdd_to_mongo_napa,'cargando ñapa JSON...')
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
        
        for json_str in local_data:
            try:
                data = json.loads(json_str)
                tweet_data = {key: data[key] for key in ['created_at', 'tweet_id', 'tweet', 'likes', 'retweet_count', 'source'] if key in data}
                user_data = {key: data[key] for key in ['user_id', 'user_name', 'user_screen_name', 'user_description', 'user_join_date', 'user_followers_count'] if key in data}
                location_data = {key: data[key] for key in ['user_location', 'lat', 'long', 'city', 'country', 'continent', 'state', 'state_code'] if key in data}                
                
                location_id = db['ubicaciones'].insert_one(location_data).inserted_id
                user_id = db['usuarios'].insert_one(user_data).inserted_id
                tweet_data['user_id'] = user_id
                tweet_data['location_id'] = location_id
                db['tweetsG'].insert_one(tweet_data)                
            except json.JSONDecodeError as e:
                print("Error decoding JSON:", e)
        client.close()

def save_rdd_to_mongo_napa():
        json_data = Fgenerales.importarFicheroJSON('./biden.csv')
        # json_data = Fgenerales.importarFicheroJSON('./trump.csv')
        local_data = json.loads(json_data)        
        client = pymongo.MongoClient(f"mongodb://{ip}:{port}/")
        db = client[bbdd]
        collection = db[table]
        
        for data in local_data:
            try:
                tweet_data = {key: data[key] for key in ['created_at', 'tweet_id', 'tweet', 'likes', 'retweet_count', 'source'] if key in data}
                user_data = {key: data[key] for key in ['user_id', 'user_name', 'user_screen_name', 'user_description', 'user_join_date', 'user_followers_count'] if key in data}
                location_data = {key: data[key] for key in ['user_location', 'lat', 'long', 'city', 'country', 'continent', 'state', 'state_code'] if key in data}                
                                
                existing_location = db['ubicaciones'].find_one({'user_location': location_data['user_location']})
                if existing_location:
                    location_id = existing_location['_id']
                else:
                    location_id = db['ubicaciones'].insert_one(location_data).inserted_id
                existing_user = db['usuarios'].find_one({'user_name': user_data['user_name']})
                if existing_user:
                    user_id = existing_user['_id']
                else:
                    user_id = db['usuarios'].insert_one(user_data).inserted_id
                tweet_data['user_id'] = user_id
                tweet_data['location_id'] = location_id
                db['tweetsG'].insert_one(tweet_data)

                # location_id = db['ubicaciones'].insert_one(location_data).inserted_id
                # user_id = db['usuarios'].insert_one(user_data).inserted_id
                # tweet_data['user_id'] = user_id
                # tweet_data['location_id'] = location_id
                # db['tweetsG'].insert_one(tweet_data)
            except json.JSONDecodeError as e:
                print("Error decoding JSON:", e)
        client.close()