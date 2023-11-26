import pymongo
# importamos las funciones de streaming
import Stream_Processor

### Funcion para guardar los tweets en MongoDB
def save_to_mongodb(rdd):
    # parametros de conexion
    ip = '192.168.1.167'
    port = 27017

    if not rdd.isEmpty():
        client = pymongo.MongoClient(f"mongodb://{ip}:{port}/")
        db = client["mydatabase"]
        
        # colecciones para guardar los tweets
        collection_short = db["tweets_short"]
        collection_long = db["tweets_long"]

        for tweet in rdd.collect():
            tweet_data = { "tweet": ','.join(tweet) }
            
            # Suponiendo que 140 caracteres es el límite para un tweet corto
            if len(tweet_data["tweet"]) < 140:  
                collection_short.insert_one(tweet_data)
            else:
                collection_long.insert_one(tweet_data)

        client.close()

### Funcion para probar la conexion a MongoDB
def test_mongo():
    ip = '192.168.1.167'
    port = 27017
    client = pymongo.MongoClient(f'mongodb://{ip}:{port}/')
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
    
def main():
    # test_mongo()

    app_name = "TweetsStreaming"
    batch_duration = 10
    ip_address = '192.168.1.167'
    port = 9999    

    sc = Stream_Processor.create_spark_session(app_name)
    ssc = Stream_Processor.create_streaming_context(sc.sparkContext, batch_duration)

    # Stream real
    # dstream = create_dstream(ssc, ip_address, port)

    # Stream simulado
    num_batches = 5
    num_tweets_per_batch = 10
    file_path = 'trump.csv'

    tweets = Stream_Processor.load_tweets_csv(file_path)
    dstream = Stream_Processor.simulate_tweets_rdd(ssc, tweets, num_batches, num_tweets_per_batch)

    dstream.foreachRDD(save_to_mongodb)

    Stream_Processor.start_streaming(ssc)
