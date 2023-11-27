import random
import csv
import json
from pyspark.sql import SparkSession
from pyspark.streaming import StreamingContext

### Funciones para crear la sesión de Spark
def create_spark_session(app_name):
    return SparkSession.builder.config("spark.driver.host", "localhost").appName(app_name).config("spark.driver.memory", "10g").getOrCreate()

### Funciones para crear el contexto de streaming
def create_streaming_context(spark_context, batch_duration):
    return StreamingContext(spark_context, batch_duration)

### Funciones para crear el DStream
def create_dstream(streaming_context, ip, port):
    return streaming_context.socketTextStream(ip, port)

### Funciones para iniciar el streaming
def start_streaming(streaming_context):
    streaming_context.start()
    streaming_context.awaitTermination()

def load_tweets_csv(file_path):
    with open(file_path, 'r', encoding='utf-8') as file:
        return list(csv.reader(file))
    
def load_tweets_json(file_path):
    with open(file_path, 'r', encoding='utf-8') as file:
        return json.load(file)
    
def get_random_tweet(tweets):
    return random.choice(tweets)

def simulate_tweets_rdd(ssc, tweets, num_batches, num_tweets_per_batch):
    rdd_queue = []
    for _ in range(num_batches):
        batch = [get_random_tweet(tweets) for _ in range(num_tweets_per_batch)]
        rdd_queue.append(ssc.sparkContext.parallelize(batch))
    return ssc.queueStream(rdd_queue)

def main():
    app_name = "TweetsStreaming"
    batch_duration = 10
    ip_address = '192.168.1.167'
    port = 9999    

    sc = create_spark_session(app_name)
    ssc = create_streaming_context(sc.sparkContext, batch_duration)

    # Stream real
    # dstream = create_dstream(ssc, ip_address, port)

    # Stream simulado
    num_batches = 5
    num_tweets_per_batch = 10
    file_path = 'trump.csv'

    tweets = load_tweets_csv(file_path)
    dstream = simulate_tweets_rdd(ssc, tweets, num_batches, num_tweets_per_batch)

    dstream.foreachRDD(lambda rdd: rdd.foreach(lambda tweet: print(','.join(tweet).encode('utf-8'))))

    start_streaming(ssc)
