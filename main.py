from pyspark.sql import SparkSession
from pyspark.streaming import StreamingContext
import pymongo
import json

# Define a function to save each RDD to MongoDB using PyMongo
def save_rdd_to_mongo(rdd):
    if not rdd.isEmpty():
        # Collect the data to the driver
        local_data = rdd.collect()
        # Initialize PyMongo client
        client = pymongo.MongoClient("mongodb://192.168.23.32:27017/")
        # Get the database and collection
        db = client["reto2"]
        collection = db["tweets"]
        # Insert data into the collection
        for tweet_data in local_data:
            try:
                tweet_json = json.loads(tweet_data)
                collection.insert_one(tweet_json)
            except json.JSONDecodeError as e:
                print("Error decoding JSON:", e)
        # Close the connection
        client.close()

# Spark session initialization
spark = SparkSession.builder \
    .appName("TwitterStream") \
    .getOrCreate()

# Create Streaming Context with a 1-second batch interval
ssc = StreamingContext(spark.sparkContext, 1)

# Define the input sources by creating input DStreams
lines = ssc.socketTextStream("192.168.23.32", 9999)

# Print each batch of data
lines.pprint()

# Apply the save_rdd_to_mongo function to each RDD in the DStream
lines.foreachRDD(save_rdd_to_mongo)

# Start the computation
ssc.start()
# Wait for the computation to terminate
ssc.awaitTermination()
