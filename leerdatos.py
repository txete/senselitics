import pandas as pd
import numpy as np
import json
from pyspark.sql import SparkSession
from pandas import json_normalize
from pyspark.sql.functions import when, col, struct, lit, count
from pyspark.sql.types import StructType, StructField, StringType, ArrayType, DoubleType, IntegerType

spark = SparkSession.builder \
    .appName("MongoDBIntegration") \
    .config("spark.mongodb.input.uri", "mongodb://192.168.23.32:27017/reto2.tweets") \
    .config("spark.mongodb.output.uri", "mongodb://192.168.23.32:27017/reto2.tweets") \
    .config("spark.jars.packages", "org.mongodb.spark:mongo-spark-connector_2.12:3.0.1") \
    .getOrCreate()

from pyspark.sql.types import StructType, StructField, StringType, LongType, BooleanType, ArrayType, DoubleType, MapType, NullType

user_schema = StructType([
    StructField("id", LongType(), nullable=False),
    StructField("id_str", StringType(), nullable=False),
    StructField("name", StringType(), nullable=True),
    StructField("screen_name", StringType(), nullable=True),
    StructField("location", StringType(), nullable=True),
    StructField("url", StringType(), nullable=True),
    StructField("description", StringType(), nullable=True),
    StructField("protected", BooleanType(), nullable=True),
    StructField("followers_count", LongType(), nullable=True),
    StructField("friends_count", LongType(), nullable=True),
    StructField("listed_count", LongType(), nullable=True),
    StructField("created_at", StringType(), nullable=True),
    StructField("favourites_count", LongType(), nullable=True),
    StructField("utc_offset", LongType(), nullable=True),
    StructField("time_zone", StringType(), nullable=True),
    StructField("geo_enabled", BooleanType(), nullable=True),
    StructField("verified", BooleanType(), nullable=True),
    StructField("statuses_count", LongType(), nullable=True),
    StructField("lang", StringType(), nullable=True),
    StructField("contributors_enabled", BooleanType(), nullable=True),
    StructField("is_translator", BooleanType(), nullable=True),
    StructField("profile_background_color", StringType(), nullable=True),
    StructField("profile_background_image_url", StringType(), nullable=True),
    StructField("profile_background_image_url_https", StringType(), nullable=True),
    StructField("profile_background_tile", BooleanType(), nullable=True),
    StructField("profile_image_url", StringType(), nullable=True),
    StructField("profile_image_url_https", StringType(), nullable=True),
    StructField("profile_banner_url", StringType(), nullable=True),
    StructField("profile_link_color", StringType(), nullable=True),
    StructField("profile_sidebar_border_color", StringType(), nullable=True),
    StructField("profile_sidebar_fill_color", StringType(), nullable=True),
    StructField("profile_text_color", StringType(), nullable=True),
    StructField("profile_use_background_image", BooleanType(), nullable=True),
    StructField("default_profile", BooleanType(), nullable=True),
    StructField("default_profile_image", BooleanType(), nullable=True),
    StructField("following", NullType(), nullable=True),
    StructField("follow_request_sent", NullType(), nullable=True),
    StructField("notifications", NullType(), nullable=True)
])

geo_schema = StructType([
    StructField("type", StringType(), nullable=True),
    StructField("coordinates", ArrayType(DoubleType(), containsNull=False), nullable=True)
])

coordinates_schema = StructType([
    StructField("type", StringType(), nullable=True),
    StructField("coordinates", ArrayType(DoubleType(), containsNull=False), nullable=True)
])

place_bounding_box_schema = StructType([
    StructField("type", StringType(), nullable=True),
    StructField("coordinates", ArrayType(ArrayType(ArrayType(DoubleType(), containsNull=False), containsNull=False), containsNull=True), nullable=True)
])

place_schema = StructType([
    StructField("id", StringType(), nullable=True),
    StructField("url", StringType(), nullable=True),
    StructField("place_type", StringType(), nullable=True),
    StructField("name", StringType(), nullable=True),
    StructField("full_name", StringType(), nullable=True),
    StructField("country_code", StringType(), nullable=True),
    StructField("country", StringType(), nullable=True),
    StructField("bounding_box", place_bounding_box_schema, nullable=True),
    StructField("attributes", MapType(StringType(), StringType()), nullable=True)
])

entities_user_mentions_schema = StructType([
    StructField("screen_name", StringType(), nullable=True),
    StructField("name", StringType(), nullable=True),
    StructField("id", LongType(), nullable=True),
    StructField("id_str", StringType(), nullable=True),
    StructField("indices", ArrayType(LongType(), containsNull=False), nullable=True)
])

entities_schema = StructType([
    StructField("hashtags", ArrayType(MapType(StringType(), StringType()), containsNull=True), nullable=True),
    StructField("urls", ArrayType(MapType(StringType(), StringType()), containsNull=True), nullable=True),
    StructField("user_mentions", ArrayType(entities_user_mentions_schema, containsNull=True), nullable=True)
])

retweeted_status_schema = StructType([
    StructField("created_at", StringType(), True),
    StructField("id", StringType(), True),  # or LongType()
    StructField("id_str", StringType(), True),
    StructField("text", StringType(), True),
    StructField("source", StringType(), True),
    StructField("truncated", BooleanType(), True),
    StructField("in_reply_to_status_id", StringType(), True),
    StructField("in_reply_to_status_id_str", StringType(), True),
    StructField("in_reply_to_user_id", StringType(), True),
    StructField("in_reply_to_user_id_str", StringType(), True),
    StructField("in_reply_to_screen_name", StringType(), True),
    StructField("user", user_schema, nullable=True),
    StructField("retweet_count", IntegerType(), True),
    StructField("favorite_count", IntegerType(), True),
    StructField("entities", entities_schema, nullable=True),
    StructField("favorited", BooleanType(), True),
    StructField("retweeted", BooleanType(), True),
    StructField("possibly_sensitive", BooleanType(), True),
    StructField("lang", StringType(), True)
])

# Main schema
tweet_schema = StructType([
    StructField("created_at", StringType(), nullable=True),
    StructField("id", LongType(), nullable=False),
    StructField("id_str", StringType(), nullable=False),
    StructField("text", StringType(), nullable=True),
    StructField("source", StringType(), nullable=True),
    StructField("truncated", BooleanType(), nullable=True),
    StructField("in_reply_to_status_id", LongType(), nullable=True),
    StructField("in_reply_to_status_id_str", StringType(), nullable=True),
    StructField("in_reply_to_user_id", LongType(), nullable=True),
    StructField("in_reply_to_user_id_str", StringType(), nullable=True),
    StructField("in_reply_to_screen_name", StringType(), nullable=True),
    StructField("user", user_schema, nullable=True),
    StructField("geo", geo_schema, nullable=True),
    StructField("coordinates", coordinates_schema, nullable=True),
    StructField("place", place_schema, nullable=True),
    StructField("retweeted_status", retweeted_status_schema, nullable=True),
    StructField("contributors", NullType(), nullable=True),
    StructField("retweet_count", LongType(), nullable=True),
    StructField("entities", entities_schema, nullable=True),
    StructField("favorited", BooleanType(), nullable=True),
    StructField("retweeted", BooleanType(), nullable=True),
    StructField("lang", StringType(), nullable=True)
])

pipeline = "[{'$limit': 100}]"

df = spark.read.format("mongo").option("pipeline", pipeline).schema(tweet_schema).load()

df.createOrReplaceTempView("tweets")

df_grouped = spark.sql("""
    SELECT parent.id, any_value(parent.text) AS text, COUNT(DISTINCT child.retweeted_status.id) AS retweets 
    FROM tweets AS parent 
    LEFT JOIN tweets AS child ON parent.id = child.retweeted_status.id 
    GROUP BY parent.id
""")

pandas_df = df_grouped.toPandas()

pandas_df['tweet_length'] = pandas_df['text'].apply(len)

tweet_lengths = pandas_df['tweet_length'].values
retweet_counts = pandas_df['retweets'].values

mean_length = np.mean(tweet_lengths)
std_dev_length = np.std(tweet_lengths)
correlation_retweets = np.corrcoef(tweet_lengths, retweet_counts)[0, 1]

# Media de longitud de tweets
# La media de longitud de los tweets, hay tweets mas largos y mas cortos
print(f"Media de longitud de tweets: {mean_length}")

# Desviación estándar de la longitud de tweets
# Esto significa que la desviación es muy grande, cuanto mas alto el numero significa que mas se alejan de la media
print(f"Desviación estándar de la longitud de tweets: {std_dev_length}")

# Correlación entre longitud de tweets y retweets
# Una correlacion negativa significa que no existe relacion entre texto y retweets
print(f"Correlación entre longitud de tweets y retweets: {correlation_retweets}")

# import pyarrow as pa
# import pyarrow.parquet as pq
# from hdfs import InsecureClient

# # Convertir Pandas DataFrame a PyArrow Table
# table = pa.Table.from_pandas(pandas_df)

# # Definir el cliente HDFS
# client = InsecureClient('http://namenode:50070', user='hdfs')

# # Escribir a HDFS como Parquet
# with client.write('/path/in/hdfs/results.parquet', overwrite=True) as writer:
#     pq.write_table(table, writer)

# from pyspark.sql.functions import mean, stddev

# # Calcular estadísticas en Spark
# mean_value = df.select(mean(df['columna_deseada'])).collect()[0][0]
# stddev_value = df.select(stddev(df['columna_deseada'])).collect()[0][0]

# from pyspark.sql.functions import corr

# # Calcular la correlación entre dos columnas
# correlation_value = df.stat.corr('columna1', 'columna2')


