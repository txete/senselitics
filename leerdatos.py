import pandas as pd
import numpy as np
import json
from pyspark.sql import SparkSession
from pandas import json_normalize

spark = SparkSession.builder \
    .appName("MongoDBIntegration") \
    .config("spark.mongodb.input.uri", "mongodb://192.168.23.32:27017/reto2.tweets") \
    .config("spark.mongodb.output.uri", "mongodb://192.168.23.32:27017/reto2.tweets") \
    .config("spark.jars.packages", "org.mongodb.spark:mongo-spark-connector_2.12:3.0.1") \
    .getOrCreate()

df = spark.read.format("mongo").load()

pandas_df = df.toPandas()

user_columns = [
    "id",
    "id_str",
    "name",
    "screen_name",
    "location",
    "url",
    "description",
    "protected",
    "followers_count",
    "friends_count",
    "listed_count",
    "created_at",
    "favourites_count",
    "utc_offset",
    "time_zone",
    "geo_enabled",
    "verified",
    "statuses_count",
    "lang",
    "contributors_enabled",
    "is_translator",
    "profile_background_color",
    "profile_background_image_url",
    "profile_background_image_url_https",
    "profile_background_tile",
    "profile_image_url",
    "profile_image_url_https",
    "profile_link_color",
    "profile_sidebar_border_color",
    "profile_sidebar_fill_color",
    "profile_text_color",
    "profile_use_background_image",
    "default_profile",
    "default_profile_image",
    "following",
    "follow_request_sent",
    "notifications"
]

pandas_df['user'] = pandas_df['user'].apply(lambda row: dict(zip(user_columns, row)))
users_df = json_normalize(pandas_df['user'])

################# HAY QUE REVISAR LAS STADISTICAS Y VER QUE NOS INTERESA #######################

# Por ejemplo, calcular la media del 'followers_count' y 'friends_count':
followers_mean = np.mean(users_df['followers_count'])
friends_mean = np.mean(users_df['friends_count'])

# Calcular la mediana de 'followers_count' y 'friends_count':
followers_median = np.median(users_df['followers_count'].dropna())  # Usar dropna() para evitar NaN
friends_median = np.median(users_df['friends_count'].dropna())

# Calcular la desviación estándar de 'followers_count' y 'friends_count':
followers_std = np.std(users_df['followers_count'])
friends_std = np.std(users_df['friends_count'])

# Calcular el mínimo y máximo de 'followers_count' y 'friends_count':
followers_min = np.min(users_df['followers_count'])
friends_min = np.min(users_df['friends_count'])

followers_max = np.max(users_df['followers_count'])
friends_max = np.max(users_df['friends_count'])

# Si quieres obtener un resumen estadístico completo de las columnas numéricas, puedes usar:
summary_stats = users_df.describe()

print(f"Media de seguidores: {followers_mean}")
print(f"Media de amigos: {friends_mean}")
print(f"Mediana de seguidores: {followers_median}")
print(f"Mediana de amigos: {friends_median}")
print(f"Desviación estándar de seguidores: {followers_std}")
print(f"Desviación estándar de amigos: {friends_std}")
print(f"Mínimo de seguidores: {followers_min}")
print(f"Mínimo de amigos: {friends_min}")
print(f"Máximo de seguidores: {followers_max}")
print(f"Máximo de amigos: {friends_max}")
print("\nResumen estadístico de usuarios:")
print(summary_stats)


# pandas_df['followers_count'] = users_df['followers_count']
# print(pandas_df['followers_count'].mean())
# print(pandas_df['followers_count'].std())
# print(pandas_df['followers_count'].corr(pandas_df['retweet_count']))

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


