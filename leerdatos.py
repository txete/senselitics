from pyspark.sql import SparkSession
import pandas as pd

spark = SparkSession.builder \
    .appName("MongoDBIntegration") \
    .config("spark.mongodb.input.uri", "mongodb://192.168.23.32:27017/reto2.tweets") \
    .config("spark.mongodb.output.uri", "mongodb://192.168.23.32:27017/reto2.tweets") \
    .config("spark.jars.packages", "org.mongodb.spark:mongo-spark-connector_2.12:3.0.1") \
    .getOrCreate()

df = spark.read.format("mongo").load()

pandas_df = df.toPandas()

print(pandas_df.head())

# import numpy as np

# # Calcular estadísticas
# mean = np.mean(pandas_df['columna_deseada'])
# std_dev = np.std(pandas_df['columna_deseada'])
# correlation = pandas_df.corr()

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


