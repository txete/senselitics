import Floading
from pyspark.sql import SparkSession

def definirSesion(ip,puerto,bbdd,tbl):
    mensaje = 'Iniciando sesión spark...'
    spark = Floading.loading(lambda: SparkSession.builder \
        .appName("MongoDBIntegration") \
        .config("spark.mongodb.input.uri", f"mongodb://{ip}:{puerto}/{bbdd}.{tbl}") \
        .config("spark.mongodb.output.uri", f"mongodb://{ip}:{puerto}/{bbdd}.{tbl}") \
        .config("spark.jars.packages", "org.mongodb.spark:mongo-spark-connector_2.12:3.0.1") \
        .getOrCreate(),mensaje)
    return spark