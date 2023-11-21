import Fhdfs, Fsession, Fddbb
from pyspark.sql.types import StructType, StructField, StringType, LongType

ip = '192.168.23.32'
puerto = '27017'
bbdd = 'reto2'
tbl = 'candidatos'

def main():
    sentimientos = []
    spark = Fsession.definirSesion(ip,puerto,bbdd,tbl)
    schema = StructType([
        StructField("_id", StringType(), nullable=False),
        StructField("tweet", StringType(), nullable=True),
        StructField("state_code", StringType(), nullable=True),
        StructField("state", StringType(), nullable=True),
        StructField("country", StringType(), nullable=True),
        StructField("retweet_count", LongType(), nullable=True),
    ])
    pipeline = [
        {'$project': {'_id': 1, 'tweet': 1, 'state_code': 1, 'state': 1, 'country': 1, 'retweet_count': 1}},
    ]
    df = Fddbb.extraerDatos(spark,schema,pipeline)
    pandas_df = df.toPandas() 
    Fhdfs.hdfsSubir(pandas_df)