import numpy as np
import Fsession, Fddbb, Fpandas
from pyspark.sql.types import StructType, StructField, StringType, LongType

ip = '192.168.23.32'
puerto = '27017'
bbdd = 'reto2'
tbl = 'candidatos'
candidatos = ['trump','biden']

def sacarEstadisticas(pandas_df):
    polaridad = pandas_df['Polaridad'].values
    retweet_counts = pandas_df['Retweet'].values

    mean_length = np.mean(retweet_counts)
    std_dev_length = np.std(retweet_counts)
    correlation_retweets = np.corrcoef(polaridad, retweet_counts)[0, 1]

    return {'mean_length':mean_length,'std_dev_length':std_dev_length,'correlation_retweets':correlation_retweets}

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
    for (index,candidato) in enumerate(candidatos):
        df_candidato = Fpandas.buscarPalabra(pandas_df,'tweet',candidato)
        df_usa = Fpandas.seleccionarUSA(df_candidato)
        if index == 0:
            index = -1
        sentimientos.append(Fpandas.convertirSentimientos(df_usa,candidato,index))
    stats = sacarEstadisticas(sentimientos)
    print(f"Media de longitud de tweets: {stats['mean_length']}")
    print(f"Desviación estándar de la longitud de tweets: {stats['std_dev_length']}")
    print(f"Correlación entre longitud de tweets y retweets: {stats['correlation_retweets']}")