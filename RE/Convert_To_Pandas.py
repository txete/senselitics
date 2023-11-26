from pyspark.sql import SparkSession
import Data_From_Mongo
import pandas as pd

### Funciones para convertir a Pandas
def convertir_pandas(mentions, states, temporal, engagement):
    df_mentions = pd.DataFrame(mentions)
    df_states = pd.DataFrame(states)
    df_temporal = pd.DataFrame(temporal)
    df_engagement = pd.DataFrame(engagement)
    return df_mentions, df_states, df_temporal, df_engagement

def main():
    db_name = 'reto2'
    collection_name = 'tweets_long'

    mentions, states, temporal, engagement = Data_From_Mongo.perform_aggregations(db_name, collection_name)
    df_mentions, df_states, df_temporal, df_engagement = convertir_pandas(mentions, states, temporal, engagement)
    
    print(df_mentions.head())
    print(df_states.head())
    print(df_temporal.head())
    print(df_engagement.head())
