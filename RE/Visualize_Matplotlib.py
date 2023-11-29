import matplotlib.pyplot as plt
import Data_From_Mongo
import Convert_To_Pandas
import pandas as pd

def main():

    db_name = 'DBreto2'
    collection_name = 'tweetsF'
    
    mentions, states, temporal, engagement = Data_From_Mongo.perform_aggregations(db_name, collection_name)
    df_mentions, df_states, df_temporal, df_engagement = Convert_To_Pandas.convertir_pandas(mentions, states, temporal, engagement)

    # Histograma para mentions_per_candidate
    df_mentions.plot(kind='bar', x='_id', y='count', title='Menciones por Candidato')
    plt.xlabel('Candidato')
    plt.ylabel('Número de Menciones')
    plt.show()

    # Diagrama de dispersión para average_engagement_per_candidate
    plt.scatter(df_engagement['avg_likes'], df_engagement['avg_retweets'])
    plt.title('Promedio de Likes vs. Retweets por Candidato')
    plt.xlabel('Promedio de Likes')
    plt.ylabel('Promedio de Retweets')
    plt.show()

    # Línea de tendencia para temporal_evolution
    df_temporal['date'] = pd.to_datetime(df_temporal['date'])
    df_temporal.sort_values('date', inplace=True)
    plt.plot(df_temporal['date'], df_temporal['count'])
    plt.title('Evolución Temporal de Menciones')
    plt.xlabel('Fecha')
    plt.ylabel('Número de Menciones')
    plt.show()
