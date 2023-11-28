import matplotlib.pyplot as plt
import Data_From_Mongo
import Convert_To_Pandas

def main():

    db_name = 'DBreto2'
    collection_name = 'tweetsF'
    
    mentions, states, temporal, engagement = Data_From_Mongo.perform_aggregations(db_name, collection_name)
    df_mentions, df_states, df_temporal, df_engagement = Convert_To_Pandas.convertir_pandas(mentions, states, temporal, engagement)

    # Histograma de menciones por candidato
    df_mentions.plot(kind='bar', x='_id', y='count', color=['blue', 'red'])
    plt.title('Frecuencia de Menciones por Candidato')
    plt.xlabel('Candidato')
    plt.ylabel('Número de Menciones')
    plt.show()

    # Diagrama de dispersión para promedio de likes y retweets
    plt.scatter(df_engagement['avg_likes'], df_engagement['avg_retweets'])
    plt.title('Promedio de Likes vs. Retweets por Candidato')
    plt.xlabel('Promedio de Likes')
    plt.ylabel('Promedio de Retweets')
    plt.show()

    # Línea de tendencia para evolución temporal de menciones
    for candidate in df_temporal['candidate'].unique():
        df_candidate = df_temporal[df_temporal['candidate'] == candidate]
        plt.plot(df_candidate['date'], df_candidate['count'], label=candidate)

    plt.title('Evolución Temporal de Menciones por Candidato')
    plt.xlabel('Fecha')
    plt.ylabel('Número de Menciones')
    plt.legend()
    plt.show()
