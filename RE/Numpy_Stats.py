import numpy as np
import Convert_To_Pandas
import Data_From_Mongo

def main():
    db_name = 'reto2'
    collection_name = 'tweets_long'

    mentions, states, temporal, engagement = Data_From_Mongo.perform_aggregations(db_name, collection_name)
    df_mentions, df_states, df_temporal, df_engagement = Convert_To_Pandas.convertir_pandas(mentions, states, temporal, engagement)

    # Ejemplo de estadísticas en df_mentions (ajustar según las columnas reales)
    if not df_mentions.empty:
        media_mentions = np.mean(df_mentions['count'])
        std_mentions = np.std(df_mentions['count'])
        print(f"Media de Menciones: {media_mentions}, Desviación Estándar de Menciones: {std_mentions}")

    # Ejemplo de estadísticas en df_states (ajustar según las columnas reales)
    if not df_states.empty:
        media_states = np.mean(df_states['count'])
        std_states = np.std(df_states['count'])
        print(f"Media de Tweets por Estado: {media_states}, Desviación Estándar de Tweets por Estado: {std_states}")

    # Ejemplo de estadísticas en df_temporal (ajustar según las columnas reales)
    if not df_temporal.empty:
        media_temporal = np.mean(df_temporal['count'])
        std_temporal = np.std(df_temporal['count'])
        print(f"Media de Menciones Temporales: {media_temporal}, Desviación Estándar de Menciones Temporales: {std_temporal}")

    # Ejemplo de estadísticas en df_engagement (ajustar según las columnas reales)
    if not df_engagement.empty:
        media_likes = np.mean(df_engagement['avg_likes'])
        std_likes = np.std(df_engagement['avg_likes'])
        media_retweets = np.mean(df_engagement['avg_retweets'])
        std_retweets = np.std(df_engagement['avg_retweets'])
        correlacion = np.corrcoef(df_engagement['avg_likes'], df_engagement['avg_retweets'])[0, 1]
        print(f"Media de Likes: {media_likes}, Desviación Estándar de Likes: {std_likes}")
        print(f"Media de Retweets: {media_retweets}, Desviación Estándar de Retweets: {std_retweets}")
        print(f"Correlación entre Likes y Retweets: {correlacion}")
