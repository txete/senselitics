import pandas as pd
import Data_From_Mongo

def export_data_to_csv(db_name, collection_name, export_folder):
    # Extraer datos
    mentions, states, temporal, engagement = Data_From_Mongo.perform_aggregations(db_name, collection_name)

    # Convertir a DataFrames de Pandas
    df_mentions = pd.DataFrame(mentions)
    df_states = pd.DataFrame(states)
    df_temporal = pd.DataFrame(temporal)
    df_engagement = pd.DataFrame(engagement)

    # Exportar a CSV
    df_mentions.to_json(f"{export_folder}/mentions_per_candidate.json", index=False)
    df_states.to_json(f"{export_folder}/tweets_by_state.json", index=False)
    df_temporal.to_json(f"{export_folder}/temporal_evolution.json", index=False)
    df_engagement.to_json(f"{export_folder}/average_engagement_per_candidate.json", index=False)

def main():
    db_name = 'reto2'
    collection_name = 'tweets_long'
    export_folder = 'exported_data'

    # Exportar los datos agregados a CSV
    export_data_to_csv(db_name, collection_name, export_folder)
