from hdfs import InsecureClient
import Export_To_PowerBi

def HDFS_client():    
    return InsecureClient('http://localhost:50070', user='raj_ops', timeout=120)

def HDFS_upload(file_path, hdfs_path):
    client = HDFS_client()
    with open(file_path, 'rb') as file:
        client.write(hdfs_path, file, overwrite=True)

def main():
    db_name = 'reto2'
    collection_name = 'tweets_long'
    directorio_json = 'RE'

    # Exportar los datos agregados a CSV
    # No necesitamos exportarlos, ya estaban si los hemos ejecutado antes
    # Export_To_PowerBi.export_data(db_name, collection_name, directorio_json)

    # Lista de nombres de archivos JSON a subir
    archivos_json = ['mentions_per_candidate.json', 'tweets_by_state.json', 'temporal_evolution.json', 'average_engagement_per_candidate.json']

    # Subir cada archivo JSON a HDFS
    for archivo_json in archivos_json:
        file_path = f"{directorio_json}/{archivo_json}"
        hdfs_path = f"/user/raj_ops/Tweet/Tweet_Processed_Data/{archivo_json}"
        HDFS_upload(file_path, hdfs_path)

    # archivos_json = ['biden.rar','trump.rar']

    # for archivo_json in archivos_json:
    #     file_path = f"{archivo_json}"
    #     hdfs_path = f"/user/raj_ops/Tweet/Tweet_Dataset/{archivo_json}"
    #     HDFS_upload(file_path, hdfs_path)

    return True