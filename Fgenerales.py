import pandas as pd
import json
import csv

def importarFichero(fichero):
    print('-------------------------------------------------------------')
    print("Importando fichero")
    return pd.read_csv(fichero)

def importarFicheroJSON(fichero):
    print('-------------------------------------------------------------')
    print("Importando fichero")
    json_data = []
    with open(fichero, mode='r', encoding='utf-8') as csv_file:
        csv_reader = csv.DictReader(csv_file)
        for row in csv_reader:
            json_data.append(row)
        return json.dumps(json_data, indent=4)
    
def combinar(arrDF):
    return pd.concat(arrDF, ignore_index=True)

def convertirJSON(df,nombre_fichero):
    print("Generando archivo JSON")
    df.to_json(nombre_fichero+'.json', orient='records', lines=True, date_format='iso')
    print("Archivo JSON exportado con éxito.")

def prueba(txt):
    print(txt)