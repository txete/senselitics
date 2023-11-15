import pandas as pd

def importarFichero(fichero):
    print('-------------------------------------------------------------')
    print("Importando fichero")
    return pd.read_csv(fichero)

def combinar(arrDF):
    return pd.concat(arrDF, ignore_index=True)

def convertirJSON(df,nombre_fichero):
    print("Generando archivo JSON")
    df.to_json(nombre_fichero+'.json', orient='records', lines=True, date_format='iso')
    print("Archivo JSON exportado con éxito.")

def prueba(txt):
    print(txt)