import json
import csv
import re

def importarFicheroJSON(fichero):
    json_data = []
    with open(fichero, mode='r', encoding='utf-8') as csv_file:
        csv_reader = csv.DictReader(csv_file)
        for row in csv_reader:
            json_data.append(row)
        return json.dumps(json_data, indent=4)
    
def es_inicio_de_registro(linea):
    """ Verifica si la línea comienza con una fecha y un número, siguiendo el patrón dado """
    return bool(re.match(r'\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2},\d+\.\d+e\+\d+', linea))

def limpiar_csv(archivo_entrada, archivo_salida):
    with open(archivo_entrada, 'r', encoding='utf-8') as file_in, \
         open(archivo_salida, 'w', encoding='utf-8') as file_out:
        
        linea_actual = ''
        for linea in file_in:
            if es_inicio_de_registro(linea):
                if linea_actual:
                    file_out.write(linea_actual + '\n')
                linea_actual = linea.strip()
            else:
                linea_actual += ' ' + linea.strip()

        if linea_actual:
            file_out.write(linea_actual + '\n')


if __name__ == "__main__":
    # limpiar_csv('hashtag_donaldtrump.csv', 'trump.csv')
    limpiar_csv('hashtag_joebiden.csv', 'biden.csv')