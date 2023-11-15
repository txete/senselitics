import re

def es_inicio_de_registro(linea):
    """ Verifica si la línea comienza con una fecha y un número, siguiendo el patrón dado """
    return bool(re.match(r'\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2},\d+\.\d+e\+\d+', linea))

def limpiar_csv(archivo_entrada, archivo_salida):
    with open(archivo_entrada, 'r', encoding='utf-8') as file_in, \
         open(archivo_salida, 'w', encoding='utf-8') as file_out:
        
        linea_actual = ''
        for linea in file_in:
            if es_inicio_de_registro(linea):
                # Si es el inicio de un nuevo registro, escribe el anterior (si existe)
                if linea_actual:
                    file_out.write(linea_actual + '\n')
                linea_actual = linea.strip()
            else:
                # Si no es el inicio, es una continuación del registro actual
                linea_actual += ' ' + linea.strip()

        # No olvides escribir la última línea si existe
        if linea_actual:
            file_out.write(linea_actual + '\n')

# Llama a la función con tus nombres de archivo
# limpiar_csv('hashtag_joebiden.csv', 'biden.csv')
limpiar_csv('hashtag_donaldtrump.csv','trump.csv')
