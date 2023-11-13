import pandas as pd
from collections import Counter

adjetivos_positivos = ['bueno', 'excelente', 'fantástico', 'positivo', 'maravilloso']
adjetivos_negativos = ['malo', 'terrible', 'horrible', 'negativo', 'desastroso']

datos = pd.read_parquet('./report.parquet')

marca = "PSOE"  # Ejemplo de marca
resultados = []

for tweet in datos['text']:
    if marca.lower() in tweet.lower():
        print(tweet)
        palabras = tweet.split()
        for palabra in palabras:
            if palabra.lower() in adjetivos_positivos:
                resultados.append((palabra, 'positivo'))
            elif palabra.lower() in adjetivos_negativos:
                resultados.append((palabra, 'negativo'))

conteo = Counter(resultados)
print(conteo)
