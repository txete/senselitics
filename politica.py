import json
import pandas as pd
from collections import Counter

adjetivos_positivos = ['strong','support','ready','willing','remarkable','fierce','inspiration','classy','joy','hope','optimist','celebrate','praise','love','courage','succeeding','together','compromise','protect','ready','peaceful','amazing','unity','prepared','comfort','honorable','leadership','positivity','understanding','caring','unity','remarkable','peace','hopeful','supportive','inspired','admirable','positive','committed','grateful','celebratory','empathy','encouraging','pride','honoring','resilient','dedication','successful','gracious','warmth','enthusiasm','heartfelt']
adjetivos_negativos = ['fuck','silly','clueless','tragedy','crisis','problem','stung','plummet','violence','murdered','recession','depression','loses','loser','lost','hissy','traitor','lying','arrogant','slimy','hater','suffers','paranoia','delusional','psychological','ruin','suffering','hell','dictate','chaos','terrorist','communist','socialism','discredit','dictator','betray','spoiled','elitist','desperate','complains','worry','problematic','disgrace','hated','hate','unhappy','angry','frustrated','negative','disappointed','sad','upset','fear','anxious','tense','stress','worried','doubt','uncertain','difficult','hard','challenge','trouble','obstacle','issue','complicated','complex','demanding','exhausting','overwhelming','intense','serious','severe','critical','dangerous','risky','harmful','toxic','damaging','hurtful','painful','harsh','aggressive','hostile','angry','furious','rage','outrage','annoyed','irritated','frustrated','displeased','upset','disappointed','letdown','unfulfilled','unhappy','sad','depressed','melancholy','gloomy','desolate','miserable','heartbroken','despair','hopeless','helpless','powerless','weak','vulnerable','insecure','doubtful','uncertain','confused','lost','perplexed','bewildered']

tweets = []

df = pd.read_csv('hashtag_joebiden.csv')

nombre_columna = 'columna_especifica'

marca = "Biden"

for index, row in df.iterrows():
    texto = row['tweet']
    if marca.lower() in texto.lower():
        palabras = texto.split()
        positive = 0
        negative = 0
        for palabra in palabras:
            if palabra.lower() in adjetivos_positivos:
                positive += 1
            elif palabra.lower() in adjetivos_negativos:
                negative += 1
        if positive > negative:
            df.at[index, 'support'] = 1
        else:
            df.at[index, 'support'] = 0

print(df)
    

# with df as file:
#     for line in file:
#         try:
#             print(line)
#         except json.JSONDecodeError:
#             print("Error al decodificar JSON en la línea:", line)

# marca = "Obama"
# resultados = []

# for tweet in tweets:
#     texto = tweet.get('text', '')
#     fecha = tweet.get('created_at', '')
#     if marca.lower() in texto.lower():
#         palabras = texto.split()
#         for palabra in palabras:
#             if palabra.lower() in adjetivos_positivos:
#                 resultados.append((fecha, palabra, 'positivo'))
#             elif palabra.lower() in adjetivos_negativos:
#                 resultados.append((fecha, palabra, 'negativo'))

# conteo = Counter(resultados)
# # print(conteo)

# df = pd.DataFrame(resultados, columns=['Fecha', 'Adjetivo', 'Sentimiento'])

# conteo_por_fecha = df.groupby(['Fecha', 'Sentimiento']).count()

# df.to_json('resultados_sentimientos.json', orient='records', lines=True, date_format='iso')

# print("Archivo JSON exportado con éxito.")