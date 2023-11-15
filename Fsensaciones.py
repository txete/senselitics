import pandas as pd
import re
import nltk
from tqdm import tqdm
import F
from nltk.corpus import stopwords
from nltk.tokenize import word_tokenize
from textblob import TextBlob

def subjetividad(tweet):
    return TextBlob(tweet).sentiment.subjectivity

def polaridad(tweet):
    return TextBlob(tweet).sentiment.polarity

def conclusion(val):
    if val<0:
        return -1
    elif val==0:
        return 0
    else:
        return 1

def seleccionarUSA(df):
    print('-------------------------------------------------------------')
    print("Seleccionando USA")
    d = {"United States of America":"United States"}
    df['country'].replace(d, inplace=True)
    df = df.loc[df['country'] == "United States"]
    return df

def limpiarTweet(tweet,pbar):
    pbar.update(1)
    tweet = tweet.lower()
    to_remove = r'\d+|http?\S+|[^A-Za-z0-9]+'
    tweet = re.sub(to_remove, ' ', tweet) 
    
    stop_words = set(stopwords.words('english'))
    word_tokens = word_tokenize(tweet)

    filtered = []
    for w in word_tokens:
        if w not in stop_words:
            filtered.append(w)
    
    return filtered

def convertirSentimientos(df,candidato):
    print('-------------------------------------------------------------')
    print("Convirtiendo Sensaciones de "+candidato)
    pbar = tqdm(total=len(df['tweet']))
    Trump = df['tweet'].apply(lambda x: limpiarTweet(x,pbar))
    pbar.close()

    subjectivity_col = df['tweet'].apply(subjetividad)
    polarity_col = df['tweet'].apply(polaridad)
    analysis_col = polarity_col.apply(conclusion)

    df = {'Tweet': df['tweet'], 'Candidato': candidato, 'Cod_Estado': df['state_code'], 'Estado': df['state'], 'Subjetividad': subjectivity_col, 'Polaridad': polarity_col, 'Sentimiento': analysis_col}
    return pd.DataFrame(df)

def comprobarSentimientos(df):
    neg_num = df[df['Sentimiento']==-1].Sentimiento.count()
    neu_num = df[df['Sentimiento']==0].Sentimiento.count()
    pos_num = df[df['Sentimiento']==1].Sentimiento.count()
    return {'neg':neg_num,'neu':neu_num,'pos':pos_num}

def obtenerSensaciones(candidatos):
    sentimientos = []
    for candidato in candidatos:
        df = seleccionarUSA(F.importarFichero(candidato+'.csv'))
        sentimientos.append(convertirSentimientos(df,candidato))
    return sentimientos

def prueba(txt):
    print(txt)