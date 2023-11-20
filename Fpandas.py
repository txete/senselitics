import pandas as pd
import Fsensaciones
from tqdm import tqdm

def seleccionarUSA(df):
    print('-------------------------------------------------------------')
    print("Seleccionando USA")
    d = {"United States of America":"United States"}
    df = df.copy()
    df['country'].replace(d, inplace=True)
    df = df.loc[df['country'] == "United States"]
    return df

def buscarPalabra(df,columna,texto):
    return df[df[columna].str.contains(texto, case=False, na=False)]

def convertirSentimientos(df,candidato,index):
    print('-------------------------------------------------------------')
    print("Convirtiendo Sensaciones de "+candidato)
    pbar = tqdm(total=len(df['tweet']))
    Trump = df['tweet'].apply(lambda x: Fsensaciones.limpiarTweet(x,pbar))
    pbar.close()

    subjectivity_col = df['tweet'].apply(Fsensaciones.subjetividad)
    polarity_col = df['tweet'].apply(Fsensaciones.polaridad)
    analysis_col = polarity_col.apply(Fsensaciones.conclusion)

    df = {'Tweet': df['tweet'], 'Retweet': df['retweet_count'], 'Candidato': candidato, 'Cod_Estado': df['state_code'], 'Estado': df['state'], 'Subjetividad': subjectivity_col, 'Polaridad': polarity_col, 'Sentimiento': analysis_col * index}
    return pd.DataFrame(df)
