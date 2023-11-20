import re
import nltk
from nltk.corpus import stopwords
from nltk.tokenize import word_tokenize
from textblob import TextBlob

def subjetividad(tweet):
    return TextBlob(tweet).sentiment.subjectivity

def polaridad(tweet):
    return TextBlob(tweet).sentiment.polarity

def conclusion(val):
    if val<0 :
        return -1
    elif val==0:
        return 0
    else:
        return 1

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

def comprobarSentimientos(df):
    neg_num = df[df['Sentimiento']==-1].Sentimiento.count()
    neu_num = df[df['Sentimiento']==0].Sentimiento.count()
    pos_num = df[df['Sentimiento']==1].Sentimiento.count()
    return {'neg':neg_num,'neu':neu_num,'pos':pos_num}

def prueba(txt):
    print(txt)