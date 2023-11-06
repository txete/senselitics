import pymongo

client = pymongo.MongoClient("mongodb://192.168.22.234/")
db = client["prueba"]
collec = db["prueba"]

nuevo_tweet = {
    "usuario": "otro",
    "contenido": "otro mas"
}

collec.insert_one(nuevo_tweet)