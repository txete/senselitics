from pymongo import MongoClient

client = MongoClient('mongodb://192.168.23.32:27017/')
db = client.admin

# Intenta obtener la lista de colecciones
try:
    collections = db.list_collection_names()
    print(collections)
except Exception as e:
    print(f"Error conectando a MongoDB: {e}")
finally:
    client.close()