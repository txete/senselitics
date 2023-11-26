import pymongo

def perform_aggregations(db_name, collection_name):
    ip = '192.168.1.167'
    port = 27017

    client = pymongo.MongoClient(f"mongodb://{ip}:{port}/")
    db = client[db_name]
    collection = db[collection_name]

    # Agregación 1: Frecuencia de Menciones de Cada Candidato
    mentions_per_candidate = collection.aggregate([
        {"$match": {"tweet": {"$regex": "Biden|Trump", "$options": "i"}}},
        {"$group": {"_id": {"$cond": [{"$regexMatch": {"input": "$tweet", "regex": "Biden", "options": "i"}}, "Biden", "Trump"]}, "count": {"$sum": 1}}}
    ])

    # Agregación 2: Número de Tweets por Estado
    tweets_by_state = collection.aggregate([
        {"$group": {"_id": "$state", "count": {"$sum": 1}}}
    ])

    # Agregación 3: Evolución Temporal de las Menciones a Cada Candidato
    temporal_evolution = collection.aggregate([
        {"$match": {"tweet": {"$regex": "Biden|Trump", "$options": "i"}}},
        {"$group": {"_id": {"date": {"$dateToString": {"format": "%Y-%m-%d", "date": "$created_at"}}, "candidate": {"$cond": [{"$regexMatch": {"input": "$tweet", "regex": "Biden", "options": "i"}}, "Biden", "Trump"]}}, "count": {"$sum": 1}}}
    ])

    # Agregación 4: Promedio de Likes y Retweets por Mención de Candidato
    average_engagement_per_candidate = collection.aggregate([
        {"$match": {"tweet": {"$regex": "Biden|Trump", "$options": "i"}}},
        {"$group": {"_id": {"$cond": [{"$regexMatch": {"input": "$tweet", "regex": "Biden", "options": "i"}}, "Biden", "Trump"]}, "avg_likes": {"$avg": "$likes"}, "avg_retweets": {"$avg": "$retweet_count"}}}
    ])

    client.close()
    return list(mentions_per_candidate), list(tweets_by_state), list(temporal_evolution), list(average_engagement_per_candidate)

def main():
    db_name = 'reto2'
    collection_name = 'tweets_long'

    mentions_per_candidate, tweets_by_state, temporal_evolution, average_engagement_per_candidate = perform_aggregations(db_name, collection_name)

    # Imprimir los resultados
    print("Menciones por Candidato:", mentions_per_candidate)
    print("Tweets por Estado:", tweets_by_state)
    print("Evolución Temporal:", temporal_evolution)
    print("Promedio de Engagement por Candidato:", average_engagement_per_candidate)
