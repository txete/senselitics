import pymongo

def perform_aggregations(db_name, collection_name):
    ip = '192.168.23.32'
    port = 27017

    client = pymongo.MongoClient(f"mongodb://{ip}:{port}/")
    db = client[db_name]
    collection = db[collection_name]

    # Agregación 1: Frecuencia de Menciones de Cada Candidato
    mentions_per_candidate = collection.aggregate([
        {
            "$match": {
                "tweet": {
                    "$regex": "Biden|Trump",
                    "$options": "i"
                }
            }
        },
        {
            "$group": {
                "_id": {
                    "$cond": [
                        {
                            "$regexMatch": {
                                "input": "$tweet",
                                "regex": "Biden",
                                "options": "i"
                            }
                        },
                        "Biden",
                        "Trump"
                    ]
                },
                "count": {
                    "$sum": 1
                }
            }
        }
    ])

    # Agregación 2: Número de Tweets por Estado
    tweets_by_state = collection.aggregate([
        {
            "$project": {
                "country": 1,
                "state": 1,
                "candidate": {
                    "$cond": [
                        {
                            "$regexMatch": {
                                "input": "$tweet",
                                "regex": "Biden",
                                "options": "i"
                            }
                        },
                        "Biden",
                        "Trump"
                    ]
                }
            }
        },
        {
            "$group": {
                "_id": {
                    "country": "$country",
                    "state": "$state",
                    "candidate": "$candidate"
                },
                "count": {
                    "$sum": 1
                }
            }
        },
        {
            "$project": {
                "id": "$_id.candidate",
                "state": "$_id.state",
                "country": "$_id.country",
                "count": 1
            }
        }
    ])

    # Agregación 3: Evolución Temporal de las Menciones a Cada Candidato
    temporal_evolution = collection.aggregate([
        {
            "$match": {
                "tweet": {
                    "$regex": "Biden|Trump",
                    "$options": "i"
                }
            }
        },
        {
            "$project": {
                "date": {
                    "$substr": [
                        "$created_at",
                        0,
                        10
                    ]
                },
                "tweet": 1
            }
        },
        {
            "$group": {
                "_id": {
                    "date": "$date",
                    "candidate": {
                        "$cond": [
                            {
                                "$regexMatch": {
                                    "input": "$tweet",
                                    "regex": "Biden",
                                    "options": "i"
                                }
                            },
                            "Biden",
                            "Trump"
                        ]
                    }
                },
                "count": {
                    "$sum": 1
                }
            }
        },
        {
            "$project": {
                "id": "$_id.candidate",
                "date": "$_id.date",
                "count": 1
            }
        }
    ])
  
    # Agregación 4: Promedio de Likes y Retweets por Mención de Candidato
    average_engagement_per_candidate = collection.aggregate([
        {
            "$match": {
                "tweet": {
                    "$regex": "Biden|Trump",
                    "$options": "i"
                },
                "likes": {
                    "$exists": True,
                    "$ne": None
                },
                "retweet_count": {
                    "$exists": True,
                    "$ne": None
                }
            }
        },
        {
            "$group": {
                "_id": {
                    "$cond": [
                        {
                            "$regexMatch": {
                                "input": "$tweet",
                                "regex": "Biden",
                                "options": "i"
                            }
                        },
                        "Biden",
                        "Trump"
                    ]
                },
                "avg_likes": {
                    "$avg": {
                        "$toDouble": "$likes"
                    }
                },
                "avg_retweets": {
                    "$avg": {
                        "$toDouble": "$retweet_count"
                    }
                }
            }
        }
    ])

    results_mentions_per_candidate = list(mentions_per_candidate)
    results_tweets_by_state = list(tweets_by_state)
    results_temporal_evolution = list(temporal_evolution)
    results_average_engagement_per_candidate = list(average_engagement_per_candidate)

    client.close()
    return results_mentions_per_candidate, results_tweets_by_state, results_temporal_evolution, results_average_engagement_per_candidate

def main():
    db_name = 'DBreto2'
    collection_name = 'tweetsF'

    mentions_per_candidate, tweets_by_state, temporal_evolution, average_engagement_per_candidate = perform_aggregations(db_name, collection_name)

    # Imprimir los resultados
    print("Menciones por Candidato:", mentions_per_candidate)
    print("Tweets por Estado:", tweets_by_state)
    print("Evolución Temporal:", temporal_evolution)
    print("Promedio de Engagement por Candidato:", average_engagement_per_candidate)
