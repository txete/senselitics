from pyspark.sql.functions import when, col, lower, avg
import Stream_Processor

def perform_aggregations(spark):
    # Leer datos de MongoDB en un DataFrame de Spark
    df = spark.read.format("mongo").load()

    # Crear una vista temporal para ejecutar consultas SQL
    df.createOrReplaceTempView("tweets")

    # Agregación 1: Frecuencia de Menciones de Cada Candidato
    mentions_per_candidate = spark.sql("""
        SELECT CASE 
                 WHEN LOWER(tweet) LIKE '%biden%' THEN 'Biden'
                 WHEN LOWER(tweet) LIKE '%trump%' THEN 'Trump'
               END AS candidate,
               COUNT(*) as count
        FROM tweets
        WHERE LOWER(tweet) LIKE '%biden%' OR LOWER(tweet) LIKE '%trump%'
        GROUP BY candidate
    """)

    # Agregación 2: Número de Tweets por Estado
    tweets_by_state = spark.sql("""
        SELECT state, COUNT(*) as count
        FROM tweets
        GROUP BY state
    """)

    # Agregación 3: Evolución Temporal de las Menciones a Cada Candidato
    temporal_evolution = spark.sql("""
        SELECT TO_DATE(created_at) as date,
               CASE 
                 WHEN LOWER(tweet) LIKE '%biden%' THEN 'Biden'
                 WHEN LOWER(tweet) LIKE '%trump%' THEN 'Trump'
               END AS candidate,
               COUNT(*) as count
        FROM tweets
        WHERE LOWER(tweet) LIKE '%biden%' OR LOWER(tweet) LIKE '%trump%'
        GROUP BY date, candidate
    """)

    # Agregación 4: Promedio de Likes y Retweets por Mención de Candidato
    average_engagement_per_candidate = df.withColumn("candidate", 
                                                      when(lower(col("tweet")).like("%biden%"), "Biden")
                                                      .otherwise(when(lower(col("tweet")).like("%trump%"), "Trump")))
    average_engagement_per_candidate = average_engagement_per_candidate.groupBy("candidate") \
        .agg(avg("likes").alias("avg_likes"), avg("retweet_count").alias("avg_retweets"))

    return mentions_per_candidate, tweets_by_state, temporal_evolution, average_engagement_per_candidate

def main():
    spark = Stream_Processor.create_spark_session("SparkSQLAggregations")

    mentions_per_candidate, tweets_by_state, temporal_evolution, average_engagement_per_candidate = perform_aggregations(spark)

    # Imprimir los resultados
    print("Menciones por Candidato:")
    mentions_per_candidate.show()
    print("Tweets por Estado:")
    tweets_by_state.show()
    print("Evolución Temporal:")
    temporal_evolution.show()
    print("Promedio de Engagement por Candidato:")
    average_engagement_per_candidate.show()