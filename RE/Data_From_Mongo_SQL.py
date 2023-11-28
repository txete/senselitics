from pyspark.sql.functions import when, col, lower, avg
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, LongType, IntegerType, TimestampType, ArrayType, DoubleType

def perform_aggregations(spark):
    # Leer datos de MongoDB en un DataFrame de Spark
    # df = spark.read.format("mongo").load()
    schema = StructType([
        StructField("_id", StringType(), True),
        StructField("created_at", StringType(), True),
        StructField("tweet_id", StringType(), True),
        StructField("tweet", StringType(), True),
        StructField("likes", StringType(), True),
        StructField("retweet_count", StringType(), True),
        StructField("source", StringType(), True),
        StructField("user_id", StringType(), True), 
        StructField("user_name", StringType(), True),
        StructField("user_screen_name", StringType(), True),
        StructField("user_description", StringType(), True),
        StructField("user_join_date", StringType(), True),
        StructField("user_followers_count", StringType(), True),
        StructField("user_location", StringType(), True),
        StructField("lat", StringType(), True),
        StructField("long", StringType(), True),
        StructField("city", StringType(), True),
        StructField("country", StringType(), True),
        StructField("continent", StringType(), True),
        StructField("state", StringType(), True),
        StructField("state_code", StringType(), True),
        StructField("collected_at", StringType(), True),
    ])

    df = spark.read.format("mongo").schema(schema).option("partitioner", "MongoSinglePartitioner").load().repartition(1)

    # Crear una vista temporal para ejecutar consultas SQL
    df.createOrReplaceTempView("tweets")

    # Agregación 1: Frecuencia de Menciones de Cada Candidato
    mentions_per_candidate = spark.sql("""
        SELECT 
            CASE 
                WHEN LOWER(tweet) LIKE '%biden%' THEN 'Biden' 
                WHEN LOWER(tweet) LIKE '%trump%' THEN 'Trump' 
            END AS candidate, 
            COUNT(*) AS count
        FROM tweets
        WHERE LOWER(tweet) LIKE '%biden%' OR LOWER(tweet) LIKE '%trump%'
        GROUP BY candidate
    """)

    # Agregación 2: Número de Tweets por Estado
    tweets_by_state = spark.sql("""
        SELECT 
            country,
            state,
            CASE 
                WHEN LOWER(tweet) LIKE '%biden%' THEN 'Biden' 
                WHEN LOWER(tweet) LIKE '%trump%' THEN 'Trump' 
            END AS candidate, 
            COUNT(*) AS count
        FROM tweets
        WHERE LOWER(tweet) LIKE '%biden%' OR LOWER(tweet) LIKE '%trump%'
        GROUP BY country, state, candidate
    """)

    # Agregación 3: Evolución Temporal de las Menciones a Cada Candidato
    temporal_evolution = spark.sql("""
        SELECT 
            TO_DATE(created_at) AS date,
            CASE 
                WHEN LOWER(tweet) LIKE '%biden%' THEN 'Biden' 
                WHEN LOWER(tweet) LIKE '%trump%' THEN 'Trump' 
            END AS candidate, 
            COUNT(*) AS count
        FROM tweets
        WHERE LOWER(tweet) LIKE '%biden%' OR LOWER(tweet) LIKE '%trump%'
        GROUP BY date, candidate
    """)

    # Agregación 4: Promedio de Likes y Retweets por Mención de Candidato
    average_engagement_per_candidate = spark.sql("""
        SELECT 
            CASE 
                WHEN LOWER(tweet) LIKE '%biden%' THEN 'Biden' 
                WHEN LOWER(tweet) LIKE '%trump%' THEN 'Trump' 
            END AS candidate,
            AVG(CAST(likes AS DOUBLE)) AS avg_likes,
            AVG(CAST(retweet_count AS DOUBLE)) AS avg_retweets
        FROM tweets
        WHERE LOWER(tweet) LIKE '%biden%' OR LOWER(tweet) LIKE '%trump%'
        GROUP BY candidate                        
    """)

    return mentions_per_candidate, tweets_by_state, temporal_evolution, average_engagement_per_candidate

def main():
    # spark = Stream_Processor.create_spark_session("SparkSQLAggregations")

    ip = '192.168.23.32'
    puerto = '27017'
    bbdd = 'DBreto2'
    tbl = 'tweetsF'

    spark =SparkSession.builder \
        .appName("MongoDBIntegration") \
        .config("spark.mongodb.input.uri", f"mongodb://{ip}:{puerto}/{bbdd}.{tbl}") \
        .config("spark.mongodb.output.uri", f"mongodb://{ip}:{puerto}/{bbdd}.{tbl}") \
        .config("spark.jars.packages", "org.mongodb.spark:mongo-spark-connector_2.12:3.0.1") \
        .getOrCreate()

    # perform_aggregations(spark)
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