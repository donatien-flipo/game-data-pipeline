import os
import sys
from pyspark.sql import SparkSession

def create_spark_session():
    minio_endpoint = os.getenv("MINIO_ENDPOINT", "http://minio:9000")
    minio_user = os.getenv("MINIO_ROOT_USER", "admin")
    minio_password = os.getenv("MINIO_ROOT_PASSWORD", "password123")
    es_host = os.getenv("ELASTICSEARCH_HOST", "elasticsearch")
    es_port = os.getenv("ELASTICSEARCH_PORT", "9200")

    return SparkSession.builder \
        .appName("Gold_to_Elasticsearch") \
        .config("spark.sql.ansi.enabled", "false") \
        .config("spark.hadoop.fs.s3a.endpoint", minio_endpoint) \
        .config("spark.hadoop.fs.s3a.access.key", minio_user) \
        .config("spark.hadoop.fs.s3a.secret.key", minio_password) \
        .config("spark.hadoop.fs.s3a.path.style.access", "true") \
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
        .config("spark.es.nodes", es_host) \
        .config("spark.es.port", es_port) \
        .config("spark.es.nodes.wan.only", "true") \
        .config("spark.es.net.ssl", "false") \
        .config("spark.es.index.auto.create", "true") \
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
        .getOrCreate()

def index_data():
    spark = create_spark_session()

    PATH_GOLD_GAMES = "s3a://data/usage/master_games"
    INDEX_NAME = "games_catalog"

    print(f"Lecture du catalogue Master Games depuis {PATH_GOLD_GAMES}...")
    df_games = spark.read.format("delta").load(PATH_GOLD_GAMES)

    if df_games.rdd.isEmpty():
        print("Aucune donnée à indexer.")
        return

    df_games_prepped = df_games.withColumn("igdb_id", df_games["igdb_id"].cast("string"))

    print(f"📤 Indexation de {df_games_prepped.count()} jeux dans l'index Elasticsearch '{INDEX_NAME}'...")
    
    (df_games_prepped.write
    .format("es")
    .option("es.mapping.id", "igdb_id")    
    .option("es.write.operation", "upsert")  
    .mode("append")                          
    .save(INDEX_NAME))
    

    print(f"Index '{INDEX_NAME}' créé/mis à jour.")
    spark.stop()

if __name__ == "__main__":
    index_data()