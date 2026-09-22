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
        .appName("Gold_to_ES_Recommendations_Indexer") \
        .config("spark.sql.ansi.enabled", "false") \
        .config("spark.sql.parquet.enableVectorizedReader", "false") \
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
        .getOrCreate()

def index_recommendations():
    spark = create_spark_session()
    PATH_GOLD_RECOMMENDATIONS = "s3a://data/usage/steam_recommendations"
    INDEX_RECOMMENDATIONS = "steam_recommendations"

    print(f"Lecture des recommandations depuis {PATH_GOLD_RECOMMENDATIONS}...")
    df_reco = spark.read.parquet(PATH_GOLD_RECOMMENDATIONS)

    if not df_reco.rdd.isEmpty():
        df_reco_prepped = df_reco.withColumn("steam_id", df_reco["steam_id"].cast("string"))
        
        print(f"Indexation de {df_reco_prepped.count()} profils dans '{INDEX_RECOMMENDATIONS}'...")
        df_reco_prepped.write \
            .format("es") \
            .option("es.mapping.id", "steam_id") \
            .option("es.write.operation", "upsert") \
            .mode("overwrite") \
            .save(INDEX_RECOMMENDATIONS)
    else:
        print("Aucune recommandation trouvée dans la table steam_recommendations.")

    spark.stop()

if __name__ == "__main__":
    index_recommendations()