import os
import sys
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import (
    StructType, StructField, StringType, LongType, ArrayType, BooleanType
)

def create_spark_session():
    minio_endpoint = os.getenv("MINIO_ENDPOINT", "http://minio:9000")
    minio_user = os.getenv("MINIO_ROOT_USER", "admin")
    minio_password = os.getenv("MINIO_ROOT_PASSWORD", "password123")

    return SparkSession.builder \
        .appName("Steam_Libraries_Consolidator") \
        .config("spark.sql.ansi.enabled", "false") \
        .config("spark.hadoop.fs.s3a.endpoint", minio_endpoint) \
        .config("spark.hadoop.fs.s3a.access.key", minio_user) \
        .config("spark.hadoop.fs.s3a.secret.key", minio_password) \
        .config("spark.hadoop.fs.s3a.path.style.access", "true") \
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
        .config("spark.hadoop.fs.s3a.aws.credentials.provider", "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider") \
        .getOrCreate()

steam_game_schema = StructType([
    StructField("appid", LongType(), True),
    StructField("name", StringType(), True),
    StructField("playtime_forever", LongType(), True), 
    StructField("playtime_2weeks", LongType(), True),   
    StructField("img_icon_url", StringType(), True),
    StructField("has_community_visible_stats", BooleanType(), True),
    StructField("has_leaderboards", BooleanType(), True),
    StructField("content_descriptorids", ArrayType(LongType()), True)
])

steam_user_schema = StructType([
    StructField("steam_id", StringType(), True),
    StructField("scraped_at", StringType(), True),
    StructField("games", ArrayType(steam_game_schema), True)
])

def format_steam_libraries():
    spark = create_spark_session()
    
    input_path = "s3a://data/raw/steam/users_library/"
    output_path = "s3a://data/formatted/steam/steam_users_consolidated.parquet"
    checkpoint_path = "s3a://data/formatted/steam/_checkpoints/steam_users"

    print("Lecture des bibliothèques Steam...")
    df = spark.readStream.schema(steam_user_schema).json(input_path)

    LIMIT_GAMES_PER_USER = 500

    processed_df = df.select(
        F.col("steam_id"),
        F.to_timestamp(F.col("scraped_at")).alias("scraped_at"),
        F.col("games")
    ).filter(
        F.col("steam_id").isNotNull() & 
        F.col("games").isNotNull() & 
        (F.size(F.col("games")) > 0) & 
        (F.size(F.col("games")) <= LIMIT_GAMES_PER_USER)
    )

    print(f"Sauvegarde des bibliothèques Steam dans {output_path}...")
    query = processed_df.writeStream \
        .format("parquet") \
        .option("checkpointLocation", checkpoint_path) \
        .trigger(availableNow=True) \
        .start(output_path)
    
    query.awaitTermination()
    print("Formattage Steam terminé")

if __name__ == "__main__":
    format_steam_libraries()