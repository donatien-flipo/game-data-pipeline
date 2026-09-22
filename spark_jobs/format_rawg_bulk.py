import os
import sys
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import *
from pyspark.sql.types import (
    StructType, StructField, StringType, LongType, 
    DoubleType, ArrayType, BooleanType, IntegerType
)
def create_spark_session():
    """Configuration Spark pour MinIO."""
    minio_endpoint = os.getenv("MINIO_ENDPOINT", "http://minio:9000")
    minio_user = os.getenv("MINIO_ROOT_USER", "admin")
    minio_password = os.getenv("MINIO_ROOT_PASSWORD", "password123")

    spark = SparkSession.builder \
        .appName("RAWG Bulk Format") \
        .config("spark.sql.ansi.enabled", "false") \
        .config("spark.hadoop.fs.s3a.endpoint", minio_endpoint) \
        .config("spark.hadoop.fs.s3a.access.key", minio_user) \
        .config("spark.hadoop.fs.s3a.secret.key", minio_password) \
        .config("spark.hadoop.fs.s3a.path.style.access", "true") \
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
        .config("spark.hadoop.fs.s3a.aws.credentials.provider", "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider") \
        .getOrCreate()
    return spark

rawg_schema = StructType([
    StructField("id", LongType(), True),
    StructField("slug", StringType(), True),
    StructField("name", StringType(), True),
    StructField("playtime", IntegerType(), True),
    StructField("released", StringType(), True),
    StructField("updated", StringType(), True),
    StructField("rating", DoubleType(), True),
    StructField("rating_top", IntegerType(), True),
    StructField("ratings_count", IntegerType(), True),
    StructField("reviews_count", IntegerType(), True),
    StructField("metacritic", IntegerType(), True),
    StructField("suggestions_count", IntegerType(), True),
    StructField("added", IntegerType(), True),
    StructField("tba", BooleanType(), True),
    StructField("added_by_status", StructType([
        StructField("yet", IntegerType(), True),
        StructField("owned", IntegerType(), True),
        StructField("beaten", IntegerType(), True),
        StructField("toplay", IntegerType(), True),
        StructField("dropped", IntegerType(), True),
        StructField("playing", IntegerType(), True)
    ]), True),
    StructField("platforms", ArrayType(StructType([
        StructField("platform", StructType([
            StructField("name", StringType(), True),
            StructField("slug", StringType(), True)
        ]))
    ])), True),
    StructField("genres", ArrayType(StructType([
        StructField("name", StringType(), True),
        StructField("slug", StringType(), True)
    ])), True),
    StructField("tags", ArrayType(StructType([
        StructField("name", StringType(), True),
        StructField("slug", StringType(), True),
        StructField("language", StringType(), True)
    ])), True),
    StructField("stores", ArrayType(StructType([
        StructField("store", StructType([
            StructField("name", StringType(), True)
        ]))
    ])), True),
    StructField("esrb_rating", StructType([
        StructField("name", StringType(), True)
    ]), True)
])

def transform_rawg_bulk():
    spark = create_spark_session()
    
    input_path = "s3a://data/raw/rawg/initial_games/*.json"
    output_path = "s3a://data/formatted/rawg/games/dt=initial"

    print(f"Lecture des fichiers bulk RAWG depuis {input_path}...")
    
    df = spark.read.schema(rawg_schema).json(input_path, multiLine=True)

    df = df.dropDuplicates(["id"])

    processed_df = df.select(
        F.col("id"),
        F.col("name"),
        F.col("slug"),
        
        F.to_date(F.col("released")).alias("release_date"),
        F.to_timestamp(F.col("updated")).alias("updated_at"),
        
        F.col("rating").cast("float"),
        F.col("metacritic").cast("int"),
        F.col("playtime").cast("int"),
        F.col("added").alias("total_added"),
        F.col("ratings_count"),
        F.col("reviews_count"),
        
        F.col("added_by_status.owned").alias("status_owned"),
        F.col("added_by_status.beaten").alias("status_beaten"),
        F.col("added_by_status.dropped").alias("status_dropped"),
        F.col("added_by_status.toplay").alias("status_toplay"),
        
        F.col("genres.name").alias("genre_names"),
        F.col("platforms.platform.name").alias("platform_names"),
        F.col("stores.store.name").alias("store_names"),
        
        F.expr("filter(tags, x -> x.language = 'eng').name").alias("tag_names"),
        
        F.col("esrb_rating.name").alias("esrb_rating_name")
    )

    processed_df = processed_df.na.fill({
        "rating": 0,
        "metacritic": 0,
        "playtime": 0,
        "status_owned": 0,
        "status_beaten": 0,
        "status_dropped": 0,
        "status_toplay": 0
    })

    print(f"Écriture du Parquet vers {output_path}...")
    processed_df.coalesce(1).write.mode("overwrite").parquet(output_path)
    
    print(f"Bulk RAWG terminé : {processed_df.count()} jeux traités.")

if __name__ == "__main__":
    transform_rawg_bulk()