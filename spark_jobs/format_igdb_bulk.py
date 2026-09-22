import os
import sys
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import (
    StructType, StructField, StringType, LongType, 
    DoubleType, ArrayType, BooleanType, IntegerType
)

def create_spark_session():
    """Configuration Spark identique pour la cohérence."""
    minio_endpoint = os.getenv("MINIO_ENDPOINT", "http://minio:9000")
    minio_user = os.getenv("MINIO_ROOT_USER", "admin")
    minio_password = os.getenv("MINIO_ROOT_PASSWORD", "password123")

    spark = SparkSession.builder \
        .appName("IGDB Bulk Format") \
        .config("spark.sql.ansi.enabled", "false") \
        .config("spark.hadoop.fs.s3a.endpoint", minio_endpoint) \
        .config("spark.hadoop.fs.s3a.access.key", minio_user) \
        .config("spark.hadoop.fs.s3a.secret.key", minio_password) \
        .config("spark.hadoop.fs.s3a.path.style.access", "true") \
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
        .config("spark.hadoop.fs.s3a.aws.credentials.provider", "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider") \
        .getOrCreate()
    return spark

name_struct = StructType([StructField("name", StringType(), True)])
igdb_schema = StructType([
    StructField("id", LongType(), True),
    StructField("name", StringType(), True),
    StructField("slug", StringType(), True),
    StructField("summary", StringType(), True),
    StructField("storyline", StringType(), True),
    StructField("collection", name_struct, True),
    StructField("parent_game", name_struct, True),
    StructField("version_parent", name_struct, True),
    StructField("genres", ArrayType(name_struct), True),
    StructField("themes", ArrayType(name_struct), True),
    StructField("game_modes", ArrayType(name_struct), True),
    StructField("player_perspectives", ArrayType(name_struct), True),
    StructField("keywords", ArrayType(name_struct), True),
    StructField("game_engines", ArrayType(name_struct), True),
    StructField("franchises", ArrayType(name_struct), True),
    StructField("similar_games", ArrayType(name_struct), True),
    StructField("remakes", ArrayType(name_struct), True),
    StructField("remasters", ArrayType(name_struct), True),
    StructField("expansions", ArrayType(name_struct), True),
    StructField("standalone_expansions", ArrayType(name_struct), True),
    StructField("platforms", ArrayType(StructType([
        StructField("name", StringType(), True),
        StructField("abbreviation", StringType(), True)
    ])), True),
    StructField("involved_companies", ArrayType(StructType([
        StructField("developer", BooleanType(), True),
        StructField("publisher", BooleanType(), True),
        StructField("company", name_struct, True)
    ])), True),
    StructField("release_dates", ArrayType(StructType([
        StructField("human", StringType(), True),
        StructField("platform", name_struct, True)
    ])), True),
    StructField("age_ratings", ArrayType(StructType([
        StructField("rating", IntegerType(), True),
        StructField("category", IntegerType(), True)
    ])), True),
    StructField("external_games", ArrayType(StructType([
        StructField("uid", StringType(), True),
        StructField("external_game_source", name_struct, True)
    ])), True),
    StructField("first_release_date", LongType(), True),
    StructField("updated_at", LongType(), True),
    StructField("rating", DoubleType(), True),
    StructField("rating_count", LongType(), True),
    StructField("aggregated_rating", DoubleType(), True),
    StructField("aggregated_rating_count", LongType(), True),
    StructField("total_rating", DoubleType(), True),
    StructField("total_rating_count", LongType(), True),
    StructField("hypes", IntegerType(), True),
    StructField("follows", IntegerType(), True)
])

def transform_bulk_igdb():
    spark = create_spark_session()
    
    input_path = "s3a://data/raw/igdb/initial_games/*.json"
    output_path = "s3a://data/formatted/igdb/games/dt=initial"

    print(f"Reading data from {input_path}...")
    df = spark.read.schema(igdb_schema).json(input_path, multiLine=True)

    df = df.dropDuplicates(["id"])

    processed_df = df.select(
        F.col("id").cast("long"),
        F.col("name"),
        F.col("slug"),
        F.col("summary"),
        F.col("storyline"),
        F.from_unixtime(F.col("first_release_date")).cast("timestamp").alias("release_date"),
        F.from_unixtime(F.col("updated_at")).cast("timestamp").alias("updated_at"),
        F.col("genres.name").alias("genre_names"),
        F.col("themes.name").alias("theme_names"),
        F.col("game_modes.name").alias("game_mode_names"),
        F.col("platforms.name").alias("platform_names"),
        F.col("keywords.name").alias("keyword_names"),
        F.expr("filter(involved_companies, x -> x.developer = true).company.name").alias("developers"),
        F.expr("filter(involved_companies, x -> x.publisher = true).company.name").alias("publishers"),
        F.col("rating").cast("float"),
        F.col("rating_count").cast("int"),
        F.col("total_rating").cast("float"),
        F.col("total_rating_count").cast("int"),
        F.col("aggregated_rating").cast("float"),
        F.col("aggregated_rating_count").cast("int"),
        F.col("hypes").cast("int"),
        F.col("follows").cast("int"),
        F.col("collection.name").alias("collection_name"),
        F.col("franchises.name").alias("franchise_names"),
        F.col("similar_games.name").alias("similar_game_names"),
        F.expr("filter(external_games, x -> x.external_game_source.name = 'Steam')[0].uid").cast("long").alias("steam_appid")

    )

    processed_df = processed_df.na.fill({
        "summary": "",
        "storyline": "",
        "rating": 0,
        "rating_count": 0,
        "hypes": 0,
        "follows": 0
    })

    processed_df.coalesce(1).write.mode("overwrite").parquet(output_path)
    
    print(f"Import Bulk terminé : {processed_df.count()} jeux uniques traités.")

if __name__ == "__main__":
    transform_bulk_igdb()