import os
import sys
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.window import Window

from pyspark.sql.types import StructType, StructField, StringType, LongType, FloatType, IntegerType, TimestampType, ArrayType
from delta.tables import DeltaTable

def create_spark_session():
    """Configuration de la session Spark pour MinIO."""
    minio_endpoint = os.getenv("MINIO_ENDPOINT", "http://minio:9000")
    minio_user = os.getenv("MINIO_ROOT_USER", "admin")
    minio_password = os.getenv("MINIO_ROOT_PASSWORD", "password123")

    return SparkSession.builder \
        .appName("Gold_Dual_Facets_Generator") \
        .config("spark.sql.ansi.enabled", "false") \
        .config("spark.hadoop.fs.s3a.endpoint", minio_endpoint) \
        .config("spark.hadoop.fs.s3a.access.key", minio_user) \
        .config("spark.hadoop.fs.s3a.secret.key", minio_password) \
        .config("spark.hadoop.fs.s3a.path.style.access", "true") \
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
        .config("spark.driver.memory", "16g") \
        .config("spark.executor.memory", "8g") \
        .config("spark.sql.shuffle.partitions", "800") \
        .config("spark.default.parallelism", "800") \
        .config("spark.memory.fraction", "0.8") \
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
        .getOrCreate()

igdb_formatted_schema = StructType([
    StructField("id", LongType(), True),
    StructField("name", StringType(), True),
    StructField("slug", StringType(), True),
    StructField("summary", StringType(), True),
    StructField("storyline", StringType(), True),
    
    StructField("release_date", TimestampType(), True),
    StructField("updated_at", TimestampType(), True),
    
    StructField("genre_names", ArrayType(StringType()), True),
    StructField("theme_names", ArrayType(StringType()), True),
    StructField("game_mode_names", ArrayType(StringType()), True),
    StructField("platform_names", ArrayType(StringType()), True),
    StructField("keyword_names", ArrayType(StringType()), True),
    
    StructField("developers", ArrayType(StringType()), True),
    StructField("publishers", ArrayType(StringType()), True),
    
    StructField("rating", FloatType(), True),
    StructField("rating_count", IntegerType(), True),
    StructField("total_rating", FloatType(), True),
    StructField("total_rating_count", IntegerType(), True),
    StructField("aggregated_rating", FloatType(), True),
    StructField("aggregated_rating_count", IntegerType(), True),
    StructField("hypes", IntegerType(), True),
    StructField("follows", IntegerType(), True),
    
    StructField("collection_name", StringType(), True),
    StructField("franchise_names", ArrayType(StringType()), True),
    StructField("similar_game_names", ArrayType(StringType()), True),
    
    StructField("steam_appid", LongType(), True),
    
    StructField("dt", StringType(), True)
])

def normalize_name(col):
    return F.regexp_replace(F.lower(col), r'[^a-z0-9]', "")

def deduplicate_temporal_data(df):
    window_spec = Window.partitionBy("id").orderBy(F.col("dt").desc())
    return df.withColumn("rn", F.row_number().over(window_spec)) \
             .filter("rn = 1").drop("rn")

def compute_dual_similarities(df_games):

    df_with_tags = df_games.withColumn(
        "unified_tags",
        F.array_distinct(
            F.concat(
                F.coalesce(F.col("genre_names"), F.array()),
                F.coalesce(F.col("theme_names"), F.array()),
                F.coalesce(F.col("tag_names"), F.array())
            )
        )
    ).withColumn("tag_count", F.size(F.col("unified_tags")))


    df_flat = df_with_tags.select(
        F.col("igdb_id"),
        F.col("igdb_name"),
        F.col("tag_count"),
        F.explode("unified_tags").alias("tag")
    )

    threshold = 1500 
    frequent_tags = df_flat.groupBy("tag").count().filter(f_col := F.col("count") > threshold)
    df_flat_filtered = df_flat.join(frequent_tags, on="tag", how="left_anti")
    
    df_pairs = df_flat_filtered.alias("a").join(
        df_flat.alias("b"),
        (F.col("a.tag") == F.col("b.tag")) & (F.col("a.igdb_id") != F.col("b.igdb_id")),
        how="inner"
    )

    df_scored = df_pairs.groupBy(
        F.col("a.igdb_id").alias("igdb_id"),
        F.col("a.igdb_name").alias("igdb_name"),
        F.col("b.igdb_name").alias("ref_name"),
        F.col("a.tag_count").alias("size_a"),
        F.col("b.tag_count").alias("size_b")
    ).agg(F.count("a.tag").alias("intersection")) \
     .withColumn(
         "similarity_score", 
         F.col("intersection") / (F.col("size_a") + F.col("size_b") - F.col("intersection"))
     )

    window_spec = Window.partitionBy("igdb_id").orderBy(F.col("similarity_score").desc(), F.col("ref_name"))
    df_top_10 = df_scored.withColumn("rank", F.row_number().over(window_spec)).filter("rank <= 10")

    df_similar_algo = df_top_10.groupBy("igdb_id").agg(
        F.collect_list("ref_name").alias("computed_similar_games")
    )

    df_result = df_games.join(df_similar_algo, on="igdb_id", how="left")

    df_clean = df_result.withColumn(
        "similar_game_names", 
        F.coalesce(F.col("similar_game_names"), F.array())
    ).withColumn(
        "computed_similar_games", 
        F.coalesce(F.col("computed_similar_games"), F.array())
    )

    return df_clean

def generate_usage_facets():
    spark = create_spark_session()

    PATH_IGDB = "s3a://data/formatted/igdb/games/"
    PATH_RAWG = "s3a://data/formatted/rawg/games/"
    PATH_STEAM = "s3a://data/formatted/steam/steam_users_consolidated.parquet"
    
    PATH_OUT_GAMES = "s3a://data/usage/master_games"
    PATH_OUT_INTERACTIONS = "s3a://data/usage/user_game_interactions"

    print("📥 1. Chargement et déduplication temporelle des sources...")
    df_igdb_clean = deduplicate_temporal_data(spark.read.schema(igdb_formatted_schema).parquet(PATH_IGDB))
    df_rawg_clean = deduplicate_temporal_data(spark.read.parquet(PATH_RAWG))
    df_steam_clean = spark.read.parquet(PATH_STEAM)

    df_igdb_prepped = df_igdb_clean
    df_igdb_ready = df_igdb_prepped.select(
        F.col("id").alias("igdb_id"),
        F.col("name").alias("igdb_name"),
        F.col("slug").alias("igdb_slug"),
        F.col("rating").alias("igdb_rating"),
        F.col("rating_count").alias("igdb_rating_count"),
        F.col("similar_game_names"),
        "steam_appid",
        F.col("release_date"),
        F.year("release_date").alias("release_year"),
        normalize_name("name").alias("norm_name"),
        "genre_names",
        "theme_names",
        "platform_names",
        "developers",
        "publishers"
    )

    df_rawg_ready = df_rawg_clean.select(
        F.col("id").alias("rawg_id"),
        F.col("rating").alias("rawg_rating"),
        F.col("metacritic").alias("rawg_metacritic"),
        F.col("status_owned").alias("rawg_status_owned"),
        F.col("status_beaten").alias("rawg_status_beaten"),
        F.col("status_dropped").alias("rawg_status_dropped"),
        F.year("release_date").alias("rawg_release_year"),
        normalize_name("name").alias("norm_name"),
        "tag_names"
    ).dropDuplicates(["norm_name", "rawg_release_year"])

    df_merged_metadata = df_igdb_ready.join(
        df_rawg_ready,
        (df_igdb_ready.norm_name == df_rawg_ready.norm_name) & 
        (df_igdb_ready.release_year == df_rawg_ready.rawg_release_year),
        how="left"
    ).drop(df_rawg_ready.norm_name)

    print("Génération des recommandation...")

    LIMIT_GAMES_PER_USER = 500
    
    df_steam_clean_filtered = df_steam_clean \
        .filter(F.col("games").isNotNull()) \
        .filter(F.size(F.col("games")) <= LIMIT_GAMES_PER_USER)

    df_steam_prepped = df_steam_clean_filtered \
        .select("steam_id", "games") \
        .repartition(100)

    df_active_interactions = df_steam_prepped.select(F.col("steam_id"), F.explode("games").alias("g")) \
        .select(
            F.col("steam_id"),
            F.col("g.appid").alias("steam_appid"),
            F.col("g.name").alias("game_name"),
            F.col("g.playtime_forever").cast("double").alias("playtime_forever"),
            F.col("g.playtime_2weeks").cast("double").alias("playtime_2weeks")
        ) \
        .filter("playtime_forever > 0")

    window_game_playtime = Window.partitionBy("steam_appid")
    df_interactions_with_avg = df_active_interactions.withColumn(
        "global_avg_playtime", 
        F.avg("playtime_forever").over(window_game_playtime)
    )

    df_interactions_final = df_interactions_with_avg.withColumn(
        "implicit_preference_score",
        F.log1p(
            F.col("playtime_forever") / F.col("global_avg_playtime")
        )
    )

    df_interactions_gold = df_interactions_final.join(
        df_merged_metadata.select(
            F.col("steam_appid").alias("meta_appid"),
            "genre_names",
            "theme_names",
            "tag_names"
        ),
        df_interactions_final.steam_appid == F.col("meta_appid"),
        how="left"
    ).drop("meta_appid")


    print("Génération du catalogue Master Games...")
    
    df_steam_aggregated = df_active_interactions.groupBy("steam_appid").agg(
        F.count("*").alias("steam_player_count"),
        F.sum("playtime_forever").alias("steam_total_playtime_forever"),
        F.sum(F.coalesce(F.col("playtime_2weeks"), F.lit(0.0))).alias("steam_total_playtime_2weeks"),
        F.avg("playtime_forever").alias("steam_avg_playtime")
    )

    df_gold_games = df_merged_metadata.join(
        df_steam_aggregated,
        df_merged_metadata.steam_appid == df_steam_aggregated.steam_appid,
        how="left"
    ).drop(df_steam_aggregated.steam_appid)

    df_gold_games = df_gold_games.na.fill({
        "steam_player_count": 0,
        "steam_total_playtime_forever": 0,
        "steam_total_playtime_2weeks": 0,
        "steam_avg_playtime": 0,
        "rawg_rating": 0,
        "rawg_metacritic": 0,
        "rawg_status_owned": 0,
        "rawg_status_beaten": 0,
        "rawg_status_dropped": 0
    })

    print("Calcul algorithmique des similarités de jeux...")
    df_gold_games_final = compute_dual_similarities(df_gold_games)

    df_gold_games_final.cache()
    df_interactions_gold.cache()

    count_catalog = df_gold_games_final.count()
    count_interactions = df_interactions_gold.count()
    count_unique_games_interactions = df_interactions_gold.select("steam_appid").distinct().count()
    try:
        delta_table = DeltaTable.forPath(spark, PATH_OUT_GAMES)
        print("Table Delta existante détectée. Lancement de la fusion...")
        
        delta_table.alias("target").merge(
            source=df_gold_games_final.alias("source"),
            condition="target.igdb_id = source.igdb_id"
        ).whenMatchedUpdateAll() \
         .whenNotMatchedInsertAll() \
         .execute()
        print("Fusion Delta effectuée.")
        
    except Exception as e:
        print(f"Initialisation de la table Delta : {e}")
        df_gold_games_final.write \
            .format("delta") \
            .mode("overwrite") \
            .save(PATH_OUT_GAMES)
        print("Table Delta initialisée.")

    print(f"Écriture de la table INTERACTIONS ({count_interactions} lignes) -> {PATH_OUT_INTERACTIONS}")
    df_interactions_gold.write.mode("overwrite").parquet(PATH_OUT_INTERACTIONS)

    df_gold_games_final.unpersist()
    df_interactions_gold.unpersist()    
    print("Succès du traitement et de la fusion")
    spark.stop()

if __name__ == "__main__":
    generate_usage_facets()