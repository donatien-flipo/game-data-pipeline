import os
import sys
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.window import Window
from pyspark.ml.feature import CountVectorizer, IDF
from pyspark.ml.clustering import KMeans
from pyspark.ml.feature import Normalizer
from pyspark.ml.evaluation import ClusteringEvaluator

def create_spark_session():
    minio_endpoint = os.getenv("MINIO_ENDPOINT", "http://minio:9000")
    minio_user = os.getenv("MINIO_ROOT_USER", "admin")
    minio_password = os.getenv("MINIO_ROOT_PASSWORD", "password123")

    return SparkSession.builder \
        .appName("Steam_Player_Clustering_ML") \
        .config("spark.sql.ansi.enabled", "false") \
        .config("spark.hadoop.fs.s3a.endpoint", minio_endpoint) \
        .config("spark.hadoop.fs.s3a.access.key", minio_user) \
        .config("spark.hadoop.fs.s3a.secret.key", minio_password) \
        .config("spark.hadoop.fs.s3a.path.style.access", "true") \
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
        .config("spark.driver.memory", "16g") \
        .config("spark.executor.memory", "8g") \
        .config("spark.sql.shuffle.partitions", "200") \
        .getOrCreate()

def train_recommender_pipeline():
    spark = create_spark_session()

    PATH_INTERACTIONS = "s3a://data/usage/user_game_interactions"
    PATH_OUT_RECOMMENDATIONS = "s3a://data/usage/steam_recommendations"

    print("📥 1. Chargement des interactions utilisateurs...")
    df_interactions = spark.read.parquet(PATH_INTERACTIONS)

    if df_interactions.rdd.isEmpty():
        print("Aucune donnée d'interaction à traiter.")
        return

    print("Agrégation des tags au niveau joueur et normalisation TF-IDF...")
    

    df_user_tags = df_interactions.groupBy("steam_id").agg(
        F.array_distinct(
            F.flatten(
                F.collect_list(
                    F.concat(
                        F.coalesce(F.col("genre_names"), F.array()),
                        F.coalesce(F.col("theme_names"), F.array()),
                        F.coalesce(F.col("tag_names"), F.array())
                    )
                )
            )
        ).alias("all_tags")
    )

    df_user_tags = df_user_tags.withColumn(
        "all_tags", 
        F.transform(F.col("all_tags"), lambda t: 
            F.regexp_replace(
                F.regexp_replace(F.lower(t), r'[^a-z0-9]', "_"), 
                r'_{2,}', "_" 
            )
        )
    )

    cv = CountVectorizer(inputCol="all_tags", outputCol="raw_features", minDF=5)
    model_cv = cv.fit(df_user_tags)
    df_vectorized = model_cv.transform(df_user_tags)

    idf = IDF(inputCol="raw_features", outputCol="features_raw")
    idf_model = idf.fit(df_vectorized)
    df_weighted = idf_model.transform(df_vectorized)

    normalizer = Normalizer(inputCol="features_raw", outputCol="features", p=2.0)
    df_scaled = normalizer.transform(df_weighted)


    num_clusters = 30
    print(f"Entraînement du modèle K-Means (K={num_clusters})...")
    
    kmeans = KMeans().setK(num_clusters).setSeed(42).setFeaturesCol("features").setPredictionCol("cluster_id")
    model = kmeans.fit(df_scaled)

    df_predictions = model.transform(df_scaled)
    df_players_clustered = df_predictions.select("steam_id", "cluster_id")
    evaluator = ClusteringEvaluator(
        predictionCol="cluster_id", 
        featuresCol="features", 
        metricName="silhouette", 
        distanceMeasure="cosine"
    )
    # Calculer le score
    silhouette_score = evaluator.evaluate(df_predictions)
    print(f"Score de Silhouette global du clustering : {silhouette_score:.4f}")

    print("Analyse et profilage dynamique des clusters...")
    
    ignored_naming_tags = [
    "open_world", "steam_trading_cards", "singleplayer", "multiplayer", 
    "indie", "action", "steam_achievements", "free_to_play", "adventure",
    "simulator", "simulation", "strategy", "rpg", "co_op", "casual", 
    "steam", "party", "early_access", "atmospheric", "2d", "3d"
    ]
    df_genre_cluster = df_user_tags.select(
        "steam_id",
        F.explode("all_tags").alias("feature_tag")
    ).join(df_players_clustered, on="steam_id", how="inner")

    total_players = df_players_clustered.count()
    df_tag_global_counts = df_genre_cluster.groupBy("feature_tag").agg(
        F.countDistinct("steam_id").alias("global_tag_users")
    ).withColumn("global_prop", F.col("global_tag_users") / total_players)

    df_cluster_sizes = df_players_clustered.groupBy("cluster_id").agg(
        F.count("steam_id").alias("cluster_size")
    )

    df_tag_cluster_counts = df_genre_cluster.groupBy("cluster_id", "feature_tag").agg(
        F.countDistinct("steam_id").alias("cluster_tag_users")
    )

    df_lift = df_tag_cluster_counts \
        .join(df_cluster_sizes, "cluster_id") \
        .join(df_tag_global_counts, "feature_tag") \
        .withColumn("cluster_prop", F.col("cluster_tag_users") / F.col("cluster_size")) \
        .withColumn("lift", F.col("cluster_prop") / F.col("global_prop"))


    MIN_GLOBAL_USERS = 150 

    df_lift_filtered = df_lift.filter(
        (F.col("global_tag_users") >= MIN_GLOBAL_USERS) &
        (~F.col("feature_tag").isin(ignored_naming_tags)) &
        (~F.col("feature_tag").rlike("(?i)controller|support|steam|remote|play|share|schermo|partida|partage|screen|speech|text|lang|audio|achievements")) &
        (F.col("cluster_prop") >= 0.15)
    )

    window_lift = Window.partitionBy("cluster_id").orderBy(F.col("lift").desc(), F.col("cluster_prop").desc())

    df_top_2_lift = df_lift_filtered \
        .withColumn("rank", F.row_number().over(window_lift)) \
        .filter("rank <= 2")

    df_cluster_names = df_top_2_lift.groupBy("cluster_id") \
        .agg(F.concat_ws(" & ", F.collect_list("feature_tag")).alias("dominant_features")) \
        .withColumn(
            "cluster_name", 
            F.concat(F.lit("Cluster "), F.col("cluster_id"), F.lit(" ("), F.col("dominant_features"), F.lit(" Fans)"))
        ).select("cluster_id", "cluster_name")

    df_players_clustered_final = df_players_clustered.join(df_cluster_names, on="cluster_id", how="left") \
        .na.fill({"cluster_name": "Unknown Cluster"})

    print("Calcul des recommandations...")
    
    df_interactions_with_cluster = df_interactions.join(df_players_clustered_final, on="steam_id", how="inner")
    
    df_cluster_score = df_interactions_with_cluster \
        .groupBy("cluster_id", "steam_appid", "game_name") \
        .agg(F.sum("implicit_preference_score").alias("cluster_score"))

    total_users = df_interactions.select("steam_id").distinct().count()
    df_game_stats = df_interactions.groupBy("steam_appid").agg(
        F.count("steam_id").alias("user_count"),
        F.sum("implicit_preference_score").alias("global_score")
    ).filter(F.col("user_count") < (total_users * 0.20))

    df_game_popularity_by_cluster = df_cluster_score.join(df_game_stats, on="steam_appid") \
    .filter(F.col("cluster_score") > 10.0) \
    .withColumn(
        "game_cluster_score",
        F.pow(F.col("cluster_score"), 1.2) / F.log1p(F.col("global_score"))
    )

    window_popularity = Window.partitionBy("cluster_id").orderBy(F.col("game_cluster_score").desc())
    df_top_games_by_cluster = df_game_popularity_by_cluster \
        .withColumn("rank", F.row_number().over(window_popularity)) \
        .filter("rank <= 30") \
        .select("cluster_id", "steam_appid", "game_name", "game_cluster_score")

    print("Filtrage final et structuration...")
    
    df_player_reco_candidates = df_players_clustered_final.join(df_top_games_by_cluster, on="cluster_id", how="inner")

    df_recommendations_filtered = df_player_reco_candidates.join(
        df_interactions.select(F.col("steam_id").alias("owned_user"), F.col("steam_appid").alias("owned_appid")),
        (df_player_reco_candidates.steam_id == F.col("owned_user")) & 
        (df_player_reco_candidates.steam_appid == F.col("owned_appid")),
        how="left_anti"
    )

    window_reco_final = Window.partitionBy("steam_id").orderBy(F.col("game_cluster_score").desc())
    df_reco_final = df_recommendations_filtered \
        .withColumn("reco_rank", F.row_number().over(window_reco_final)) \
        .filter("reco_rank <= 10")

    df_es_ready = df_reco_final.groupBy("steam_id", "cluster_id", "cluster_name").agg(
        F.collect_list("game_name").alias("recommended_games"),
        F.collect_list(F.round("game_cluster_score", 2)).alias("recommendation_scores")
    )

    df_es_ready.write.mode("overwrite").parquet(PATH_OUT_RECOMMENDATIONS)
    
    print(f"Pipeline ML exécuté avec succès. {df_es_ready.count()} joueurs segmentés et recommandés.")
    df_es_ready.show(5, truncate=False)
    spark.stop()

if __name__ == "__main__":
    train_recommender_pipeline()