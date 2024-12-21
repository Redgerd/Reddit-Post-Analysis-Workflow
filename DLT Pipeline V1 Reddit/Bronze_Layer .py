# Databricks notebook source
pip install praw

# COMMAND ----------

from pyspark.sql import SparkSession
from datetime import datetime
import praw
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, TimestampType

# Reddit API credentials
reddit = praw.Reddit(
    client_id="Cb9XMUcLMOnhnSO7JyHOSw",
    client_secret="XZlHL-BNgvaVJYasA6lG0DjFbqXbmA",
    user_agent="Delta Live pipeline"
)

# Fetch subreddit data
def fetch_subreddit_data(subreddit_name="pakistan", limit=100):
    posts = []
    subreddit = reddit.subreddit(subreddit_name)
    for post in subreddit.hot(limit=limit):
        posts.append({
            "post_id": post.id,
            "title": post.title,
            "description": post.selftext if post.selftext else None,
            "subreddit": subreddit_name,
            "author": str(post.author),
            "score": post.score,
            "created_utc": datetime.utcfromtimestamp(post.created_utc),
            "url": post.url
        })
    return posts

# Create Spark session
spark = SparkSession.builder \
    .appName("Reddit Bronze Layer") \
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
    .getOrCreate()

# Fetch raw data
raw_data = fetch_subreddit_data(subreddit_name="pakistan", limit=100)

# Define schema
schema = StructType([
    StructField("post_id", StringType(), True),
    StructField("title", StringType(), True),
    StructField("description", StringType(), True),
    StructField("subreddit", StringType(), True),
    StructField("author", StringType(), True),
    StructField("score", IntegerType(), True),
    StructField("created_utc", TimestampType(), True),
    StructField("url", StringType(), True)
])

# Create DataFrame
df = spark.createDataFrame(raw_data, schema=schema)

# Define path for the bronze layer folder in DBFS (Databricks File System)
bronze_layer_path = "dbfs:/mnt/big_data_analytics_v/big_data_analytics_sesssion_v/volume_reddit/bronze_reddit_posts"

# Save the DataFrame as Delta format in the bronze layer folder
df.write.format("delta").mode("overwrite").save(bronze_layer_path)

# COMMAND ----------

