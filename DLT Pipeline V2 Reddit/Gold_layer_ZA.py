# Databricks notebook source
pip install NRCLex

# COMMAND ----------

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, length, udf, when, count, avg, max, min, row_number
from pyspark.sql.types import StringType, FloatType, IntegerType
from pyspark.sql.window import Window
from textblob import TextBlob
import dlt

# Create Spark Session
spark = SparkSession.builder \
    .appName("Enhanced Reddit Gold Layer") \
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
    .getOrCreate()

# COMMAND ----------

# --- Sentiment Analysis Functions ---
def calculate_sentiment_polarity(text):
    if text:
        blob = TextBlob(text)
        return blob.sentiment.polarity
    return 0.0

def classify_sentiment(polarity):
    if polarity > 0.5:
        return "Very Positive"
    elif polarity > 0.1:
        return "Positive"
    elif polarity < -0.5:
        return "Very Negative"
    elif polarity < -0.1:
        return "Negative"
    else:
        return "Neutral"
    
# Register UDFs
udf_calculate_sentiment_polarity = udf(calculate_sentiment_polarity, FloatType())
udf_classify_sentiment = udf(classify_sentiment, StringType())

# COMMAND ----------

# --- Step 1: Silver Layer ---
@dlt.table(
    name="silver_reddit_layer",
    comment="Silver Layer Data for Reddit posts"
)
def silver_reddit_layer():
    return spark.read.format("delta").load("dbfs:/mnt/big_data_analytics_v/big_data_analytics_sesssion_v/tables/silver_reddit_layer")

# --- Step 2: Intermediate Gold Layer Transformation ---
@dlt.view(
    name="gold_reddit_layer_transformed",
    comment="Transformed data with sentiment analysis and additional metrics"
)
def gold_reddit_layer_transformed():
    silver_df = dlt.read("silver_reddit_layer")
    
    # Apply transformations: Sentiment Analysis and Additional Metrics
    transformed_df = silver_df \
        .withColumn("title_polarity", udf_calculate_sentiment_polarity(col("title"))) \
        .withColumn("title_sentiment", udf_classify_sentiment(col("title_polarity"))) \
        .withColumn("description_polarity", udf_calculate_sentiment_polarity(col("description"))) \
        .withColumn("description_sentiment", udf_classify_sentiment(col("description_polarity"))) \
        .withColumn("title_length", length(col("title"))) \
        .withColumn("description_length", length(col("description")))
    
    return transformed_df


# COMMAND ----------


# --- Step 3: Gold Layer with Final Schema ---
@dlt.table(
    name="gold_reddit_layer",
    comment="Gold Layer with enhanced Reddit post data"
)
def gold_reddit_layer():
    transformed_df = dlt.read("gold_reddit_layer_transformed")
    
    # Select only required columns for the final Gold Layer
    gold_df = transformed_df.select(
        "post_id",
        "title",
        "description",
        "subreddit",
        "author",
        "score",
        "created_at",
        "url",
        "has_url",
        "hour",
        "week",
        "title_polarity",
        "title_sentiment",
        "description_polarity",
        "description_sentiment",
        "title_length",
        "description_length"
    )
    return gold_df
