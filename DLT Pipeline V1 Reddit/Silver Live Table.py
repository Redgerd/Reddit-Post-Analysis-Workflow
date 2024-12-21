# Databricks notebook source
# MAGIC %sql
# MAGIC USE CATALOG big_data_analytics_v;
# MAGIC USE SCHEMA big_data_analytics_sesssion_v;
# MAGIC CREATE OR REFRESH LIVE TABLE silver_reddit_layer
# MAGIC COMMENT "Transformed and filtered Reddit posts data for analysis with enforced uniqueness and constraints."
# MAGIC TBLPROPERTIES ("quality" = "silver")
# MAGIC AS
# MAGIC SELECT
# MAGIC     post_id,
# MAGIC     title,
# MAGIC     description,
# MAGIC     subreddit,
# MAGIC     author,
# MAGIC     score,
# MAGIC     CAST(unix_timestamp(created_utc) AS TIMESTAMP) AS created_at,
# MAGIC     url,
# MAGIC     (url IS NOT NULL) AS has_url,
# MAGIC     created_utc,
# MAGIC     HOUR(CAST(unix_timestamp(created_utc) AS TIMESTAMP)) AS hour,
# MAGIC     WEEKOFYEAR(CAST(unix_timestamp(created_utc) AS TIMESTAMP)) AS week,
# MAGIC     COUNT(post_id) OVER (PARTITION BY author, subreddit) AS post_count,
# MAGIC     AVG(score) OVER (PARTITION BY author, subreddit) AS avg_score,
# MAGIC     ROW_NUMBER() OVER (PARTITION BY subreddit ORDER BY score DESC) AS rank
# MAGIC FROM bronze_reddit_posts;
# MAGIC