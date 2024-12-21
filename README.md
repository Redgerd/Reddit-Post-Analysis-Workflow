# Reddit-to-Delta-Live

![Python](https://img.shields.io/badge/python-%233776AB.svg?style=for-the-badge&logo=python&logoColor=white) 
![Azure Databricks](https://img.shields.io/badge/Azure%20Databricks-%23FF6600.svg?style=for-the-badge&logo=azuredatabricks&logoColor=white) 
![PySpark](https://img.shields.io/badge/PySpark-%23E26A00.svg?style=for-the-badge&logo=apache-spark&logoColor=white)

## Overview

The **Reddit-to-Delta-Live** project ingests data from Reddit in near real-time and processes it through a Delta Live pipeline. The raw Reddit data is ingested into the **Bronze Layer**, enriched in the **Silver Layer**, and transformed into the final **Gold Layer**, which is stored in Delta format for further analysis. The pipeline is designed to handle large volumes of data efficiently and ensure that the data is processed, cleaned, and enriched for downstream use.

## Project Architecture

- **Bronze Layer**: Raw data ingestion from Reddit via API. This layer stores unprocessed data, such as post ID, title, author, score, and creation timestamp in Delta tables.
- **Silver Layer**: Data refinement and cleaning. Duplicates are removed, missing values are handled, and the data is transformed and enriched using sentiment and emotion analysis.
- **Gold Layer**: Final processed data, including sentiment and emotion features, stored in Delta format, ready for analysis or reporting.

## Data Flow

1. **Data Ingestion**: Reddit posts are ingested via the Reddit API and stored in the Bronze Layer as raw data in Delta format.
2. **Data Transformation**: In the Silver Layer, the data is cleaned and transformed. This includes handling missing data, removing duplicates, and performing aggregations.
3. **Enrichment**: Sentiment and emotion analysis is performed on Reddit post titles and descriptions, and features like TF-IDF are extracted.
4. **Final Output**: The cleaned and enriched data is stored in the Gold Layer in Delta format, ready for further analysis.

## Technologies Used

- **Python**: For data processing and transformation.
- **Azure Databricks**: For managing and running Spark-based workflows in the cloud.
- **PySpark**: For distributed data processing and transformation.

![WhatsApp Image 2024-12-21 at 19 51 03_b606a2c6](https://github.com/user-attachments/assets/2aed2924-5c44-4310-9f3e-93389d70d356)




