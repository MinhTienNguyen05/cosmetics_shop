# E-Commerce Data Analytics Platform: Streaming Simulation & Business Optimization

![Apache Kafka](https://img.shields.io/badge/Apache_Kafka-231F20?style=for-the-badge&logo=apache-kafka&logoColor=white)
![Go](https://img.shields.io/badge/Go-00ADD8?style=for-the-badge&logo=go&logoColor=white)
![Apache Airflow](https://img.shields.io/badge/Airflow-017CEE?style=for-the-badge&logo=Apache%20Airflow&logoColor=white)
![Databricks](https://img.shields.io/badge/Databricks-FF3621?style=for-the-badge&logo=Databricks&logoColor=white)
![PySpark](https://img.shields.io/badge/PySpark-E25A1C?style=for-the-badge&logo=Apache-Spark&logoColor=white)
![Power BI](https://img.shields.io/badge/Power_BI-F2C811?style=for-the-badge&logo=PowerBI&logoColor=black)

## Executive summary
This project is an end-to-end **Data Engineering and Analytics** solution built for a high-traffic cosmetics e-commerce platform. It features a custom-built streaming engine that simulates real-world event generation, ingesting **>12 million historical event logs** through a strict Medallion Architecture on Databricks. 

Beyond building a scalable pipeline, this project bridges the gap between raw data and **Actionable Business Insights**—successfully addressing bot traffic mitigation, checkout funnel bottlenecks, and B2B customer segmentation.
---
## Core Business Impacts & Insights
* ** Bot & Spam Mitigation: Engineered a Dynamic Blacklisting algorithm using left_anti joins in PySpark. Effectively isolated and filtered out extensive malicious bot traffic (e.g., sessions with >100 clicks/sec) at the Silver layer, ensuring high data purity for downstream financial reporting.
* ** Funnel Optimization:** Uncovered a critical revenue leak at the checkout stage (3M Add-to-Carts translated to only 270K Orders). Identified high shipping costs relative to AOV (~$12) as the root cause and proposed a threshold-based "Freeship" strategy.
* ** B2B Wholesale Discovery (RFM):** Built an RFM segmentation model that detected a hidden cluster of "VIP" wholesale buyers (Spa & Salon owners). Despite being a small segment, they demonstrated a **32% conversion rate** (2.5x the platform average) and an **82.1% view-to-order rate**, validating the need for a dedicated B2B portal and 1-on-1 Telesales pipeline.

---

## Technical Architecture

The system relies on a containerized streaming engine and a Databricks-powered Medallion Pipeline, orchestrated by Apache Airflow.

### 1. Simulated Streaming Engine (`Go` & `Kafka`)
* A custom high-performance Kafka Producer written in **Go** (`main.go`) designed to simulate an active e-commerce backend.
* Streams historical CSV event logs into JSON payloads, publishing them sequentially to the `ecommerce_events` Kafka topic with a controlled batch size and checkpointing mechanism.
* Fully containerized environment orchestrated via `docker-compose.yaml`.

### 2. Medallion Data Pipeline (`Databricks` & `PySpark`)
* ** Bronze Layer (`bronze.ipynb` / Airflow DAG):** Ingests raw JSON streams from Kafka into Delta Lake. Preserves the exact state of the source data with append-only semantics.
* ** Silver Layer (`silver.ipynb`):** Applies robust data quality rules. Casts data types, imputes missing values (e.g., assigning `unbranded`), and filters out negative prices and bot-generated sessions.
* ** Gold Layer (`gold.ipynb`):** Transforms the cleansed data into a highly optimized **Star Schema**.
  * **Dimension Tables:** `dim_user`, `dim_product`, `dim_date`.
  * **Fact Tables:** `fact_daily_performance`, `fact_user_funnel`, `fact_rfm_segmentation`.
<img width="557" height="426" alt="image" src="https://github.com/user-attachments/assets/2061896c-b9cb-4907-9eed-fee2dcada3f1" />


### 3. Orchestration (`Airflow`)
* Automated workflow scheduling using **Apache Airflow**.
* `bronze_ingestion_dag.py` triggers the Kafka-to-Bronze loaders, ensuring seamless and fault-tolerant data ingestion.

---

## Repository Structure

```text
├── airflow/                             # Workflow Orchestration
│   ├── dags/
│   │   ├── bronze_ingestion_dag.py      # Main DAG for data ingestion
│   │   └── scripts/
│   │       └── kafka_to_bronze_loader.py# Kafka consumer logic
│   └── requirements.txt
├── databricks_processing/               # Medallion Architecture Logic
│   ├── bronze.ipynb                     # Raw ingestion & landing
│   ├── silver.ipynb                     # Data cleansing & Bot filtering
│   └── gold.ipynb                       # Star Schema Data Marts
├── local_streaming_engine/              # Data Generation
│   ├── producer/
│   │   ├── main.go                      # Go-based Kafka Producer
│   │   ├── go.mod
│   │   └── go.sum
│   └── tests/
│       └── test_auth.py                 # Databricks API Authentication Tests
├── docker-compose.yaml                  # Infrastructure provisioning
└── README.md

```

## Semantic Model & BI Strategy (Power BI)

* **Performance Optimized:** Leveraged a strict 1-to-Many, Single-Direction cross-filtering model (Dim ➔ Fact) to maximize performance on large datasets.
* **Dynamic DAX Measures:** Used DAX for dynamic aggregations (e.g., `AOV = DIVIDE(SUM(GMV), SUM(Orders))`) instead of hard-coded columns, enabling stakeholders to drill down securely by Brand, Category, and Time.

---
