# ⚽ Football Data Warehouse for Match Analysis & Scouting

![Project Status](https://img.shields.io/badge/Status-Completed-success)
![University](https://img.shields.io/badge/HUST-School_of_Applied_Mathematics_and_Informatics-red)

## 📖 Overview
This project involves designing and implementing a comprehensive **Data Warehouse (DW)** system tailored for football analytics. The system ingests raw event data from **StatsBomb**, processes it through an ETL pipeline, and creates a structured storage solution to support tactical analysis, player performance evaluation, and scouting.

Using the **FC Barcelona** (La Liga) dataset as a case study, the project demonstrates how to transform semi-structured JSON data into actionable insights using a Star Schema architecture.

## 🏗 System Architecture
The system follows a modern data pipeline architecture composed of four main layers:

1.  **Data Source**: Open data from StatsBomb (JSON format).
2.  **Staging (Data Lake)**: **MinIO** is used for object storage to hold raw and intermediate data.
3.  **Data Warehouse**: **Apache Spark** performs ETL/ELT processes to flatten and transform data, which is then loaded into **PostgreSQL**.
4.  **Analytics & BI**: **Microsoft PowerBI** connects to the DW to visualize key metrics (xG, PPDA, Heatmaps).

## 🛠 Tech Stack
The entire system is containerized using **Docker** for consistency and easy deployment.

* **Orchestration:** Apache Airflow
* **Data Processing:** Apache Spark (PySpark)
* **Storage (Data Lake):** MinIO
* **Data Warehouse:** PostgreSQL
* **Visualization:** Microsoft PowerBI
* **Language:** Python, SQL

## 🗄 Data Modeling (Star Schema)
The Data Warehouse is designed using a **Star Schema** optimized for analytical queries.

### Fact Tables
* **`fact_event`**: Granular details of every on-ball action (pass, shot, dribble, etc.) with coordinates.
* **`fact_player_match_stats`**: Aggregated stats per player per match (goals, assists, xG, xA, passes).
* **`fact_team_match_stats`**: Team-level metrics like Possession %, PPDA, Goals Conceded.
* **`fact_player_season_stats`**: Long-term performance metrics for scouting.

### Dimension Tables
* **`dim_player` & `dim_team`**: Handles **SCD Type 2** (Slowly Changing Dimensions) to track transfers and manager changes over time.
* **`dim_match`**, **`dim_date`**, **`dim_location`**, **`dim_event_type`**.

## 📊 Key Analytics Features
The system calculates advanced football metrics not present in the raw data:
* **xG (Expected Goals):** Measuring the quality of a shot.
* **PPDA (Passes Per Defensive Action):** Measuring pressing intensity.
* **Heatmaps:** Visualizing player activity zones.
* **Possession-Adjusted Stats:** Normalizing defensive metrics based on ball possession.

## 🔄 Airflow DAGs
The pipeline is automated via Airflow with the following task dependencies:

1.  **Ingest:** Fetch JSON files from StatsBomb GitHub -> Upload to MinIO.
2.  **Dim Processing:** Spark jobs process dimensions (Parallel execution).
3.  **Fact Processing:** Spark jobs create `fact_event` (Lookup IDs from Dims).
4.  **Aggregation:** Calculate derived stats for Players/Teams.

## 🚀 Getting Started

### Prerequisites
* Docker & Docker Compose
* PowerBI Desktop (for viewing reports)

### Installation
1.  **Clone the repository:**
    ```bash
    git clone [https://github.com/your-username/football-data-warehouse.git](https://github.com/your-username/football-data-warehouse.git)
    cd football-data-warehouse
    ```

2.  **Start the services:**
    ```bash
    docker-compose up -d
    ```
    *This will start Airflow Webserver, Scheduler, Spark Master/Worker, MinIO, and PostgreSQL.*

3.  **Access the interfaces:**
    * **Airflow:** `http://localhost:8080`
    * **MinIO:** `http://localhost:9001`
    * **Spark Master:** `http://localhost:8081`

4.  **Trigger the Pipeline:**
    * Go to Airflow UI and trigger the `ingest_full_laliga_statsbomb` DAG.

## 📈 Dashboard Screenshots
*(Add your screenshots here based on the report)*
* **Match Analysis:** Shot maps, pass networks, defensive actions.
* **Player Analysis:** Performance vs. Expected metrics (xG/xA).
* **Opponent Analysis:** Team strengths/weaknesses and formation analysis.

## 👨‍💻 Author
**Nguyen Phu Vinh**
* **Student ID:** 20227169
* **Class:** Mathematics-Informatics 01 K67
* **Institution:** Hanoi University of Science and Technology

---
*Based on the Project Report: "Building a Data Warehouse for Football Analysis" (2025).*