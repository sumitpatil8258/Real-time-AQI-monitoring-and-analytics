# 🌍 Real-Time AQI Monitoring and Analytics Using Big Data Technologies

## 📖 Project Overview

This project implements a real-time Air Quality Index (AQI) monitoring pipeline using Apache Kafka, Apache Spark Structured Streaming, MySQL, Docker, and Power BI.

AQI data is read from a CSV dataset, streamed through Kafka, processed in real time using Spark Structured Streaming, stored in a MySQL database, and visualized in Power BI.

---

# 🏗️ System Architecture

```
                +------------------+
                | AQI_dataset.csv  |
                +--------+---------+
                         |
                         |
                 Kafka Producer
                    (Python)
                         |
                         ▼
                +------------------+
                |  Apache Kafka    |
                |   aqi-topic      |
                +--------+---------+
                         |
                         ▼
        Spark Structured Streaming
                         |
           Data Cleaning & Processing
                         |
                         ▼
                +------------------+
                |      MySQL       |
                |     aqi_db       |
                +--------+---------+
                         |
                         ▼
                +------------------+
                |    Power BI      |
                |    Dashboard     |
                +------------------+
```

---

# 📂 Project Structure

```text
AQI_Project/
│
├── Dockerfile
├── docker-compose.yml
├── requirements.txt
│
├── dashboard/
│   └── PowerBI.pbix
│
├── data/
│   └── AQI_dataset.csv
│
├── mysql/
│   └── create_table.sql
│
├── producer/
│   └── kafka_producer.py
│
└── spark/
    ├── spark_streaming.py
    └── jars/
        └── mysql-connector-j-9.0.0.jar
```

---

# 🛠️ Technologies Used

- Python 3.11
- Apache Kafka 3.7
- Apache Spark 3.5.2
- PySpark
- MySQL 8.4
- Docker
- Docker Compose
- Power BI Desktop

---

# 📋 Prerequisites

Install the following before running the project:

- Docker Desktop
- WSL2 (Ubuntu)
- Power BI Desktop
- Git
- Python 3.11 

---

# 🚀 Running the Project

## Step 1: Open WSL

```bash
wsl
```

---

## Step 2: Navigate to the project

```bash
cd ~/AQI_Project
```

---

## Step 3: Verify project structure

```bash
tree
```

---

## Step 4: Start Docker Desktop

Verify Docker:

```bash
docker ps
```

---

## Step 5: Start all containers

```bash
docker compose up -d
```

---

## Step 6: Verify containers

```bash
docker ps
```

Expected containers:

```
mysql
kafka
spark-master
spark-worker
```

---

## Step 7: Verify Spark

Open

```
http://localhost:8080
```

Expected:

- Spark Master Alive
- Spark Worker Connected

---

## Step 8: Verify MySQL

```bash
docker exec -it mysql mysql -u aqi_user -paqi_pass
```

```sql
USE aqi_db;

SHOW TABLES;

SELECT COUNT(*) FROM aqi_data;
```

Exit

```sql
exit;
```

---

# ▶ Start Spark Streaming

```
cd spark
./start_consumer.sh
```

Leave this terminal running.

---

# ▶ Run Kafka Producer

Open another WSL terminal.

```bash
cd ~/AQI_Project
```
Run:

```bash
docker compose run --rm producer
```

If above gives ERROR 

```docker compose up --build producer
docker compose up producer
```

Expected:

```
Sending records to Kafka...

All records sent successfully.
```

---

# Verify Spark Streaming

Spark should continuously display batches:

```
Batch 1

Batch 2

Batch 3
```

---

# Verify Data in MySQL

```bash
docker exec -it mysql mysql -u aqi_user -paqi_pass
```

```sql
USE aqi_db;

SELECT * FROM aqi_data LIMIT 10;

SELECT COUNT(*) FROM aqi_data;
```

---

# 📊 Power BI Dashboard

Open

```
dashboard/PowerBI.pbix
```

Connect to MySQL using


Server

```
localhost:3307
```

Database

```
aqi_monitoring
```

username --> select authentication --> database

```
root
```

password 

```
root
```

Refresh

```
Home
    ↓
Refresh
```

# Useful Docker Commands

## Check containers

```bash
docker ps
```

---

## Stop project

```bash
docker compose down
```

---

## Restart project

```bash
docker compose down

docker compose up -d
```

---

## Kafka logs

```bash
docker logs kafka --tail 50
```

---

## Spark Master logs

```bash
docker logs spark-master --tail 50
```

---

## Spark Worker logs

```bash
docker logs spark-worker --tail 50
```

---

## MySQL logs

```bash
docker logs mysql --tail 50
```

---

## Verify MySQL records

```bash
docker exec -it mysql mysql -u aqi_user -paqi_pass
```

```sql
USE aqi_db;

SELECT COUNT(*) FROM aqi_data;

SELECT * FROM aqi_data LIMIT 5;
```

---

# 📈 Future Improvements

- Apache Airflow for workflow scheduling
- Real-time dashboard auto-refresh
- AWS S3 Data Lake integration
- Historical AQI analytics
- AQI prediction using Machine Learning
- Kubernetes deployment
- Grafana monitoring

---

# 👤 Author

**Sumit Patil & Rohan Ghotane**

**Project:** Real-Time AQI Monitoring and Analytics Using Big Data Technologies

---

# 📄 License

This project is intended for educational and learning purposes.
