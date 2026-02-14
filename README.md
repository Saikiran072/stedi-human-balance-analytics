# 🧍‍♂️ STEDI Human Balance Analytics

### Spark & AWS Glue Data Lakehouse Project

---

## 📌 Project Introduction

The STEDI Human Balance Analytics project focuses on building a data lakehouse solution using AWS Glue and Apache Spark. The objective is to process raw sensor data, curate trusted datasets, and prepare step trainer data that can be used for machine learning applications.


---

## 📌 Project Overview

This project implements a **Data Lakehouse solution on AWS** for STEDI — a hardware company that produces a Step Trainer device used for balance training.

As a Data Engineer on the STEDI team, the objective is to:

- Ingest sensor and mobile app data into AWS S3
- Build a structured data lake using AWS Glue & Spark
- Transform raw data into trusted and curated datasets
- Prepare privacy-compliant machine learning training data

The final curated dataset enables Data Scientists to train a machine learning model that accurately detects steps in real-time.

---

## 🏗️ Architecture Overview

The project follows a **multi-layered Data Lake architecture**:

Landing Zone (Raw Data - S3)  
 ↓  
Trusted Zone (Cleaned & Validated - S3)  
 ↓  
Curated Zone (ML Ready Data - S3)

---

## 🛠️ Technologies Used

- AWS S3
- AWS Glue
- Apache Spark (PySpark)
- AWS IAM
- AWS Lake Formation
- Glue Data Catalog

---

## 📂 Dataset Description

STEDI produces three types of data:

### 1️⃣ Customer Data

Collected from the mobile application.

Includes:

- Customer name
- Email
- Phone
- Registration details
- Research consent flag

Only customers who agreed to share data for research are used for ML training.

---

### 2️⃣ Accelerometer Data

Collected from the mobile device sensors.

Includes:

- Timestamp
- X, Y, Z acceleration values
- User email

---

### 3️⃣ Step Trainer Sensor Data

Collected from the STEDI Step Trainer hardware.

Includes:

- Timestamp
- Distance measurement
- Device sensor readings

---

## 🔄 ETL Pipeline Implementation

AWS Glue Jobs were created to transform data across zones.

---

### 🥇 1. Customer Landing → Trusted

Script: `customer_landing_to_trusted.py`

- Filters customers who agreed to share research data
- Removes invalid records
- Writes clean data to Trusted zone

---

### 🥈 2. Accelerometer Landing → Trusted

Script: `accelerometer_landing_to_trusted.py`

- Joins accelerometer data with trusted customers
- Ensures only consented users’ data is retained
- Stores clean data in Trusted zone

---

### 🥉 3. Step Trainer Landing → Trusted

Script: `step_trainer_to_trusted.py`

- Cleans raw step trainer sensor data
- Removes corrupt or incomplete records
- Stores validated sensor data

---

### 🧠 4. Customer Trusted → Curated

Script: `customer_trusted_to_curated.py`

- Prepares customer dataset for analytical queries
- Optimizes schema for performance

---

### 🤖 5. ML Curated Dataset Creation

Script: `ml_curated.py`

- Joins:
  - Trusted Accelerometer Data
  - Trusted Step Trainer Data
  - Curated Customer Data

- Produces ML-ready dataset
- Ensures:
  - Only research-consented users included
  - Data is privacy compliant
  - Ready for machine learning training

---

## 🔐 Privacy & Data Governance

Privacy is a primary requirement in this project.

- Only customers who explicitly agreed to research sharing are included
- Non-consented users are excluded at the trusted layer
- Data is separated into zones to prevent accidental misuse

---

## 📊 Data Lake Zones Explained

| Zone    | Purpose                              |
| ------- | ------------------------------------ |
| Landing | Raw ingested data from S3            |
| Trusted | Cleaned & validated datasets         |
| Curated | Business-ready and ML-ready datasets |

---

## 🎯 Project Outcome

After completing the ETL pipeline:

- Clean, structured data lakehouse built on AWS
- Privacy-compliant ML training dataset created
- Sensor data curated for machine learning model development
- Scalable Spark-based transformation pipeline implemented
- Data Scientists can now train real-time step detection models

---

## 💡 Key Learnings

Through this project, I gained hands-on experience in:

- Building Data Lake architecture on AWS
- Writing AWS Glue ETL jobs using PySpark
- Implementing multi-layer data transformations
- Performing secure data filtering for privacy compliance
- Joining large datasets efficiently in Spark
- Designing ML-ready curated datasets
