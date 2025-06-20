# 🏗️ Build Data Warehouse with Airflow & Python for E-Commerce

🚀 This project delivers an end-to-end **ELT data pipeline** for an eCommerce platform. The pipeline efficiently **extracts**, **loads**, and **transforms** sales data into a PostgreSQL data warehouse, then visualizes actionable insights through **Power BI**.

---

## ⚙️ Key Technologies & Architecture

- 🐍 **Python** – Used to build custom scripts to extract data from CSV files and MySQL databases.
- 🐘 **PostgreSQL** – Acts as both the **staging area** and **data warehouse**.
- 🌬️ **Apache Airflow** – Orchestrates the ELT pipeline from extraction to transformation.
- 🐳 **Docker** – Ensures consistent development and deployment environments.
- 📊 **Power BI** – Visualizes data and delivers insightful sales dashboards.

---

## 📌 Project Workflow Overview

> The diagram below shows the overall pipeline from data ingestion to visualization:

![Workflow Diagram](https://github.com/user-attachments/assets/6eaf6963-8272-437a-9bdf-1b57bfc538e2)

![Architecture](https://github.com/user-attachments/assets/45bd11be-7501-4d12-b33a-47399a763518)

---

## 🌀 Airflow DAGs

> Apache Airflow orchestrates the entire data pipeline, ensuring dependencies are met and scheduling is handled.

![Airflow DAG](https://github.com/user-attachments/assets/6cb80da3-b49c-4dc6-a5e7-ddad4472e1e4)

---

## 📊 Dashboard with Power BI

> Interactive and insightful visualizations created in Power BI for business users to explore and analyze sales performance.

![Dashboard 1](https://github.com/user-attachments/assets/1ddd3953-787f-4ca9-919a-51743c454e5a)  
![Dashboard 2](https://github.com/user-attachments/assets/7358654c-68c4-4ebb-bb2b-890367772cd9)  
![Dashboard 3](https://github.com/user-attachments/assets/d60323e9-591b-4b2c-986c-671b87707894)

---

## 🎯 Project Goals

- Automate the ELT process using Apache Airflow  
- Store and transform structured sales data in a PostgreSQL data warehouse  
- Provide business intelligence through Power BI dashboards  
- Containerize the pipeline for reproducibility and deployment with Docker  

---


