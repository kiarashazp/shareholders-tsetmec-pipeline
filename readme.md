# 📊 Overview

This project is an end-to-end **ETL pipeline** that fetches shareholder data from the **TSETMC API**, transforms it into a normalized, flat schema, writes interim **CSVs**, and upserts the data into a **PostgreSQL** database.  
The workflow is orchestrated with **Apache Airflow** ⏳ and packaged via **Docker Compose** 🐳.

## 🔑 Key points

- **📡 Source**  
  TSETMC Shareholder API (`https://cdn.tsetmc.com/api/Shareholder/{symbol}/{date}`).


- **📅 Dates**  
  Last **10 working Jalali days** (skipping Thu/Fri and official holidays via an external calendar API), converted to **Gregorian `YYYYMMDD`**.


- **🗄️ Data model**  
  Three normalized tables with time-series friendly indexes:  
  - `symbols`  
  - `holders`  
  - `holdings_daily`
  

- **🛡️ Parsing & quality**  
  - Defensive parsing on CSV load  
  - Robust numeric normalization for `shares` and `percentage` (comma cleanup + sensible defaults)  

## 📂 Repository layout
- **🏗️ `models.py`**  
  SQLAlchemy ORM models (`Symbol`, `Holder`, `HoldingDaily`).


- **⚙ `database.py`️**  
  SQLAlchemy `engine` + `SessionLocal` configured from env.


- **🏁 `initialize_database.py`**  
  Creates tables from `Base.metadata`.


- **🔄 `etl_tasks.py`**  
  Airflow `@task` functions:  
  - 📥 Symbols read  
  - 📅 Date generation  
  - 🔗 Combinations  
  - 🌐 API fetch  
  - 📝 CSV write  
  - 🗄️ Postgres upsert  
  - 🧹 Cleanup  

  ➡️ All tasks accept/return simple **str/dict lists**.

 
- **📌 `dags/tsetmec_history_dag.py`**  
  The DAG definition (**daily schedule**).

### 📝 Notes
At the beginning of each module/method there is a **docstring** providing additional explanations.  
Refer to them for deeper understanding.  

##  🚀 Getting Started Guide

This guide explains how to set up and run the ETL pipeline project with **Postgres**, **pgAdmin**, and **Airflow UI** using Docker Compose.

##  🔑 Setup environment
    POSTGRES_AIRFLOW_USER=
    POSTGRES_AIRFLOW_PASSWORD=
    POSTGRES_AIRFLOW_DB=
    POSTGRES_ETL_USER=
    POSTGRES_ETL_PASSWORD=
    POSTGRES_ETL_DB=
    AIRFLOW__CORE__FERNET_KEY=your_fernet_key_here
    AIRFLOW_ADMIN_USERNAME=
    AIRFLOW_ADMIN_PASSWORD=
    PGADMIN_EMAIL=
    PGADMIN_PASSWORD=
    SHARED_DIR=
    prefix_csv_shareholders=

## ▶️ Start the stack
docker-compose up -d. **This will start**:

    Postgres (Airflow metadata)
    Postgres (ETL target DB)
    pgAdmin
    etl-init
    Airflow Init services
    Airflow Scheduler
    Airflow Webserver

## 🌐 Access the services
    pgAdmin → http://localhost:8081
    Login with:
    Email: PGADMIN_EMAIL
    Password: PGADMIN_PASSWORD

    Airflow UI → http://localhost:8080 (after 2 min from up docker)
    Login with:
    Username: AIRFLOW_ADMIN_USERNAME
    Password: AIRFLOW_ADMIN_PASSWORD

## ➡️ Add a new server connection in pgAdmin:

    Name: postgres-etl 
    Host: postgres-etl
    Port: 5432
    User: POSTGRES_ETL_USER
    Password: POSTGRES_ETL_PASSWORD
