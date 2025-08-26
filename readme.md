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
