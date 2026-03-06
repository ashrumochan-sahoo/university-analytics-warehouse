# University Academic Analytics Data Warehouse

A production-style data warehouse implementation using **Microsoft SQL Server** and **Python**, featuring a **snowflake schema**, **SCD Type 2** for historical tracking, **incremental batch loading**, and **advanced OLAP queries** with window functions.

---

## 🎯 Project Overview

This project demonstrates enterprise-level data warehousing skills by building a complete academic analytics system that:
- Consolidates student, course, instructor, and enrollment data
- Tracks historical changes using Slowly Changing Dimensions (Type 2)
- Supports incremental semester-based data loading
- Enables complex analytical queries for enrollment trends, performance metrics, and resource utilization

**Built for:** Data Engineer, Data Analyst, BI Developer roles  
**Tech Stack:** SQL Server 2022, Python 3.9, DBeaver, Docker, Git

---

## 📊 Architecture

### Data Flow
```
CSV Files → Python Ingestion → Staging Tables → SQL Transformations → Snowflake Schema → OLAP Queries
```

### Snowflake Schema Design
```
                    ┌─────────────────┐
                    │ dim_department  │
                    └────────┬────────┘
                             │
              ┌──────────────┴──────────────┐
              ↓                             ↓
     ┌────────────────┐            ┌────────────────┐
     │  dim_course    │            │ dim_instructor │
     └───────┬────────┘            └───────┬────────┘
             │                             │
             │        ┌────────────┐       │
             └───────→│            │←──────┘
                      │            │
     ┌────────────────→│  fact_    │←──────────────┐
     │                │  enrollment│               │
     │                │            │               │
     │                └────────────┘               │
     ↓                      ↑                      ↓
┌────────────┐        ┌────┴──────┐        ┌────────────┐
│ dim_student│        │ dim_date  │        │ dim_facility│
│ (SCD Type 2)│       └───────────┘        └────────────┘
└────────────┘
```

**Fact Table:** `fact_enrollment` (3,490 enrollments)  
**Dimensions:** 6 dimension tables with normalized department shared between courses and instructors

---

## 🔑 Key Features

### 1. Slowly Changing Dimension (Type 2)
Tracks student attribute changes (major, enrollment status, financial aid) over time with full audit history.

### 2. Incremental Loading
Semester-based batch loading that processes only new data using NOT EXISTS checks.

### 3. Advanced OLAP Queries
Production-ready analytical SQL with window functions (RANK, LAG, CUME_DIST), ROLLUP, and CUBE.

### 4. Data Quality Testing
16 automated pytest validations covering nulls, duplicates, referential integrity, and business rules.

---

## 📁 Project Structure
```
university-analytics-warehouse/
├── data/raw/                   # 5 CSV source files
├── ingestion/                  # Python ETL scripts
├── transformations/            # SQL DDL and DML scripts
├── tests/                      # Pytest data quality suite
├── requirements.txt
└── README.md
```

---

## 🚀 Quick Start
```bash
# 1. Clone repository
git clone https://github.com/ashrumochan-sahoo/university-analytics-warehouse.git

# 2. Start SQL Server
docker run -e "ACCEPT_EULA=Y" -e "MSSQL_SA_PASSWORD=UniAdmin@123" \
  -p 1433:1433 --name sql-server -d \
  mcr.microsoft.com/mssql/server:2022-latest

# 3. Setup Python
python3 -m venv venv
source venv/bin/activate
pip install -r requirements.txt

# 4. Load data
python3 ingestion/load_staging.py

# 5. Run tests
python3 -m pytest tests/ -v
```

---

## 📊 Data Summary

| Entity | Count | Details |
|--------|-------|---------|
| **Students** | 520 (500 + 20 historical) | SCD Type 2 tracking |
| **Courses** | 80 | 10 departments |
| **Enrollments** | 3,490 | 5 semesters |
| **Tests** | 16 | All passing ✅ |

---

## 🎯 Interview Talking Points

✅ Snowflake schema with normalized shared dimensions  
✅ SCD Type 2 with effective dating for audit trails  
✅ Incremental semester loads without full refresh  
✅ Window functions: RANK, LAG, LEAD, CUME_DIST  
✅ Multi-dimensional aggregations: ROLLUP & CUBE  
✅ Automated data quality validation with pytest  

---

## 👤 Author

**Ashrumochan Sahoo**  
Data Engineering Professional | 2.5 Years Experience  
[LinkedIn](https://linkedin.com/in/ashrumochan-sahoo) | [GitHub](https://github.com/ashrumochan-sahoo)
