# 🎓 University Academic Analytics Data Warehouse

![Python](https://img.shields.io/badge/Python-3.9-blue?logo=python&logoColor=white)
![SQL Server](https://img.shields.io/badge/SQL%20Server-2022-red?logo=microsoftsqlserver&logoColor=white)
![Docker](https://img.shields.io/badge/Docker-Enabled-blue?logo=docker&logoColor=white)
![Tests](https://img.shields.io/badge/Tests-16%20Passing-brightgreen?logo=pytest&logoColor=white)
![SCD](https://img.shields.io/badge/SCD-Type%202-orange)
![Schema](https://img.shields.io/badge/Schema-Snowflake-blueviolet)
![Status](https://img.shields.io/badge/Status-Complete-success)

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
                      │  fact_     │
     ┌────────────────→│ enrollment│←──────────────┐
     │                └────────────┘               │
     ↓                      ↑                      ↓
┌────────────┐        ┌────┴──────┐        ┌────────────┐
│ dim_student│        │ dim_date  │        │dim_facility│
│ (SCD Type2)│        └───────────┘        └────────────┘
└────────────┘
```

**Fact Table:** `fact_enrollment` (3,490 enrollments)  
**Dimensions:** 6 dimension tables with normalized department shared between courses and instructors

---

## 🔑 Key Features

### 1. Slowly Changing Dimension (Type 2)
Tracks student attribute changes (major, enrollment status, financial aid) over time:
- **Effective dating:** `effective_start_date` and `effective_end_date`
- **Current flag:** `is_current` for active records
- **Full history:** Maintains all historical versions

**Example:** When student S0050 changed major from English to Biology:
- Old record: `effective_end_date = 2024-08-15, is_current = 0`
- New record: `effective_start_date = 2024-08-15, is_current = 1`

### 2. Incremental Loading
Semester-based batch loading that processes only new data:
- Detects new enrollments using `NOT EXISTS` check
- Prevents duplicate loads
- Reduces processing time by avoiding full refresh

**Loaded Semesters:**
- Fall 2023: 588 enrollments
- Spring 2024: 684 enrollments
- Fall 2024: 788 enrollments
- Spring 2025: 791 enrollments
- Fall 2025: 639 enrollments (incremental load)

### 3. Advanced OLAP Queries
Production-ready analytical SQL demonstrating:

**Window Functions:**
- `ROW_NUMBER()`, `RANK()`, `DENSE_RANK()` - Student performance rankings
- `LAG()`, `LEAD()` - Year-over-year enrollment trends
- `CUME_DIST()`, `PERCENT_RANK()` - GPA percentile analysis
- `NTILE()` - Quartile distribution

**Multi-Dimensional Aggregations:**
- `ROLLUP` - Hierarchical subtotals (Department → Course → Total)
- `CUBE` - All dimensional combinations (Department × Semester × Financial Aid)

**Business Insights:**
- Enrollment trends with YoY growth rates
- Instructor teaching load analysis
- Grade distribution by department
- Facility utilization metrics

### 4. Data Quality Testing
16 automated pytest validations covering:
- No NULLs in critical columns
- No duplicate primary keys
- Referential integrity (all FKs valid)
- SCD Type 2 correctness (no duplicate current records)
- Business rules (grade values, attendance range)

---

## 📁 Project Structure
```
university-analytics-warehouse/
├── data/
│   ├── raw/                    # CSV source files
│   │   ├── students.csv        # 500 students
│   │   ├── courses.csv         # 80 courses
│   │   ├── instructors.csv     # 30 instructors
│   │   ├── facilities.csv      # 20 classrooms/labs
│   │   └── enrollments.csv     # 3,490 enrollments
│   └── processed/              # Output files
│
├── ingestion/
│   ├── load_staging.py         # Load CSV → staging tables
│   ├── simulate_student_changes.py  # Generate SCD Type 2 scenarios
│   ├── generate_new_semester.py     # Create new semester data
│   └── incremental_loader.py   # Incremental batch loader
│
├── transformations/
│   ├── create_schema.sql       # DDL for all tables
│   ├── dim_*.sql               # Dimension population scripts
│   ├── update_dim_student_scd.sql  # SCD Type 2 update logic
│   ├── fact_enrollment.sql     # Fact table load
│   ├── incremental_fact_load.sql   # Incremental loading
│   └── olap_queries.sql        # 8 advanced analytical queries
│
├── tests/
│   └── test_data_quality.py    # 16 pytest validations
│
├── requirements.txt
└── README.md
```

---

## 🚀 Getting Started

### Prerequisites
- **Docker Desktop** (for SQL Server)
- **Python 3.9+**
- **DBeaver** or any SQL client
- **Git**

### Installation

**1. Clone the repository:**
```bash
git clone https://github.com/ashrumochan-sahoo/university-analytics-warehouse.git
cd university-analytics-warehouse
```

**2. Start SQL Server in Docker:**
```bash
docker run -e "ACCEPT_EULA=Y" \
  -e "MSSQL_SA_PASSWORD=UniAdmin@123" \
  -p 1433:1433 \
  --name sql-server \
  -d \
  mcr.microsoft.com/mssql/server:2022-latest
```

**3. Set up Python environment:**
```bash
python3 -m venv venv
source venv/bin/activate  # On Windows: venv\Scripts\activate
pip install -r requirements.txt
```

**4. Load data:**
```bash
# Load staging tables
python3 ingestion/load_staging.py

# Run dimension and fact SQL scripts in DBeaver
# (See transformations/ folder)
```

**5. Run tests:**
```bash
python3 -m pytest tests/test_data_quality.py -v
```

---

## 📈 Sample Queries

### Student GPA Percentile Ranking
```sql
SELECT 
    student_id,
    first_name + ' ' + last_name AS name,
    major,
    gpa,
    CAST(CUME_DIST() OVER (ORDER BY gpa) * 100 AS DECIMAL(5,2)) AS percentile
FROM dim_student
WHERE is_current = 1
ORDER BY gpa DESC;
```

### Enrollment Trends with Year-over-Year Growth
```sql
SELECT 
    semester,
    total_enrollments,
    LAG(total_enrollments) OVER (ORDER BY year) AS prev_year,
    total_enrollments - LAG(total_enrollments) OVER (ORDER BY year) AS growth
FROM semester_enrollments;
```

---

## 📊 Data Summary

| Entity | Count | Details |
|--------|-------|---------|
| **Students** | 520 (500 + 20 historical) | SCD Type 2 tracking |
| **Courses** | 80 | Across 10 departments |
| **Instructors** | 30 | Various titles and departments |
| **Facilities** | 20 | Classrooms, Labs, Lecture Halls |
| **Enrollments** | 3,490 | 5 semesters (Fall 2023 → Fall 2025) |
| **Departments** | 10 | Computer Science, Business, etc. |
| **Date Range** | 2018-2030 | 4,748 dates for time-series analysis |
| **Tests** | 16 | All passing (0.37s) ✅ |

---

## 🛠️ Technologies Used

| Category | Tool | Purpose |
|----------|------|---------|
| **Database** | SQL Server 2022 | Enterprise data warehouse |
| **Container** | Docker | Portable SQL Server deployment |
| **Query Tool** | DBeaver Community | Database management |
| **Language** | Python 3.9 | ETL scripting and data generation |
| **Libraries** | pandas, pymssql, sqlalchemy, pytest | Data manipulation, DB connectivity, testing |
| **Version Control** | Git + GitHub | Source code management |

---

## 🎯 Interview Talking Points

**"Walk me through your data warehouse project"**
> "I built a university analytics warehouse in SQL Server with a snowflake schema tracking 3,490 enrollments across 5 semesters. I implemented SCD Type 2 to track student major changes - when 20 students changed majors, the system automatically expired old records and inserted new versions with effective dating. For incremental loading, I detect new semester data using NOT EXISTS checks to avoid reprocessing existing records. I wrote 8 advanced OLAP queries using window functions like RANK, LAG, CUME_DIST, plus ROLLUP and CUBE for multi-dimensional analysis. All validated by 16 automated pytest tests."

**"What's the difference between star and snowflake schema?"**
> "Star schema denormalizes dimensions for query speed. Snowflake schema normalizes shared attributes - in my project, dim_department is referenced by both courses and instructors, eliminating redundancy. The tradeoff is an extra join for queries, but I gain data consistency."

**"Explain your SCD Type 2 implementation"**
> "My dim_student table has effective_start_date, effective_end_date, and is_current flag. I use a temp table to detect changes, then UPDATE old records to set is_current=0 and effective_end_date, followed by INSERT of new records with updated attributes and is_current=1. This preserves full audit history."

---

## 👤 Author

**Ashrumochan Sahoo**  
Data Engineering Professional | 2.5 Years Experience  
[LinkedIn](www.linkedin.com/in/ashrumochan-sahoo-6a0982199) | [GitHub](https://github.com/ashrumochan-sahoo)

---

## 📄 License

This project is for portfolio and educational purposes.
