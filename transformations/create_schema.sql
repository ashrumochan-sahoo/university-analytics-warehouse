-- ============================================================
-- UNIVERSITY ANALYTICS DATA WAREHOUSE
-- Snowflake Schema Implementation
-- Author: Ashrumochan Sahoo
-- ============================================================

USE university_dw;

-- Drop tables if they exist (for clean reruns)
DROP TABLE IF EXISTS fact_enrollment;
DROP TABLE IF EXISTS dim_student;
DROP TABLE IF EXISTS dim_course;
DROP TABLE IF EXISTS dim_instructor;
DROP TABLE IF EXISTS dim_facility;
DROP TABLE IF EXISTS dim_department;
DROP TABLE IF EXISTS dim_date;
DROP TABLE IF EXISTS staging_students;
DROP TABLE IF EXISTS staging_courses;
DROP TABLE IF EXISTS staging_instructors;
DROP TABLE IF EXISTS staging_facilities;
DROP TABLE IF EXISTS staging_enrollments;

-- ============================================================
-- STAGING TABLES (Raw data landing zone)
-- ============================================================

CREATE TABLE staging_students (
    student_id VARCHAR(10) PRIMARY KEY,
    first_name VARCHAR(50),
    last_name VARCHAR(50),
    email VARCHAR(100),
    date_of_birth DATE,
    gender VARCHAR(10),
    major VARCHAR(100),
    enrollment_status VARCHAR(20),
    financial_aid VARCHAR(3),
    gpa DECIMAL(3,2),
    enrollment_date DATE
);

CREATE TABLE staging_courses (
    course_id VARCHAR(10) PRIMARY KEY,
    course_code VARCHAR(20),
    course_name VARCHAR(200),
    department VARCHAR(100),
    credits INT,
    level VARCHAR(20)
);

CREATE TABLE staging_instructors (
    instructor_id VARCHAR(10) PRIMARY KEY,
    first_name VARCHAR(50),
    last_name VARCHAR(50),
    email VARCHAR(100),
    department VARCHAR(100),
    title VARCHAR(50),
    hire_date DATE,
    office_location VARCHAR(100)
);

CREATE TABLE staging_facilities (
    facility_id VARCHAR(10) PRIMARY KEY,
    building VARCHAR(100),
    room_number VARCHAR(20),
    room_type VARCHAR(50),
    capacity INT,
    has_projector VARCHAR(3),
    has_computers VARCHAR(3)
);

CREATE TABLE staging_enrollments (
    enrollment_id VARCHAR(10) PRIMARY KEY,
    student_id VARCHAR(10),
    course_id VARCHAR(10),
    instructor_id VARCHAR(10),
    facility_id VARCHAR(10),
    semester VARCHAR(20),
    enrollment_date DATE,
    grade VARCHAR(5),
    attendance_rate DECIMAL(3,2),
    credits INT
);

-- ============================================================
-- DIMENSION TABLES (Snowflake Schema)
-- ============================================================

-- dim_department (shared by instructors and courses)
CREATE TABLE dim_department (
    department_key INT IDENTITY(1,1) PRIMARY KEY,
    department_name VARCHAR(100) UNIQUE NOT NULL,
    created_date DATETIME DEFAULT GETDATE()
);

-- dim_date (shared time dimension)
CREATE TABLE dim_date (
    date_key INT PRIMARY KEY,
    full_date DATE NOT NULL,
    year INT,
    quarter INT,
    month INT,
    month_name VARCHAR(20),
    day INT,
    day_of_week INT,
    day_name VARCHAR(20),
    week_of_year INT,
    is_weekend BIT
);

-- dim_student (SCD Type 2)
CREATE TABLE dim_student (
    student_key INT IDENTITY(1,1) PRIMARY KEY,
    student_id VARCHAR(10) NOT NULL,
    first_name VARCHAR(50),
    last_name VARCHAR(50),
    email VARCHAR(100),
    date_of_birth DATE,
    gender VARCHAR(10),
    major VARCHAR(100),
    enrollment_status VARCHAR(20),
    financial_aid VARCHAR(3),
    gpa DECIMAL(3,2),
    enrollment_date DATE,
    effective_start_date DATE NOT NULL,
    effective_end_date DATE,
    is_current BIT DEFAULT 1
);

CREATE INDEX idx_student_id ON dim_student(student_id);
CREATE INDEX idx_is_current ON dim_student(is_current);

-- dim_course
CREATE TABLE dim_course (
    course_key INT IDENTITY(1,1) PRIMARY KEY,
    course_id VARCHAR(10) UNIQUE NOT NULL,
    course_code VARCHAR(20),
    course_name VARCHAR(200),
    department_key INT,
    credits INT,
    level VARCHAR(20),
    FOREIGN KEY (department_key) REFERENCES dim_department(department_key)
);

-- dim_instructor
CREATE TABLE dim_instructor (
    instructor_key INT IDENTITY(1,1) PRIMARY KEY,
    instructor_id VARCHAR(10) UNIQUE NOT NULL,
    first_name VARCHAR(50),
    last_name VARCHAR(50),
    email VARCHAR(100),
    department_key INT,
    title VARCHAR(50),
    hire_date DATE,
    office_location VARCHAR(100),
    FOREIGN KEY (department_key) REFERENCES dim_department(department_key)
);

-- dim_facility
CREATE TABLE dim_facility (
    facility_key INT IDENTITY(1,1) PRIMARY KEY,
    facility_id VARCHAR(10) UNIQUE NOT NULL,
    building VARCHAR(100),
    room_number VARCHAR(20),
    room_type VARCHAR(50),
    capacity INT,
    has_projector VARCHAR(3),
    has_computers VARCHAR(3)
);

-- ============================================================
-- FACT TABLE
-- ============================================================

CREATE TABLE fact_enrollment (
    enrollment_key INT IDENTITY(1,1) PRIMARY KEY,
    enrollment_id VARCHAR(10) UNIQUE NOT NULL,
    student_key INT NOT NULL,
    course_key INT NOT NULL,
    instructor_key INT NOT NULL,
    facility_key INT NOT NULL,
    date_key INT NOT NULL,
    semester VARCHAR(20),
    grade VARCHAR(5),
    attendance_rate DECIMAL(3,2),
    credits INT,
    FOREIGN KEY (student_key) REFERENCES dim_student(student_key),
    FOREIGN KEY (course_key) REFERENCES dim_course(course_key),
    FOREIGN KEY (instructor_key) REFERENCES dim_instructor(instructor_key),
    FOREIGN KEY (facility_key) REFERENCES dim_facility(facility_key),
    FOREIGN KEY (date_key) REFERENCES dim_date(date_key)
);

CREATE INDEX idx_semester ON fact_enrollment(semester);
CREATE INDEX idx_grade ON fact_enrollment(grade);
CREATE INDEX idx_student ON fact_enrollment(student_key);
CREATE INDEX idx_course ON fact_enrollment(course_key);

SELECT 'Schema creation complete!' AS Status;