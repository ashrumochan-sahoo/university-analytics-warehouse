-- ============================================================
-- Populate dim_student with SCD Type 2 logic
-- Initial load: All students marked as current
-- ============================================================

USE university_dw;

DELETE FROM dim_student;

-- Initial load: Insert all students as current records
INSERT INTO dim_student (
    student_id, first_name, last_name, email, date_of_birth, gender,
    major, enrollment_status, financial_aid, gpa, enrollment_date,
    effective_start_date, effective_end_date, is_current
)
SELECT 
    student_id,
    first_name,
    last_name,
    email,
    date_of_birth,
    gender,
    major,
    enrollment_status,
    financial_aid,
    gpa,
    enrollment_date,
    enrollment_date AS effective_start_date,
    NULL AS effective_end_date,
    1 AS is_current
FROM staging_students;

-- Verify
SELECT COUNT(*) AS total_students FROM dim_student;
SELECT COUNT(*) AS current_students FROM dim_student WHERE is_current = 1;
SELECT TOP 5 * FROM dim_student;
