-- ============================================================
-- SCD Type 2 Update Logic for dim_student
-- Detects changes and maintains history
-- ============================================================

USE university_dw;

-- Temporary table to hold changed students
IF OBJECT_ID('tempdb..#changed_students') IS NOT NULL DROP TABLE #changed_students;

SELECT 
    ss.student_id,
    ss.first_name,
    ss.last_name,
    ss.email,
    ss.date_of_birth,
    ss.gender,
    ss.major AS new_major,
    ss.enrollment_status AS new_status,
    ss.financial_aid AS new_aid,
    ss.gpa AS new_gpa,
    ss.enrollment_date,
    ds.student_key AS old_student_key,
    ds.major AS old_major,
    ds.enrollment_status AS old_status,
    ds.financial_aid AS old_aid,
    ds.gpa AS old_gpa
INTO #changed_students
FROM staging_students ss
INNER JOIN dim_student ds 
    ON ss.student_id = ds.student_id 
    AND ds.is_current = 1
WHERE 
    ss.major != ds.major 
    OR ss.enrollment_status != ds.enrollment_status
    OR ss.financial_aid != ds.financial_aid
    OR ABS(ss.gpa - ds.gpa) > 0.01;

-- Show what will change
SELECT COUNT(*) AS students_to_update FROM #changed_students;

-- Step 1: Close old records
UPDATE dim_student
SET 
    effective_end_date = '2024-08-15',
    is_current = 0
WHERE student_key IN (
    SELECT old_student_key FROM #changed_students
);

-- Step 2: Insert new records for changed students
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
    new_major,
    new_status,
    new_aid,
    new_gpa,
    enrollment_date,
    '2024-08-15' AS effective_start_date,
    NULL AS effective_end_date,
    1 AS is_current
FROM #changed_students;

-- Verification
SELECT 
    COUNT(*) AS total_records,
    SUM(CASE WHEN is_current = 1 THEN 1 ELSE 0 END) AS current_records,
    SUM(CASE WHEN is_current = 0 THEN 1 ELSE 0 END) AS historical_records
FROM dim_student;

-- Show sample of changed students (both old and new records)
SELECT TOP 10
    student_id,
    major,
    enrollment_status,
    effective_start_date,
    effective_end_date,
    is_current
FROM dim_student
WHERE student_id IN (
    SELECT student_id FROM #changed_students
)
ORDER BY student_id, effective_start_date;

DROP TABLE #changed_students;
