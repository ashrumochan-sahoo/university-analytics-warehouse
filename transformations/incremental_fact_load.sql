-- ============================================================
-- Incremental Load - Insert only new semester enrollments
-- ============================================================

USE university_dw;

-- Insert only Fall 2025 enrollments (new semester)
INSERT INTO fact_enrollment (
    enrollment_id, student_key, course_key, instructor_key, facility_key, 
    date_key, semester, grade, attendance_rate, credits
)
SELECT 
    se.enrollment_id,
    ds.student_key,
    dc.course_key,
    di.instructor_key,
    df.facility_key,
    CAST(FORMAT(se.enrollment_date, 'yyyyMMdd') AS INT) AS date_key,
    se.semester,
    se.grade,
    se.attendance_rate,
    se.credits
FROM staging_enrollments se
JOIN dim_student ds ON se.student_id = ds.student_id AND ds.is_current = 1
JOIN dim_course dc ON se.course_id = dc.course_id
JOIN dim_instructor di ON se.instructor_id = di.instructor_id
JOIN dim_facility df ON se.facility_id = df.facility_id
WHERE se.semester = 'Fall 2025'
  AND NOT EXISTS (
      SELECT 1 FROM fact_enrollment fe 
      WHERE fe.enrollment_id = se.enrollment_id
  );

-- Verification
SELECT 
    'Total Enrollments' AS metric,
    COUNT(*) AS value
FROM fact_enrollment
UNION ALL
SELECT 
    'Fall 2025 Enrollments',
    COUNT(*)
FROM fact_enrollment
WHERE semester = 'Fall 2025';

-- Show enrollment breakdown by semester
SELECT semester, COUNT(*) AS enrollments
FROM fact_enrollment
GROUP BY semester
ORDER BY semester;
