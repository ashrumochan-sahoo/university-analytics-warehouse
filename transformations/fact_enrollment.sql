-- ============================================================
-- Populate fact_enrollment table
-- Joins all dimension keys with enrollment facts
-- ============================================================

USE university_dw;

DELETE FROM fact_enrollment;

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
JOIN dim_facility df ON se.facility_id = df.facility_id;

-- Verify
SELECT COUNT(*) AS total_enrollments FROM fact_enrollment;

-- Show enrollment breakdown by semester
SELECT semester, COUNT(*) AS enrollments
FROM fact_enrollment
GROUP BY semester
ORDER BY semester;

-- Sample records
SELECT TOP 5 * FROM fact_enrollment;
