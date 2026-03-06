-- ============================================================
-- ADVANCED OLAP QUERIES
-- Demonstrating window functions, ROLLUP, CUBE, and analytics
-- ============================================================

USE university_dw;

-- ============================================================
-- QUERY 1: Student Ranking Within Each Course
-- Window Function: ROW_NUMBER(), RANK(), DENSE_RANK()
-- ============================================================
PRINT 'Query 1: Student Ranking Within Courses';

SELECT TOP 20
    s.student_id,
    s.first_name + ' ' + s.last_name AS student_name,
    c.course_name,
    f.grade,
    ROW_NUMBER() OVER (PARTITION BY f.course_key ORDER BY 
        CASE f.grade
            WHEN 'A' THEN 1 WHEN 'A-' THEN 2
            WHEN 'B+' THEN 3 WHEN 'B' THEN 4 WHEN 'B-' THEN 5
            WHEN 'C+' THEN 6 WHEN 'C' THEN 7 WHEN 'C-' THEN 8
            WHEN 'D' THEN 9 WHEN 'F' THEN 10
        END
    ) AS row_num,
    RANK() OVER (PARTITION BY f.course_key ORDER BY 
        CASE f.grade
            WHEN 'A' THEN 1 WHEN 'A-' THEN 2
            WHEN 'B+' THEN 3 WHEN 'B' THEN 4 WHEN 'B-' THEN 5
            WHEN 'C+' THEN 6 WHEN 'C' THEN 7 WHEN 'C-' THEN 8
            WHEN 'D' THEN 9 WHEN 'F' THEN 10
        END
    ) AS rank_num,
    DENSE_RANK() OVER (PARTITION BY f.course_key ORDER BY 
        CASE f.grade
            WHEN 'A' THEN 1 WHEN 'A-' THEN 2
            WHEN 'B+' THEN 3 WHEN 'B' THEN 4 WHEN 'B-' THEN 5
            WHEN 'C+' THEN 6 WHEN 'C' THEN 7 WHEN 'C-' THEN 8
            WHEN 'D' THEN 9 WHEN 'F' THEN 10
        END
    ) AS dense_rank_num
FROM fact_enrollment f
JOIN dim_student s ON f.student_key = s.student_key
JOIN dim_course c ON f.course_key = c.course_key
WHERE f.semester = 'Fall 2024'
ORDER BY c.course_name, row_num;


-- ============================================================
-- QUERY 2: Enrollment Trends with Year-over-Year Comparison
-- Window Function: LAG(), LEAD()
-- ============================================================
PRINT 'Query 2: Enrollment Trends with YoY Comparison';

WITH semester_enrollments AS (
    SELECT 
        f.semester,
        d.year,
        d.quarter,
        COUNT(*) AS total_enrollments,
        AVG(f.attendance_rate) AS avg_attendance
    FROM fact_enrollment f
    JOIN dim_date d ON f.date_key = d.date_key
    GROUP BY f.semester, d.year, d.quarter
)
SELECT 
    semester,
    year,
    total_enrollments,
    LAG(total_enrollments) OVER (ORDER BY year, quarter) AS prev_semester_enrollments,
    total_enrollments - LAG(total_enrollments) OVER (ORDER BY year, quarter) AS enrollment_change,
    CAST(
        CASE 
            WHEN LAG(total_enrollments) OVER (ORDER BY year, quarter) > 0
            THEN ((total_enrollments - LAG(total_enrollments) OVER (ORDER BY year, quarter)) * 100.0 / 
                  LAG(total_enrollments) OVER (ORDER BY year, quarter))
            ELSE 0
        END AS DECIMAL(5,2)
    ) AS pct_change,
    CAST(avg_attendance AS DECIMAL(4,2)) AS avg_attendance_rate
FROM semester_enrollments
ORDER BY year, quarter;


-- ============================================================
-- QUERY 3: Department Enrollment with ROLLUP
-- Multi-level aggregation: Department → Course → Total
-- ============================================================
PRINT 'Query 3: Department Enrollment Analysis with ROLLUP';

SELECT 
    ISNULL(dept.department_name, 'GRAND TOTAL') AS department,
    ISNULL(c.course_name, 'Department Subtotal') AS course,
    COUNT(f.enrollment_key) AS total_enrollments,
    COUNT(DISTINCT f.student_key) AS unique_students,
    CAST(AVG(f.attendance_rate) AS DECIMAL(4,2)) AS avg_attendance
FROM fact_enrollment f
JOIN dim_course c ON f.course_key = c.course_key
JOIN dim_department dept ON c.department_key = dept.department_key
WHERE f.semester = 'Fall 2024'
GROUP BY ROLLUP(dept.department_name, c.course_name)
ORDER BY 
    CASE WHEN dept.department_name IS NULL THEN 1 ELSE 0 END,
    dept.department_name,
    CASE WHEN c.course_name IS NULL THEN 1 ELSE 0 END,
    c.course_name;


-- ============================================================
-- QUERY 4: Multi-Dimensional Analysis with CUBE
-- Department × Semester × Financial Aid
-- ============================================================
PRINT 'Query 4: Multi-Dimensional Enrollment Analysis with CUBE';

SELECT 
    ISNULL(dept.department_name, 'ALL DEPARTMENTS') AS department,
    ISNULL(f.semester, 'ALL SEMESTERS') AS semester,
    ISNULL(s.financial_aid, 'ALL AID STATUS') AS financial_aid,
    COUNT(f.enrollment_key) AS enrollments,
    COUNT(DISTINCT f.student_key) AS students
FROM fact_enrollment f
JOIN dim_course c ON f.course_key = c.course_key
JOIN dim_department dept ON c.department_key = dept.department_key
JOIN dim_student s ON f.student_key = s.student_key AND s.is_current = 1
GROUP BY CUBE(dept.department_name, f.semester, s.financial_aid)
HAVING COUNT(f.enrollment_key) > 10
ORDER BY 
    CASE WHEN dept.department_name IS NULL THEN 1 ELSE 0 END,
    dept.department_name,
    semester,
    financial_aid;


-- ============================================================
-- QUERY 5: Student Performance Percentiles
-- Window Function: CUME_DIST(), PERCENT_RANK()
-- ============================================================
PRINT 'Query 5: Student GPA Percentile Analysis';

SELECT TOP 30
    s.student_id,
    s.first_name + ' ' + s.last_name AS student_name,
    s.major,
    s.gpa,
    CAST(CUME_DIST() OVER (ORDER BY s.gpa) * 100 AS DECIMAL(5,2)) AS percentile,
    CAST(PERCENT_RANK() OVER (ORDER BY s.gpa) * 100 AS DECIMAL(5,2)) AS percent_rank,
    NTILE(4) OVER (ORDER BY s.gpa DESC) AS quartile
FROM dim_student s
WHERE s.is_current = 1
ORDER BY s.gpa DESC;


-- ============================================================
-- QUERY 6: Instructor Teaching Load Analysis
-- Window Function: SUM() OVER with PARTITION
-- ============================================================
PRINT 'Query 6: Instructor Teaching Load per Academic Year';

WITH instructor_load AS (
    SELECT 
        i.instructor_id,
        i.first_name + ' ' + i.last_name AS instructor_name,
        i.title,
        dept.department_name,
        d.year,
        COUNT(DISTINCT f.course_key) AS courses_taught,
        COUNT(f.enrollment_key) AS total_students,
        SUM(COUNT(f.enrollment_key)) OVER (PARTITION BY i.instructor_key) AS career_total_students
    FROM fact_enrollment f
    JOIN dim_instructor i ON f.instructor_key = i.instructor_key
    JOIN dim_department dept ON i.department_key = dept.department_key
    JOIN dim_date d ON f.date_key = d.date_key
    GROUP BY i.instructor_key, i.instructor_id, i.first_name, i.last_name, 
             i.title, dept.department_name, d.year
)
SELECT TOP 20
    instructor_name,
    title,
    department_name,
    year,
    courses_taught,
    total_students,
    career_total_students,
    CAST(total_students * 100.0 / career_total_students AS DECIMAL(5,2)) AS pct_of_career_load
FROM instructor_load
ORDER BY career_total_students DESC, year DESC;


-- ============================================================
-- QUERY 7: Grade Distribution by Department and Semester
-- Window Function: Multiple aggregations
-- ============================================================
PRINT 'Query 7: Grade Distribution Analysis';

SELECT 
    dept.department_name,
    f.semester,
    f.grade,
    COUNT(*) AS grade_count,
    CAST(COUNT(*) * 100.0 / SUM(COUNT(*)) OVER (PARTITION BY dept.department_name, f.semester) AS DECIMAL(5,2)) AS pct_of_semester
FROM fact_enrollment f
JOIN dim_course c ON f.course_key = c.course_key
JOIN dim_department dept ON c.department_key = dept.department_key
WHERE f.semester IN ('Fall 2024', 'Spring 2025')
GROUP BY dept.department_name, f.semester, f.grade
ORDER BY dept.department_name, f.semester, 
    CASE f.grade
        WHEN 'A' THEN 1 WHEN 'A-' THEN 2
        WHEN 'B+' THEN 3 WHEN 'B' THEN 4 WHEN 'B-' THEN 5
        WHEN 'C+' THEN 6 WHEN 'C' THEN 7 WHEN 'C-' THEN 8
        WHEN 'D' THEN 9 WHEN 'F' THEN 10
    END;


-- ============================================================
-- QUERY 8: Facility Utilization by Quarter
-- ============================================================
PRINT 'Query 8: Facility Utilization Analysis';

SELECT 
    fac.building,
    fac.room_type,
    d.year,
    d.quarter,
    COUNT(f.enrollment_key) AS times_used,
    COUNT(DISTINCT f.course_key) AS unique_courses,
    SUM(fac.capacity) AS total_capacity,
    COUNT(f.enrollment_key) AS total_students
FROM fact_enrollment f
JOIN dim_facility fac ON f.facility_key = fac.facility_key
JOIN dim_date d ON f.date_key = d.date_key
GROUP BY fac.building, fac.room_type, d.year, d.quarter
HAVING COUNT(f.enrollment_key) > 10
ORDER BY d.year, d.quarter, times_used DESC;
