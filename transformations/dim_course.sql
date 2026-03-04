-- ============================================================
-- Populate dim_course dimension
-- ============================================================

USE university_dw;

DELETE FROM dim_course;

INSERT INTO dim_course (course_id, course_code, course_name, department_key, credits, level)
SELECT 
    sc.course_id,
    sc.course_code,
    sc.course_name,
    dd.department_key,
    sc.credits,
    sc.level
FROM staging_courses sc
JOIN dim_department dd ON sc.department = dd.department_name;

SELECT COUNT(*) AS total_courses FROM dim_course;
SELECT TOP 5 * FROM dim_course;
