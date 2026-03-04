-- ============================================================
-- Populate dim_instructor dimension
-- ============================================================

USE university_dw;

DELETE FROM dim_instructor;

INSERT INTO dim_instructor (instructor_id, first_name, last_name, email, department_key, title, hire_date, office_location)
SELECT 
    si.instructor_id,
    si.first_name,
    si.last_name,
    si.email,
    dd.department_key,
    si.title,
    si.hire_date,
    si.office_location
FROM staging_instructors si
JOIN dim_department dd ON si.department = dd.department_name;

SELECT COUNT(*) AS total_instructors FROM dim_instructor;
SELECT TOP 5 * FROM dim_instructor;
