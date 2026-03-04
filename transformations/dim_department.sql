-- ============================================================
-- Populate dim_department (Snowflake shared dimension)
-- ============================================================

USE university_dw;

-- Clear existing data
DELETE FROM dim_department;

-- Insert unique departments from courses
INSERT INTO dim_department (department_name)
SELECT DISTINCT department
FROM staging_courses
WHERE department IS NOT NULL
UNION
SELECT DISTINCT department
FROM staging_instructors
WHERE department IS NOT NULL;

-- Verify
SELECT department_key, department_name, created_date
FROM dim_department
ORDER BY department_name;