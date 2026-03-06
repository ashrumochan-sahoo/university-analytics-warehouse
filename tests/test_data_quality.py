"""
Data Quality Tests for University Analytics Warehouse
Tests staging tables, dimensions, and fact table integrity
"""
import pytest
import pymssql
from dotenv import load_dotenv
import os

load_dotenv()

@pytest.fixture(scope="module")
def db_connection():
    """Database connection fixture"""
    conn = pymssql.connect(
        server=os.getenv('DB_SERVER'),
        port=int(os.getenv('DB_PORT')),
        user=os.getenv('DB_USER'),
        password=os.getenv('DB_PASSWORD'),
        database=os.getenv('DB_NAME')
    )
    yield conn
    conn.close()


# ============================================================
# STAGING TABLE TESTS
# ============================================================

def test_staging_students_no_nulls(db_connection):
    """Test: No NULL values in critical staging_students columns"""
    cursor = db_connection.cursor()
    cursor.execute("""
        SELECT COUNT(*) FROM staging_students 
        WHERE student_id IS NULL 
           OR first_name IS NULL 
           OR last_name IS NULL
    """)
    null_count = cursor.fetchone()[0]
    assert null_count == 0, f"Found {null_count} NULL values in critical columns"


def test_staging_courses_no_duplicates(db_connection):
    """Test: No duplicate course_id in staging_courses"""
    cursor = db_connection.cursor()
    cursor.execute("""
        SELECT course_id, COUNT(*) as cnt 
        FROM staging_courses 
        GROUP BY course_id 
        HAVING COUNT(*) > 1
    """)
    duplicates = cursor.fetchall()
    assert len(duplicates) == 0, f"Found {len(duplicates)} duplicate course IDs"


def test_staging_enrollments_row_count(db_connection):
    """Test: Staging enrollments has expected row count"""
    cursor = db_connection.cursor()
    cursor.execute("SELECT COUNT(*) FROM staging_enrollments")
    count = cursor.fetchone()[0]
    assert count >= 2851, f"Expected at least 2851 enrollments, got {count}"


# ============================================================
# DIMENSION TABLE TESTS
# ============================================================

def test_dim_department_populated(db_connection):
    """Test: dim_department has 10 departments"""
    cursor = db_connection.cursor()
    cursor.execute("SELECT COUNT(*) FROM dim_department")
    count = cursor.fetchone()[0]
    assert count == 10, f"Expected 10 departments, got {count}"


def test_dim_date_range(db_connection):
    """Test: dim_date covers 2018-2030 range"""
    cursor = db_connection.cursor()
    cursor.execute("""
        SELECT MIN(year) as min_year, MAX(year) as max_year 
        FROM dim_date
    """)
    min_year, max_year = cursor.fetchone()
    assert min_year == 2018, f"Expected min year 2018, got {min_year}"
    assert max_year == 2030, f"Expected max year 2030, got {max_year}"


def test_dim_student_scd_integrity(db_connection):
    """Test: SCD Type 2 - Each current student has is_current=1"""
    cursor = db_connection.cursor()
    cursor.execute("""
        SELECT student_id, COUNT(*) as current_count
        FROM dim_student
        WHERE is_current = 1
        GROUP BY student_id
        HAVING COUNT(*) > 1
    """)
    duplicates = cursor.fetchall()
    assert len(duplicates) == 0, f"Found {len(duplicates)} students with multiple current records"


def test_dim_student_effective_dates(db_connection):
    """Test: Historical records have effective_end_date set"""
    cursor = db_connection.cursor()
    cursor.execute("""
        SELECT COUNT(*) FROM dim_student 
        WHERE is_current = 0 AND effective_end_date IS NULL
    """)
    invalid_count = cursor.fetchone()[0]
    assert invalid_count == 0, f"Found {invalid_count} historical records without end date"


def test_dim_course_foreign_keys(db_connection):
    """Test: All courses link to valid departments"""
    cursor = db_connection.cursor()
    cursor.execute("""
        SELECT COUNT(*) FROM dim_course c
        LEFT JOIN dim_department d ON c.department_key = d.department_key
        WHERE d.department_key IS NULL
    """)
    orphan_count = cursor.fetchone()[0]
    assert orphan_count == 0, f"Found {orphan_count} courses with invalid department_key"


# ============================================================
# FACT TABLE TESTS
# ============================================================

def test_fact_enrollment_no_nulls(db_connection):
    """Test: No NULL foreign keys in fact_enrollment"""
    cursor = db_connection.cursor()
    cursor.execute("""
        SELECT COUNT(*) FROM fact_enrollment
        WHERE student_key IS NULL
           OR course_key IS NULL
           OR instructor_key IS NULL
           OR facility_key IS NULL
           OR date_key IS NULL
    """)
    null_count = cursor.fetchone()[0]
    assert null_count == 0, f"Found {null_count} NULL foreign keys in fact table"


def test_fact_enrollment_referential_integrity_student(db_connection):
    """Test: All fact student_keys exist in dim_student"""
    cursor = db_connection.cursor()
    cursor.execute("""
        SELECT COUNT(*) FROM fact_enrollment f
        LEFT JOIN dim_student s ON f.student_key = s.student_key
        WHERE s.student_key IS NULL
    """)
    orphan_count = cursor.fetchone()[0]
    assert orphan_count == 0, f"Found {orphan_count} orphaned student references"


def test_fact_enrollment_referential_integrity_course(db_connection):
    """Test: All fact course_keys exist in dim_course"""
    cursor = db_connection.cursor()
    cursor.execute("""
        SELECT COUNT(*) FROM fact_enrollment f
        LEFT JOIN dim_course c ON f.course_key = c.course_key
        WHERE c.course_key IS NULL
    """)
    orphan_count = cursor.fetchone()[0]
    assert orphan_count == 0, f"Found {orphan_count} orphaned course references"


def test_fact_enrollment_no_duplicates(db_connection):
    """Test: No duplicate enrollment_id in fact table"""
    cursor = db_connection.cursor()
    cursor.execute("""
        SELECT enrollment_id, COUNT(*) as cnt
        FROM fact_enrollment
        GROUP BY enrollment_id
        HAVING COUNT(*) > 1
    """)
    duplicates = cursor.fetchall()
    assert len(duplicates) == 0, f"Found {len(duplicates)} duplicate enrollment IDs"


def test_fact_enrollment_grade_values(db_connection):
    """Test: Only valid grades in fact table"""
    cursor = db_connection.cursor()
    cursor.execute("""
        SELECT COUNT(*) FROM fact_enrollment
        WHERE grade NOT IN ('A','A-','B+','B','B-','C+','C','C-','D','F')
    """)
    invalid_count = cursor.fetchone()[0]
    assert invalid_count == 0, f"Found {invalid_count} invalid grade values"


def test_fact_enrollment_attendance_range(db_connection):
    """Test: Attendance rate between 0 and 1"""
    cursor = db_connection.cursor()
    cursor.execute("""
        SELECT COUNT(*) FROM fact_enrollment
        WHERE attendance_rate < 0 OR attendance_rate > 1
    """)
    invalid_count = cursor.fetchone()[0]
    assert invalid_count == 0, f"Found {invalid_count} invalid attendance rates"


# ============================================================
# BUSINESS LOGIC TESTS
# ============================================================

def test_enrollment_count_by_semester(db_connection):
    """Test: Each semester has reasonable enrollment count"""
    cursor = db_connection.cursor()
    cursor.execute("""
        SELECT semester, COUNT(*) as cnt
        FROM fact_enrollment
        GROUP BY semester
    """)
    semesters = cursor.fetchall()
    for semester, count in semesters:
        assert count >= 500, f"{semester} has only {count} enrollments (expected ≥500)"


def test_scd_type2_history_exists(db_connection):
    """Test: At least some students have historical records"""
    cursor = db_connection.cursor()
    cursor.execute("""
        SELECT COUNT(DISTINCT student_id) FROM dim_student
        WHERE is_current = 0
    """)
    historical_students = cursor.fetchone()[0]
    assert historical_students >= 15, f"Expected ≥15 students with history, got {historical_students}"
