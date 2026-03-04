"""
Load raw CSV files into SQL Server staging tables
"""
import pandas as pd
import pymssql
from dotenv import load_dotenv
import os

load_dotenv()

# Database connection
conn = pymssql.connect(
    server=os.getenv('DB_SERVER'),
    port=int(os.getenv('DB_PORT')),
    user=os.getenv('DB_USER'),
    password=os.getenv('DB_PASSWORD'),
    database=os.getenv('DB_NAME')
)
cursor = conn.cursor()

print("=" * 60)
print("LOADING DATA INTO STAGING TABLES")
print("=" * 60)

# 1. Load students
print("\n1. Loading staging_students...")
df_students = pd.read_csv('data/raw/students.csv')
cursor.execute("TRUNCATE TABLE staging_students")
for _, row in df_students.iterrows():
    cursor.execute("""
        INSERT INTO staging_students 
        (student_id, first_name, last_name, email, date_of_birth, gender, 
         major, enrollment_status, financial_aid, gpa, enrollment_date)
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
    """, tuple(row))
conn.commit()
print(f"   ✓ Loaded {len(df_students)} students")

# 2. Load courses
print("\n2. Loading staging_courses...")
df_courses = pd.read_csv('data/raw/courses.csv')
cursor.execute("TRUNCATE TABLE staging_courses")
for _, row in df_courses.iterrows():
    cursor.execute("""
        INSERT INTO staging_courses 
        (course_id, course_code, course_name, department, credits, level)
        VALUES (%s, %s, %s, %s, %s, %s)
    """, tuple(row))
conn.commit()
print(f"   ✓ Loaded {len(df_courses)} courses")

# 3. Load instructors
print("\n3. Loading staging_instructors...")
df_instructors = pd.read_csv('data/raw/instructors.csv')
cursor.execute("TRUNCATE TABLE staging_instructors")
for _, row in df_instructors.iterrows():
    cursor.execute("""
        INSERT INTO staging_instructors 
        (instructor_id, first_name, last_name, email, department, 
         title, hire_date, office_location)
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
    """, tuple(row))
conn.commit()
print(f"   ✓ Loaded {len(df_instructors)} instructors")

# 4. Load facilities
print("\n4. Loading staging_facilities...")
df_facilities = pd.read_csv('data/raw/facilities.csv')
cursor.execute("TRUNCATE TABLE staging_facilities")
for _, row in df_facilities.iterrows():
    cursor.execute("""
        INSERT INTO staging_facilities 
        (facility_id, building, room_number, room_type, capacity, 
         has_projector, has_computers)
        VALUES (%s, %s, %s, %s, %s, %s, %s)
    """, tuple(row))
conn.commit()
print(f"   ✓ Loaded {len(df_facilities)} facilities")

# 5. Load enrollments
print("\n5. Loading staging_enrollments...")
df_enrollments = pd.read_csv('data/raw/enrollments.csv')
cursor.execute("TRUNCATE TABLE staging_enrollments")
for _, row in df_enrollments.iterrows():
    cursor.execute("""
        INSERT INTO staging_enrollments 
        (enrollment_id, student_id, course_id, instructor_id, facility_id, 
         semester, enrollment_date, grade, attendance_rate, credits)
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
    """, tuple(row))
conn.commit()
print(f"   ✓ Loaded {len(df_enrollments)} enrollments")

# Verify row counts
print("\n" + "=" * 60)
print("STAGING TABLE ROW COUNTS")
print("=" * 60)
tables = ['staging_students', 'staging_courses', 'staging_instructors', 
          'staging_facilities', 'staging_enrollments']
for table in tables:
    cursor.execute(f"SELECT COUNT(*) FROM {table}")
    count = cursor.fetchone()[0]
    print(f"{table:<30}: {count:>6,} rows")

cursor.close()
conn.close()

print("\n" + "=" * 60)
print("STAGING LOAD COMPLETE")
print("=" * 60)
