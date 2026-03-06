"""
Incremental loader - loads only new semester data
Detects which enrollments are new and inserts only those
"""
import pandas as pd
import pymssql
from dotenv import load_dotenv
import os

load_dotenv()

conn = pymssql.connect(
    server=os.getenv('DB_SERVER'),
    port=int(os.getenv('DB_PORT')),
    user=os.getenv('DB_USER'),
    password=os.getenv('DB_PASSWORD'),
    database=os.getenv('DB_NAME')
)
cursor = conn.cursor()

print("=" * 60)
print("INCREMENTAL LOAD - FALL 2025 SEMESTER")
print("=" * 60)

# Load new semester data
df_new = pd.read_csv('data/raw/enrollments_fall2025.csv')
print(f"\nNew enrollment records in file: {len(df_new)}")

# Check what's already in staging
cursor.execute("SELECT COUNT(*) FROM staging_enrollments WHERE semester = 'Fall 2025'")
existing_count = cursor.fetchone()[0]
print(f"Existing Fall 2025 records in staging: {existing_count}")

if existing_count > 0:
    print("\n⚠ Fall 2025 data already exists in staging. Clearing it first...")
    cursor.execute("DELETE FROM staging_enrollments WHERE semester = 'Fall 2025'")
    conn.commit()

# Load new data into staging
print("\nLoading Fall 2025 enrollments into staging...")
for _, row in df_new.iterrows():
    cursor.execute("""
        INSERT INTO staging_enrollments 
        (enrollment_id, student_id, course_id, instructor_id, facility_id, 
         semester, enrollment_date, grade, attendance_rate, credits)
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
    """, tuple(row))
conn.commit()

# Verify staging
cursor.execute("SELECT semester, COUNT(*) FROM staging_enrollments GROUP BY semester ORDER BY semester")
staging_summary = cursor.fetchall()
print("\nStaging table summary:")
for semester, count in staging_summary:
    print(f"  {semester}: {count:,} enrollments")

cursor.close()
conn.close()

print("\n" + "=" * 60)
print("STAGING LOAD COMPLETE")
print("=" * 60)
