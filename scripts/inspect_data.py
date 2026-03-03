import pandas as pd

print("=" * 60)
print("DATA FILE INSPECTION")
print("=" * 60)

# 1. Students
print("\n1. STUDENTS.CSV")
print("-" * 60)
df_students = pd.read_csv('data/raw/students.csv')
print(f"Rows: {len(df_students):,}")
print(f"Columns: {list(df_students.columns)}")
print("\nFirst 3 rows:")
print(df_students.head(3))

# 2. Courses
print("\n\n2. COURSES.CSV")
print("-" * 60)
df_courses = pd.read_csv('data/raw/courses.csv')
print(f"Rows: {len(df_courses):,}")
print(f"Columns: {list(df_courses.columns)}")
print("\nFirst 3 rows:")
print(df_courses.head(3))

# 3. Instructors
print("\n\n3. INSTRUCTORS.CSV")
print("-" * 60)
df_instructors = pd.read_csv('data/raw/instructors.csv')
print(f"Rows: {len(df_instructors):,}")
print(f"Columns: {list(df_instructors.columns)}")
print("\nFirst 3 rows:")
print(df_instructors.head(3))

# 4. Facilities
print("\n\n4. FACILITIES.CSV")
print("-" * 60)
df_facilities = pd.read_csv('data/raw/facilities.csv')
print(f"Rows: {len(df_facilities):,}")
print(f"Columns: {list(df_facilities.columns)}")
print("\nFirst 3 rows:")
print(df_facilities.head(3))

# 5. Enrollments
print("\n\n5. ENROLLMENTS.CSV")
print("-" * 60)
df_enrollments = pd.read_csv('data/raw/enrollments.csv')
print(f"Rows: {len(df_enrollments):,}")
print(f"Columns: {list(df_enrollments.columns)}")
print("\nFirst 3 rows:")
print(df_enrollments.head(3))
print("\nSemesters in data:")
print(df_enrollments['semester'].value_counts())

print("\n" + "=" * 60)
print("INSPECTION COMPLETE")
print("=" * 60)
