"""
Generate Fall 2025 semester enrollment data for incremental loading demo
"""
import pandas as pd
import numpy as np
import random

np.random.seed(100)
random.seed(100)

print("=" * 60)
print("GENERATING FALL 2025 SEMESTER DATA")
print("=" * 60)

# Load existing data
df_students = pd.read_csv('data/raw/students_updated.csv')
df_courses = pd.read_csv('data/raw/courses.csv')
df_instructors = pd.read_csv('data/raw/instructors.csv')
df_facilities = pd.read_csv('data/raw/facilities.csv')
df_existing = pd.read_csv('data/raw/enrollments.csv')

grades = ['A','A-','B+','B','B-','C+','C','C-','D','F']
grade_weights = [0.15, 0.15, 0.15, 0.15, 0.10, 0.10, 0.08, 0.05, 0.04, 0.03]

# Fall 2025 semester
semester = {'term': 'Fall 2025', 'start': '2025-09-01', 'end': '2025-12-15'}

# Select 150-200 random students
active_students = random.sample(range(1, 501), random.randint(150, 200))

enrollments = []
enrollment_id = df_existing['enrollment_id'].max()  # Continue from last ID
enrollment_id = int(enrollment_id[1:]) + 1  # Remove 'E' prefix

for student_num in active_students:
    num_courses = random.randint(3, 5)
    student_courses = random.sample(range(1, len(df_courses)+1), num_courses)
    
    for course_num in student_courses:
        instructor_num = random.randint(1, 30)
        facility_num = random.randint(1, 20)
        
        enrollment = {
            'enrollment_id': f'E{enrollment_id:05d}',
            'student_id': f'S{student_num:04d}',
            'course_id': f'C{course_num:03d}',
            'instructor_id': f'I{instructor_num:03d}',
            'facility_id': f'F{facility_num:03d}',
            'semester': semester['term'],
            'enrollment_date': semester['start'],
            'grade': np.random.choice(grades, p=grade_weights),
            'attendance_rate': round(random.uniform(0.65, 1.0), 2),
            'credits': df_courses.iloc[course_num-1]['credits']
        }
        enrollments.append(enrollment)
        enrollment_id += 1

df_new = pd.DataFrame(enrollments)

# Save new semester data separately
df_new.to_csv('data/raw/enrollments_fall2025.csv', index=False)

print(f"\n✓ Generated {len(df_new)} enrollments for Fall 2025")
print(f"  Students enrolled: {len(active_students)}")
print(f"  Date range: {semester['start']} to {semester['end']}")
print(f"\nFile saved: data/raw/enrollments_fall2025.csv")

# Show breakdown
print("\nCourse enrollment distribution:")
course_counts = df_new['course_id'].value_counts().head(5)
for course, count in course_counts.items():
    print(f"  {course}: {count} students")

print("=" * 60)
