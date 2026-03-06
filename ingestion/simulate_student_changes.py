"""
Simulate student attribute changes for SCD Type 2 demonstration
Changes: major, enrollment_status, financial_aid, GPA
"""
import pandas as pd
import random
from datetime import datetime, timedelta

print("=" * 60)
print("SIMULATING STUDENT CHANGES FOR SCD TYPE 2")
print("=" * 60)

# Load current students
df = pd.read_csv('data/raw/students.csv')

# Select 20 random students to change
students_to_change = random.sample(range(len(df)), 20)

departments = ['Computer Science','Business Administration','Engineering','Biology',
               'Psychology','Mathematics','English','Chemistry','Physics','Economics']

changes = []
for idx in students_to_change:
    student = df.iloc[idx].copy()
    
    # Change major (most common SCD Type 2 scenario)
    old_major = student['major']
    new_major = random.choice([d for d in departments if d != old_major])
    
    # Sometimes also change other attributes
    change_aid = random.random() < 0.3
    change_status = random.random() < 0.2
    change_gpa = random.random() < 0.4
    
    changes.append({
        'student_id': student['student_id'],
        'old_major': old_major,
        'new_major': new_major,
        'old_financial_aid': student['financial_aid'],
        'new_financial_aid': 'No' if change_aid and student['financial_aid'] == 'Yes' else student['financial_aid'],
        'old_enrollment_status': student['enrollment_status'],
        'new_enrollment_status': 'Part-time' if change_status and student['enrollment_status'] == 'Full-time' else student['enrollment_status'],
        'old_gpa': student['gpa'],
        'new_gpa': round(student['gpa'] + random.uniform(-0.5, 0.5), 2) if change_gpa else student['gpa'],
        'change_date': '2024-08-15'  # Before Fall 2024 semester
    })
    
    # Update the student in dataframe
    df.loc[idx, 'major'] = new_major
    if change_aid:
        df.loc[idx, 'financial_aid'] = 'No' if student['financial_aid'] == 'Yes' else 'Yes'
    if change_status:
        df.loc[idx, 'enrollment_status'] = 'Part-time' if student['enrollment_status'] == 'Full-time' else 'Part-time'
    if change_gpa:
        df.loc[idx, 'gpa'] = round(student['gpa'] + random.uniform(-0.5, 0.5), 2)

# Save updated students (this simulates receiving updated data feed)
df.to_csv('data/raw/students_updated.csv', index=False)

# Save change log
df_changes = pd.DataFrame(changes)
df_changes.to_csv('data/processed/student_changes_log.csv', index=False)

print("\nChanges simulated:")
print(f"  Total students changed: {len(changes)}")
print(f"\nSample changes:")
for i, change in enumerate(changes[:5], 1):
    print(f"\n  {i}. {change['student_id']}")
    print(f"     Major: {change['old_major']} → {change['new_major']}")
    if change['old_financial_aid'] != change['new_financial_aid']:
        print(f"     Financial Aid: {change['old_financial_aid']} → {change['new_financial_aid']}")

print(f"\n✓ Updated file saved: data/raw/students_updated.csv")
print(f"✓ Change log saved: data/processed/student_changes_log.csv")
print("=" * 60)
