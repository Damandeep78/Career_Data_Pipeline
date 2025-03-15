# %%
import pandas as pd
import json
import duckdb
from datetime import datetime, timedelta
import os
from airflow import DAG
from airflow.operators.python import PythonOperator


# %%
#Extracting data from JSON file
def extract_data(ti, json_file= '/opt/airflow/plugins/professionals_nested.json'):
    with open(json_file, 'r') as f:
        data = json.load(f)
    ti.xcom_push(key='raw_data', value=data)

# %%
#Transforming to structured data

def transform_data(ti):
    data = ti.xcom_pull(key='raw_data', task_ids='extract_data')
    professionals = data['professionals']

    professionals_list = []
    jobs_list = []
    skills_list = []
    certifications_list = []
    educations_list = []

    for professional in professionals:
        professional_id = professional['professional_id']
        professionals_list.append({
            'professional_id': professional_id,
            'years_experience': professional['years_experience'],
            'current_industry': professional['current_industry'],
            'current_role': professional['current_role'],
            'education_level': professional['education_level']
        })

        for job in professional.get('jobs', []):
            jobs_list.append({
                'job_id': job['job_id'],
                'professional_id': professional_id,
                'company': job['company'],
                'industry': job['industry'],
                'role': job['role'],
                'start_date': job['start_date'],
                'end_date': job['end_date'],
                'salary_band': job['salary_band']
            })

        for skill in professional.get('skills', []):
            skills_list.append({
                'skill_id': skill['skill_id'],
                'professional_id': professional_id,
                'skill_name': skill['skill_name'],
                'proficiency_level': skill['proficiency_level'],
                'years_experience': skill['years_experience']
            })

        for certification in professional.get('certifications', []):
            certifications_list.append({
                'certification_id': certification['certification_id'],
                'professional_id': professional_id,
                'certification_name': certification['certification_name'],
                'issuing_organization': certification['issuing_organization'],
                'date_earned': certification['date_earned'],
                'expiration_date': certification['expiration_date']
            })

        for education in professional.get('education', []):
            educations_list.append({
                'education_id': education['education_id'],
                'professional_id': professional_id,
                'degree': education['degree'],
                'institution': education['institution'],
                'field_of_study': education['field_of_study'],
                'graduation_date': education['graduation_date']
            })

    professionals_df = pd.DataFrame(professionals_list)
    jobs_df = pd.DataFrame(jobs_list)
    skills_df = pd.DataFrame(skills_list)
    certifications_df = pd.DataFrame(certifications_list)
    educations_df = pd.DataFrame(educations_list)

    ti.xcom_push(key='professionals_df', value=professionals_df.to_json())
    ti.xcom_push(key='jobs_df', value=jobs_df.to_json())
    ti.xcom_push(key='skills_df', value=skills_df.to_json())
    ti.xcom_push(key='certifications_df', value=certifications_df.to_json())
    ti.xcom_push(key='educations_df', value=educations_df.to_json())
       





# %%
# Data validation
def validate_data_task(ti):
    professionals_df = pd.read_json(ti.xcom_pull(key='professionals_df', task_ids='transform_data'))
    jobs_df = pd.read_json(ti.xcom_pull(key='jobs_df', task_ids='transform_data'))
    skills_df = pd.read_json(ti.xcom_pull(key='skills_df', task_ids='transform_data'))
    certifications_df = pd.read_json(ti.xcom_pull(key='certifications_df', task_ids='transform_data'))
    educations_df = pd.read_json(ti.xcom_pull(key='educations_df', task_ids='transform_data'))

    def validate_data(jobs_df, certifications_df, educations_df, skills_df):
        """Performs data validation and quality checks."""
        for df, date_cols, df_name in [(jobs_df, ['start_date', 'end_date'], 'jobs_df'),
                              (certifications_df, ['date_earned', 'expiration_date'], 'certifications_df'),
                              (educations_df, ['graduation_date'], 'educations_df')]:
            for col in date_cols:
                if col in df.columns:
                    df[col] = pd.to_datetime(df[col], errors='coerce')
                    null_dates = df[df[col].isnull()]
                    if not null_dates.empty:
                        print(f"Invalid date format in {col} of {df_name}:")
                        print(null_dates)

        if 'start_date' in jobs_df.columns and 'end_date' in jobs_df.columns:
            invalid_dates = jobs_df[jobs_df['end_date'] < jobs_df['start_date']].dropna(subset=['end_date'])
            if not invalid_dates.empty:
                print("Job end dates before start dates:")
                print(invalid_dates)

        if 'salary_band' in jobs_df.columns:
            salary_outliers = jobs_df[jobs_df['salary_band'] > jobs_df['salary_band'].quantile(0.99)]
            if not salary_outliers.empty:
                print("Salary band outliers:")
                print(salary_outliers)

        if 'years_experience' in skills_df.columns:
          skill_exp_outliers = skills_df[skills_df['years_experience'] > skills_df['years_experience'].quantile(0.99)]
          if not skill_exp_outliers.empty:
              print("Skill years experience outliers:")
              print(skill_exp_outliers)

    validate_data(jobs_df, certifications_df, educations_df, skills_df)

# %%
#Loading data to DuckDB
def load_data_task(ti):
    professionals_df = pd.read_json(ti.xcom_pull(key='professionals_df', task_ids='transform_data'))
    jobs_df = pd.read_json(ti.xcom_pull(key='jobs_df', task_ids='transform_data'))
    skills_df = pd.read_json(ti.xcom_pull(key='skills_df', task_ids='transform_data'))
    certifications_df = pd.read_json(ti.xcom_pull(key='certifications_df', task_ids='transform_data'))
    educations_df = pd.read_json(ti.xcom_pull(key='educations_df', task_ids='transform_data'))

    for df, date_cols in [(jobs_df, ['start_date', 'end_date']),
                          (certifications_df, ['date_earned', 'expiration_date']),
                          (educations_df, ['graduation_date'])]:
        for col in date_cols:
            if col in df.columns:
                df[col] = pd.to_datetime(df[col], errors='coerce')

    con = duckdb.connect('/opt/airflow/plugins/career_data.duckdb', read_only=False)

    con.register('professionals', professionals_df)
    con.register('jobs', jobs_df)
    con.register('skills', skills_df)
    con.register('certifications', certifications_df)
    con.register('educations', educations_df)

    # Dimension Tables
    con.execute("""
        CREATE OR REPLACE TABLE DimProfessionals AS
        SELECT professional_id, years_experience
        FROM professionals;
    """)

    con.execute("""
        CREATE OR REPLACE TABLE DimSkills AS
        SELECT skill_id, professional_id, skill_name, years_experience
        FROM skills;
    """)

    con.execute("""
        CREATE OR REPLACE TABLE DimCertifications AS
        SELECT certification_id, professional_id, date_earned, expiration_date
        FROM certifications;
    """)

    con.execute("""
        CREATE OR REPLACE TABLE DimEducations AS
        SELECT education_id, professional_id, graduation_date
        FROM educations;
    """)

    # Fact Table
    con.execute("""
        CREATE OR REPLACE TABLE FactJobs AS
        SELECT
            job_id,
            professional_id,
            company,
            industry,
            role,
            start_date,
            end_date,
            salary_band,
            DATE_DIFF('day', start_date, end_date) AS job_duration
        FROM jobs;
    """)

    con.close()

# %%
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2025, 2, 22),
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

# %%
with DAG(
    dag_id='career_analytics_pipeline',
    default_args=default_args,
    schedule='@once',
    catchup=False,
) as dag:
    extract_data_task = PythonOperator(
        task_id='extract_data',
        python_callable=extract_data,
    )

    transform_data_task = PythonOperator(
        task_id='transform_data',
        python_callable=transform_data,
    )

    validate_data_task_op = PythonOperator(
        task_id='validate_data',
        python_callable=validate_data_task,
    )

    load_data_task_op = PythonOperator(
        task_id='load_data',
        python_callable=load_data_task,
    )

    extract_data_task >> transform_data_task >> validate_data_task_op >> load_data_task_op

# %%



