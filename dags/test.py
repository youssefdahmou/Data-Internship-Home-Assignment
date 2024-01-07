import os
import json
from airflow.operators.python_operator import PythonOperator
from airflow.providers.sqlite.hooks.sqlite import SqliteHook

# ... (previous code remains unchanged)

#@task()
def load(transformed_dir):
    """Load transformed data to SQLite database."""
    sqlite_hook = SqliteHook(sqlite_conn_id='sqlite_default')

    for filename in os.listdir(transformed_dir):
        if filename.endswith('.json'):
            transformed_filepath = os.path.join(transformed_dir, filename)

            with open(transformed_filepath, 'r') as file:
                data = json.load(file)

                # Insert data into SQLite database tables
                job_id = insert_job(sqlite_hook, data['job'])
                insert_company(sqlite_hook, job_id, data['company'])
                insert_education(sqlite_hook, job_id, data['education'])
                insert_experience(sqlite_hook, job_id, data['experience'])
                insert_salary(sqlite_hook, job_id, data['salary'])
                insert_location(sqlite_hook, job_id, data['location'])

def insert_job(sqlite_hook, job_data):
    """Insert job data into the job table and return the job_id."""
    query = """
        INSERT INTO job (title, industry, description, employment_type, date_posted)
        VALUES (?, ?, ?, ?, ?)
    """
    params = (
        job_data['title'],
        job_data['industry'],
        job_data['description'],
        job_data['employment_type'],
        job_data['date_posted'],
    )
    return sqlite_hook.insert_rows(table='job', rows=[params])

def insert_company(sqlite_hook, job_id, company_data):
    """Insert company data into the company table."""
    query = """
        INSERT INTO company (job_id, name, link)
        VALUES (?, ?, ?)
    """
    params = (job_id, company_data['name'], company_data['link'])
    sqlite_hook.run(query, parameters=params)

def insert_education(sqlite_hook, job_id, education_data):
    """Insert education data into the education table."""
    query = """
        INSERT INTO education (job_id, required_credential)
        VALUES (?, ?)
    """
    params = (job_id, education_data['required_credential'])
    sqlite_hook.run(query, parameters=params)

def insert_experience(sqlite_hook, job_id, experience_data):
    """Insert experience data into the experience table."""
    query = """
        INSERT INTO experience (job_id, months_of_experience, seniority_level)
        VALUES (?, ?, ?)
    """
    params = (job_id, experience_data['months_of_experience'], experience_data['seniority_level'])
    sqlite_hook.run(query, parameters=params)

def insert_salary(sqlite_hook, job_id, salary_data):
    """Insert salary data into the salary table."""
    query = """
        INSERT INTO salary (job_id, currency, min_value, max_value, unit)
        VALUES (?, ?, ?, ?, ?)
    """
    params = (job_id, salary_data['currency'], salary_data['min_value'], salary_data['max_value'], salary_data['unit'])
    sqlite_hook.run(query, parameters=params)

def insert_location(sqlite_hook, job_id, location_data):
    """Insert location data into the location table."""
    query = """
        INSERT INTO location (job_id, country, locality, region, postal_code, street_address, latitude, longitude)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?)
    """
    params = (
        job_id,
        location_data['country'],
        location_data['locality'],
        location_data['region'],
        location_data['postal_code'],
        location_data['street_address'],
        location_data['latitude'],
        location_data['longitude'],
    )
    sqlite_hook.run(query, parameters=params)

transformed_dir = os.path.join(os.path.dirname(__file__), '..' ,'staging', 'transformed')
load(transformed_dir)