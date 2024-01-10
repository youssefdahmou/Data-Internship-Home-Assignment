from datetime import timedelta, datetime
import os
import requests as re
import json
import pandas as pd 
from airflow.operators.python import PythonOperator
from airflow.providers.sqlite.hooks.sqlite import SqliteHook
from airflow.decorators import dag, task
from airflow.providers.sqlite.hooks.sqlite import SqliteHook
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator
from airflow.utils.dates import days_ago

import logging


logging.basicConfig(level=logging.DEBUG)
extracted_dir = os.path.join(os.path.dirname(__file__), '..',  'staging', 'extracted')


TABLES_CREATION_QUERY = """CREATE TABLE IF NOT EXISTS job (id INTEGER PRIMARY KEY AUTOINCREMENT,title VARCHAR(225),industry VARCHAR(225),description TEXT,employment_type VARCHAR(125),date_posted DATE);

CREATE TABLE IF NOT EXISTS company (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    job_id INTEGER,
    name VARCHAR(225),
    link TEXT,
    FOREIGN KEY (job_id) REFERENCES job(id)
);

CREATE TABLE IF NOT EXISTS education (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    job_id INTEGER,
    required_credential VARCHAR(225),
    FOREIGN KEY (job_id) REFERENCES job(id)
);

CREATE TABLE IF NOT EXISTS experience (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    job_id INTEGER,
    months_of_experience INTEGER,
    seniority_level VARCHAR(25),
    FOREIGN KEY (job_id) REFERENCES job(id)
);

CREATE TABLE IF NOT EXISTS salary (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    job_id INTEGER,
    currency VARCHAR(3),
    min_value NUMERIC,
    max_value NUMERIC,
    unit VARCHAR(12),
    FOREIGN KEY (job_id) REFERENCES job(id)
);

CREATE TABLE IF NOT EXISTS location (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    job_id INTEGER,
    country VARCHAR(60),
    locality VARCHAR(60),
    region VARCHAR(60),
    postal_code VARCHAR(25),
    street_address VARCHAR(225),
    latitude NUMERIC,
    longitude NUMERIC,
    FOREIGN KEY (job_id) REFERENCES job(id)
)
"""


# ------------------------------------- data extraction ---------------------------------------------------

@task()
def extract():
    """Extract data from jobs.csv."""
    # Read jobs.csv and extract the context column
    csv_path = os.path.join(os.path.dirname(__file__), '..', 'source', 'jobs.csv')
    df = pd.read_csv(csv_path)
    context_data = df['context']
    extracted_dir = os.path.join(os.path.dirname(__file__), '..',  'staging', 'extracted')
    os.makedirs(extracted_dir, exist_ok=True)
    for index, item in enumerate(context_data):
        filename = f"extracted_{index}.txt"
        filepath = os.path.join(extracted_dir, filename)
        with open(filepath, 'w') as file:
            file.write(str(item))
    return extracted_dir


#------------------------------------------------------ data transformation --------------------------
def get_nested_value(data, keys):
    current_value = data
    for key in keys:
        if current_value is None:
            return None
        current_value = current_value.get(key)
    return current_value

def clean_description(description):
    """Clean job description."""
    # Remove HTML tags
    description = description.replace('.&lt;br&gt;&lt;br&gt;&lt;strong&gt;' ,  '')
    description = description.replace('&lt;br&gt;&lt;/u&gt;&lt;/strong&gt;&lt;ul&gt;&lt;li&gt;;' ,  '')
    description = description.replace('&lt;br&gt;&lt;br&gt;&lt;/strong&gt' ,  '')
    description = description.replace('&lt;/li&gt;&lt;li&gt' ,  '')
    description = description.replace('&lt;br&gt;&lt;/li&gt;&lt;/ul&gt;&lt;strong&gt' ,  '')
    description = description.replace('&lt;br&gt;&lt;/strong&gt;&lt;ul&gt;&lt;li&gt' ,  '')
    description = description.replace('br&gt;&lt;/u&gt;&lt;/strong&gt;&lt;ul&gt;&lt;li&gt' ,  '')
    description = description.replace('&lt;br&gt;&lt;br&gt' ,  '')
    description = description.replace('/li&gt;&lt;/ul&gt' ,  '')
    cleaned_description = re.sub('<[^<]+?>', '', description)
    cleaned_description = re.sub('[^A-Za-z0-9]+', ' ', cleaned_description)
    cleaned_description = cleaned_description.strip()  

    return cleaned_description

@task()
def transform(extracted_dir):
    """Transform data and save to staging/transformed as json files."""
    transformed_dir = os.path.join(os.path.dirname(__file__), '..' ,'staging', 'transformed')
    os.makedirs(transformed_dir, exist_ok=True)

    for filename in os.listdir(extracted_dir):
        try :
            if filename.endswith('.txt'):
                extracted_filepath = os.path.join(extracted_dir, filename)
        
                with open(extracted_filepath, 'r') as file:
                    json_string = file.read()
                    data = json.loads(json_string)
        
                    # Perform transformation according to the desired schema
                    transformed_data = {
                        "job": {
                            "title": get_nested_value(data, ['title']),
                            "industry": get_nested_value(data, ["industry"]),
                            "description": clean_description(get_nested_value(data, ["description"])),
                            "employment_type": get_nested_value(data, ['employmentType']),
                            "date_posted": get_nested_value(data, ['datePosted']),
                        },
                        "company": {
                            "name": get_nested_value(data, ['hiringOrganization', 'name']),
                            "link": get_nested_value(data, ['hiringOrganization', 'sameAs']),
                        },
                        "education": {
                            "required_credential": get_nested_value(data, ['educationRequirements', 'credentialCategory']),
                        },
                        "experience": {
                            "months_of_experience": get_nested_value(data, ['experienceRequirements', 'monthsOfExperience']),
                            "seniority_level": get_nested_value(data, ['title']),
                        },
                        "salary": {
                            "currency": get_nested_value(data, ['baseSalary', 'currency']),
                            "min_value": get_nested_value(data, ['baseSalary', 'value', 'minValue']),
                            "max_value": get_nested_value(data, ['baseSalary', 'value', 'maxValue']),
                            "unit": get_nested_value(data, ['baseSalary', 'value', 'unitText']),
                        },
                        "location": {
                            "country": get_nested_value(data, ['jobLocation', 'address', 'addressCountry']),
                            "locality": get_nested_value(data, ['jobLocation', 'address', 'addressLocality']),
                            "region": get_nested_value(data, ['jobLocation', 'address', 'addressRegion']),
                            "postal_code": get_nested_value(data, ['jobLocation', 'address', 'postalCode']),
                            "street_address": get_nested_value(data, ['jobLocation', 'address', 'streetAddress']),
                            "latitude": get_nested_value(data, ['jobLocation', 'latitude']),
                            "longitude": get_nested_value(data, ['jobLocation', 'longitude']),
                        },
                    }
        
                    
                    transformed_filename = f"transformed_{filename.replace('.txt', '.json')}"
                    transformed_filepath = os.path.join(transformed_dir, transformed_filename)
            
                    with open(transformed_filepath, 'w') as transformed_file:
                        json.dump(transformed_data, transformed_file, indent=2)
        except : pass 
    return transformed_dir


#-----------------------------------------------------  loading the data into sqllite db --------------------------

@task()
def load(transformed_dir):
    """Load transformed data to SQLite database."""
    sqlite_hook = SqliteHook(sqlite_conn_id='jobs')

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
    query_max_id = "SELECT MAX(id) FROM job;"
    try:
        max_id = sqlite_hook.get_first(query_max_id)[0] or 0
        job_data['id'] = max_id + 1
        query_insert_job = """
            INSERT INTO job (id, title, industry, description, employment_type, date_posted)
            VALUES (?, ?, ?, ?, ?, ?)
        """
        params = (
            job_data['id'],
            job_data['title'],
            job_data['industry'],
            job_data['description'],
            job_data['employment_type'],
            job_data['date_posted'],
        )
        sqlite_hook.insert_rows(table='job', rows=[params])
        return max_id + 1
    except Exception as e:
        raise Exception(f"Error inserting job data: {e}")

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



# ------------------------------------ task 4 : creating tables ------------------------------
@task()
def create_tables(query):
    """Create all tables at once if they don't exist."""
    sqlite_hook = SqliteHook(sqlite_conn_id='jobs')
    try:
        for statement in query.split(';'):
            statement = statement.strip()
            if statement:
                sqlite_hook.run(statement)
        return 'The tables are created successfully.'
    except Exception as e:
        raise Exception(f"Error creating tables: {e}")


# -------------------------------------------------------- dags  management-------------------------


DAG_DEFAULT_ARGS = {
    "depends_on_past": False,
    "retries": 3,
    "retry_delay": timedelta(minutes=0.000000001),
    'start_date': days_ago(5)
}

@dag(
    dag_id="etl_dag",
    description="ETL LinkedIn job posts",
    tags=["etl"],
    schedule="0 0 * * *",
    catchup=False,
    default_args=DAG_DEFAULT_ARGS
)
def etl_dag():
    create_tables2 = create_tables(TABLES_CREATION_QUERY)

    extracted_data = extract()
    transformed_data = transform(extracted_data)
    load_task = load(transformed_data)

    # Use the set_downstream method to set task dependencies
    create_tables2.set_downstream(extracted_data)
    extracted_data.set_downstream(transformed_data)
    transformed_data.set_downstream(load_task)
etl_dag()

# https://maxcotec.com/learning/how-to-create-a-dag-in-airflow/