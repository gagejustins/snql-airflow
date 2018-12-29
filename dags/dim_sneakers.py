from airflow import DAG
from airflow.hooks import PostgresHook
from airflow.operators import PythonOperator
from datetime import datetime

#Create inherited PythonOperator class that handles SQL templates
class SQLTemplatedPythonOperator(PythonOperator):
    template_ext = ('.sql', '.abcdef')

default_args = {
    'owner': 'jgage',
    'depends_on_past': False,
    'email_on_failure': True,
    'email': 'gagejustins+airflow@gmail.com',
    'retries': 3
}

dag = DAG(
    dag_id='dim_sneakers',
    default_args=default_args,
    start_date=datetime(2018, 12, 21),
    schedule_interval='0 2 * * *',
    template_searchpath='sql/'
)

def create_staging():

    create_sql = """CREATE TABLE IF NOT EXISTS dim_sneakers(
                sneaker_id INT,
                sneaker_name VARCHAR,
                color VARCHAR,
                created_at TIMESTAMP,
                is_owned BOOLEAN,
                sold_at TIMESTAMP,
                trashed_at TIMESTAMP,
                given_at TIMESTAMP,
                manufacturer_name VARCHAR,
                collaborator_name VARCHAR,
                count_wears INT,
                count_cleans INT,
                count_walks INT,
                updated_at TIMESTAMP,
                is_current BOOLEAN);"""

    delete_sql = """DELETE FROM dim_sneakers"""

    #Initialize staging hook
    staging_hook = PostgresHook('snql_staging')

    #Run create table query
    staging_hook.run(create_sql)

    #Run delete records query
    staging_hook.run(delete_sql)

def pull_and_insert(**kwargs):

    #Pull query from sql directory
    query = kwargs['templates_dict']['query']

    #Initialize snql hook and pull data
    snql_hook = PostgresHook('snql')
    results = snql_hook.get_records(query)

    #Initialize staging hook and insert data to staging table
    staging_hook = PostgresHook('snql_staging')
    staging_hook.insert_rows('dim_sneakers', results)

def create_target():

    create_sql = """CREATE TABLE IF NOT EXISTS dim_sneakers(
                sneaker_id INT,
                sneaker_name VARCHAR,
                color VARCHAR,
                created_at TIMESTAMP,
                is_owned BOOLEAN,
                sold_at TIMESTAMP,
                trashed_at TIMESTAMP,
                given_at TIMESTAMP,
                manufacturer_name VARCHAR,
                collaborator_name VARCHAR,
                count_wears INT,
                count_cleans INT,
                count_walks INT,
                updated_at TIMESTAMP,
                is_current BOOLEAN);"""

    #Initialize target hook
    snql_hook = PostgresHook('snql')

    #Run create sql
    snql_hook.run(create_sql)

def populate_target(**kwargs):

    #Pull data from staging
    staging_hook = PostgresHook('snql_staging')
    results = staging_hook.get_records("SELECT * FROM dim_sneakers")

    #Initialize hook to snql
    snql_hook = PostgresHook('snql')

    #Pull query from sql directory
    set_current_query = kwargs['templates_dict']['query']

    #Set current dim_sneakers.is_current to FALSE
    snql_hook.run(set_current_query)

    #Insert into target table
    snql_hook.insert_rows('dim_sneakers', results)

def delete_from_staging():

    staging_hook = PostgresHook('snql_staging')
    staging_hook.run('DELETE FROM dim_sneakers;')

create_staging_table = PythonOperator(
    task_id = 'create_staging_table',
    python_callable = create_staging,
    email_on_failure = True,
    email = 'gagejustins+airflow@gmail.com',
    dag = dag
)

pull_and_insert_to_staging = SQLTemplatedPythonOperator(
    task_id = 'pull_and_insert_to_staging',
    templates_dict = {'query': 'dim_sneakers/extract.sql'},
    python_callable = pull_and_insert,
    params = {'ds': datetime.utcnow()},
    email_on_failure = True,
    email = 'gagejustins+airflow@gmail.com',
    provide_context=True,
    dag = dag
)

create_target_table = PythonOperator(
    task_id = 'create_target_table',
    python_callable = create_target,
    email_on_failure = True,
    email = 'gagejustins+airflow@gmail.com',
    dag = dag
)

populate_target_table = SQLTemplatedPythonOperator(
    task_id = 'populate_target_table',
    templates_dict = {'query': 'dim_sneakers/set_current.sql'},
    python_callable = populate_target,
    params = {'ds': datetime.utcnow()},
    email_on_failure = True,
    email = 'gagejustins+airflow@gmail.com',
    provide_context = True,
    dag = dag
)

delete_from_staging = PythonOperator(
    task_id = 'delete_from_staging',
    python_callable = delete_from_staging,
    email_on_failure = True,
    email = 'gagejustins+airflow@gmail.com',
    dag = dag
)

create_staging_table >> pull_and_insert_to_staging >> create_target_table >> populate_target_table >> delete_from_staging
