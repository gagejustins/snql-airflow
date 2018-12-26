from airflow import DAG
from airflow.hooks import PostgresHook
from airflow.operators import PythonOperator
from datetime import datetime

default_args = {
	'owner': 'Justin',
	'depends_on_past': False,
	'retries': 3
	}

dag = DAG(
	dag_id='snql_test',
	default_args=default_args,
	start_date=datetime(2018,12,25),
	schedule_interval='0 2 * * *')

def hello_world():
	return "helerrrr!"

def select():
	hook = PostgresHook('snql')
	results = hook.get_records(sql='select * from sneakers;')
	print(results)

task1 = PythonOperator(
	task_id='select',
	python_callable=select,
	dag=dag)

task2 = PythonOperator(
	task_id='hello_world',
	python_callable=hello_world,
	dag=dag)

task1 >> task2
