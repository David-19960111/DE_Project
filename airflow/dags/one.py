from airflow.operators.python import PythonOperator
from airflow import DAG 
from datetime import timedelta
from confluent_kafka import Producer

import boto3
import pprint 
import datetime
import requests
import json


def fetch_weather_data():
    try:
        #Realiza la solicitud a la API de clima
        api_url = 'https://api.openweathermap.org/data/2.5/weather?lat=4&lon=7&appid=4e25cb0d4efec8285c751febd053deb3'
        response = requests.get(api_url)
        weather_data = response.json()
        return weather_data
    except Exception as e:
        print(f'Error al obtener datos del clima: {str(e)}')
        return None 

def print_json(**kwargs):
    ti = kwargs['ti']
    json_data = ti.xcom_pull(task_ids ='fetch_weather_task') #Obtener el JSON de la tarea anterior
    if json_data:
        pprint.pprint(json_data) #Imprimir el JSON utilizando pprint
    else:
        print('No se pudo obtener el JSON de datos del clima')

def json_serialization(**kwargs):
    ti = kwargs['ti']
    json_data = ti.xcom_pull(task_ids = 'fetch_weather_task') #Obtener el JSON de la tarea anterior
    if json_data:
        producer_config = {
            'bootstrap.servers':'localhost:9092',
            'client.id':'airflow'
        }
        producer = Producer(producer_config)
        json_message = json.dumps(json_data)
        producer.produce('airflow-spark', value=json_message)
        producer.flush()
    else:
        print('No se pudo obtener el JSON dedatos del clima')

def create_s3_bucket():
    s3 = boto3.client('s3')
    bucket_name = 'mi-bucket-de-datos'
    try:
        s3.head_bucket(Bucket=bucket_name)
        print(f'El bucket {bucket_name} ya existe.')
    except Exception as e:
        print(f'Error al verificar el bucket: {e}')
        print(f'Creando el bucket {bucket_name}')
        s3.create_bucket(Bucket=bucket_name)
        print(f'Bucket{bucket_name} creado exitosamente.')


dag = DAG(
    dag_id = "DAG_API_CLIMA",
    schedule_interval = timedelta(seconds=5),
    start_date=datetime(2025, 3, 5), #año, mes, dia
    catchup = False 
)

task_1 = PythonOperator(
    task_id='fetch_weather_task',
    python_callable=fetch_weather_data,
    dag=dag,
    provide_context=True,
)

task_2 = PythonOperator(
    task_id='print_logs',
    python_callable=print_json,
    dag=dag,
    provide_context=True,
)

task_3 = PythonOperator(
    task_id='send task to kafka-spark',
    python_callable=json_serialization,
    dag=dag,
    provide_context=True,
)

create_bucket_task = PythonOperator(
    task_id='create_s3_bucket',
    python_callable=create_s3_bucket,
    dag=dag,
    provide_context=True,
)

task_1 >> task_2 >> task_3 >> create_bucket_task