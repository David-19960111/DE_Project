from airflow.operators.python import PythonOperator
from airflow import DAG 
from datetime import timedelta
from confluent_kafka import Producer
from datetime import datetime

import boto3
import pprint 
import requests
import json

S3_BUCKET_NAME = 'bucket-DE'
KAFKA_BROKER = 'localhost:9092'
KAFKA_TOPIC = 'airflow-spark'

def fetch_weather_data():
    try:
        #Realiza la solicitud a la API de clima
        api_url = 'https://api.openweathermap.org/data/2.5/weather?lat=40.416&lon=-3.703&appid=4e25cb0d4efec8285c751febd053deb3'
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
        try:
            producer_config = {
                'bootstrap.servers': KAFKA_BROKER,
                'client.id':'airflow'
            }
            producer = Producer(producer_config)
            json_message = json.dumps(json_data)
            producer.produce(KAFKA_TOPIC, value=json_message)
            producer.flush()
            print("Datos enviados a kafka con exito.")
        except Exception as e:
            print(f"Error enviando datos a Kafka: {e}")
    else:
        print('No se pudo obtener el JSON dedatos del clima')

def upload_to_s3(**kwargs):
    """Sube los datos del clima a S3"""
    ti = kwargs['ti']
    json_data = ti.xcom_pull(task_ids='fetch_weather_task', key='weather_data')

    if json_data:
        s3 = boto3.client('s3')
        s3_file_name = f'weather_data_{datetime.now().strftime("%Y%m%d_%H%M%S")}.json'

        try:
            # Verifica si el bucket existe
            s3.head_bucket(Bucket=S3_BUCKET_NAME)
        except Exception:
            print(f'Creando bucket {S3_BUCKET_NAME}')
            s3.create_bucket(Bucket=S3_BUCKET_NAME, CreateBucketConfiguration={'LocationConstraint': 'us-east-1'})

        # Sube el archivo a S3
        s3.put_object(Bucket=S3_BUCKET_NAME, Key=s3_file_name, Body=json.dumps(json_data))
        print(f'Datos subidos a S3: s3://{S3_BUCKET_NAME}/{s3_file_name}')
    else:
        print('No se pudo obtener el JSON para subir a S3')


dag = DAG(
    dag_id = "DAG_API_CLIMA",
    schedule_interval = timedelta(minutes=1),
    start_date=datetime(2025, 3, 7), #año, mes, dia
    catchup = False,
    tags = ['weather', 'kafka', 's3']
)

task_1 = PythonOperator(
    task_id='fetch_weather_task',
    python_callable=fetch_weather_data,
    dag=dag,
)

task_2 = PythonOperator(
    task_id='print_logs',
    python_callable=print_json,
    dag=dag,
)

task_3 = PythonOperator(
    task_id='send_task_to_kafka-spark',
    python_callable=json_serialization,
    dag=dag,
)

task_4 = PythonOperator(
    task_id='upload_to_s3',
    python_callable=upload_to_s3,
    dag=dag,
    provide_context=True,
)

task_1 >> task_2 >> task_3 >> task_4