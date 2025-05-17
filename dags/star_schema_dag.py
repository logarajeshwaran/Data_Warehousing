from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.empty import EmptyOperator
from airflow.providers.http.sensors.http import HttpSensor
from airflow.providers.mysql.hooks.mysql import MySqlHook
from airflow.providers.slack.operators.slack_webhook import SlackWebhookOperator
from airflow.models import Variable
from datetime import datetime
import requests
import sys
import os
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__),'..')))
from scripts.star_schema.process_data import transform_city_data, transform_weather_data , transform_date_data
from scripts.star_schema.ddl_scripts import *


def task_fail_slack_alert(context):
    slack_msg = f"""
            :red_circle: Task Failed.
            *Task*: {context.get('task_instance').task_id}
            *Dag*: {context.get('task_instance').dag_id}
            *Execution Time*: {context.get('execution_date')}
            *Log Url*: {context.get('task_instance').log_url}
            """
    failed_alert = SlackWebhookOperator(
        task_id='slack_notification_on_failure',
        slack_webhook_conn_id='slack_connection',
        message=slack_msg,
        channel='#airflow-notifications',
        username='airflow-bot',
    )
    return failed_alert.execute(context=context)


def dag_success_slack_alert(context):
    slack_msg = f"""
            :large_green_circle: DAG Succeeded!
            *Dag*: {context.get('dag').dag_id}
            *Execution Time*: {context.get('execution_date')}
            """
    success_alert = SlackWebhookOperator(
        task_id='slack_notification_on_success',
        slack_webhook_conn_id='slack_connection',
        message=slack_msg,
        channel='#airflow-notifications',
        username='airflow-bot',
    )
    return success_alert.execute(context=context)




dag = DAG(
    'star_schema_dag',
    start_date=datetime(2025, 5, 1),
    schedule_interval=None,
    catchup=False,
    on_failure_callback=task_fail_slack_alert,
    on_success_callback=dag_success_slack_alert,
    )


check_api = HttpSensor(
    task_id='check_api',
    http_conn_id='api_connection',
    endpoint='data/2.5/weather',    
    request_params={'q': 'London', 'appid': Variable.get('API_KEY')},
    response_check=lambda response: 'London' in response.text,
    poke_interval=5,
    timeout=20,
    dag=dag,
    )

def extract_data(**context):
    cities = ['London', 'Paris', 'Berlin', 'Rome', 'Madrid', 'Moscow', 'Tokyo', 'Delhi', 'Beijing', 'New York']
    results = []
    output_path = '/opt/airflow/data/weather_data.json'
    api_key = Variable.get('API_KEY')
    for city in cities:
        response = requests.get(f'http://api.openweathermap.org/data/2.5/weather?q={city}&appid={api_key}')
        if response.status_code == 200:
            results.append(response.json())
        else:
            results.append({'city': city, 'error': response.text})

    with open(output_path, 'w') as f:
        import json
        json.dump(results, f, indent=2)

    context['task_instance'].xcom_push(key='weather_data', value=output_path)

def transform_data(**context):
    weather_data_path = context['task_instance'].xcom_pull(task_ids='extract_task', key='weather_data')
    city_df = transform_city_data(weather_data_path)
    weather_df = transform_weather_data(weather_data_path)
    date_df = transform_date_data()
    context['task_instance'].xcom_push(key='city_data', value=city_df)
    context['task_instance'].xcom_push(key='weather_data', value=weather_df)
    context['task_instance'].xcom_push(key='date_data', value=date_df)

def load_data(**context):
    mysql_hook = MySqlHook(mysql_conn_id='mysql_default')
    conn = mysql_hook.get_conn()
    print("Connection established to MySQL database.")
    cursor = conn.cursor()
    print("Cursor created.")
    cursor.execute(DROP_TABLES)
    print("Existing tables dropped.")
    cursor.execute(DIM_CITY)
    print("DIM_CITY table created.")
    cursor.execute(DIM_DATE)
    print("DIM_DATE table created.")
    cursor.execute(FACT_WEATHER)
    print("FACT_WEATHER table created.")

    city_data = context['task_instance'].xcom_pull(task_ids='transform_data', key='city_data')

    for city in city_data:
        cursor.execute(
            "INSERT INTO dim_city (city_id, city_name, country_code, latitude, longitude) VALUES (%s, %s, %s, %s, %s)",
            (city['city_id'], city['city'], city['country_code'], city['latitude'], city['longitude'])
        )
    print("City data loaded into dim_city table.")
    conn.commit()
    print("Data committed to MySQL database.")

    date_data = context['task_instance'].xcom_pull(task_ids='transform_data', key='date_data')

    for date in date_data:
        cursor.execute(
            "INSERT INTO dim_date (date_id, year, month, day, day_of_week, is_weekend) VALUES (%s, %s, %s, %s, %s, %s)",
            (date['date_id'], date['year'], date['month'], date['day'], date['day_of_week'], date['is_weekend'])
        )

    print("Date data loaded into dim_date table.")
    conn.commit()

    weather_data = context['task_instance'].xcom_pull(task_ids='transform_data', key='weather_data')

    for weather in weather_data:
        cursor.execute(
            "INSERT INTO fact_weather (city_id, date_id, temperature, humidity) VALUES (%s, %s, %s, %s)",
            (weather['city_id'], weather['date_id'], weather['temperature'], weather['humidity'])
        )   

    print("Weather data loaded into fact_weather table.")
    conn.commit()
    print("Data committed to MySQL database.")

    print("Data committed to MySQL database.")
    conn.close()
    print("Connection closed.")

start_task = EmptyOperator(
    task_id='start_task',
    dag=dag,
    )

extract_task = PythonOperator(
    task_id='extract_task',
    python_callable=extract_data,
    provide_context=True,
    dag=dag,
    )

transform_task = PythonOperator(
    task_id='transform_data',
    python_callable=transform_data,
    provide_context=True,
    dag=dag,
    )

load_task = PythonOperator(
    task_id='load_data',
    python_callable=load_data,
    provide_context=True,
    dag=dag,
    )

end_task = EmptyOperator(
    task_id='end_task',
    dag=dag,
    )


start_task >> check_api >> extract_task  >> transform_task >> load_task >> end_task