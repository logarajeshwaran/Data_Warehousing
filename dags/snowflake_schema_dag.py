from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.empty import EmptyOperator
from airflow.providers.http.sensors.http import HttpSensor
from airflow.providers.slack.operators.slack_webhook import SlackWebhookOperator
from airflow.providers.mysql.hooks.mysql import MySqlHook
from airflow.models import Variable
from datetime import datetime
import requests
import sys
import os

sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__),'..')))
from scripts.snowflake_schema.transform_data import extract_show_data
from scripts.snowflake_schema.schema_ddl import DIM_COUNTRY, DIM_SHOW,DROP_TABLES

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


def extract_data_from_api(page, **kwargs):
    url = Variable.get('API_URL')
    result = []
    output_path = '/opt/airflow/data/tv_shows_extracted_data.json'
    request_url = f"{url}/shows?page={page}"
    response = requests.get(request_url, params={'appid': Variable.get('API_KEY_TV_MAZE')})
    if response.status_code == 200:
        result.append(response.json())
    else:
        result.append({'error': response.text})
    
    with open(output_path, 'w') as f:
        import json
        json.dump(result[0], f, indent=2)
    f.close()
    kwargs['ti'].xcom_push(key='extracted_data', value=output_path)


def transform_data(**kwargs):
    input_path = kwargs['ti'].xcom_pull(task_ids='extract_data', key='extracted_data')
    transformed_data = extract_show_data(input_path)
    show_df, country_df = transformed_data
    kwargs['ti'].xcom_push(key='show_data', value=show_df)
    kwargs['ti'].xcom_push(key='country_data', value=country_df)

def load_data_to_mysql(**kwargs):
    show_data = kwargs['ti'].xcom_pull(task_ids='transform_data', key='show_data')
    country_data = kwargs['ti'].xcom_pull(task_ids='transform_data', key='country_data')
    print("show_data", show_data)
    print("country_data", country_data)
    mysql_hook = MySqlHook(mysql_conn_id='mysql_default')
    conn = mysql_hook.get_conn()
    cursor = conn.cursor()
    cursor.execute(DROP_TABLES)
    cursor.execute(DIM_COUNTRY)
    cursor.execute(DIM_SHOW)
    for country in country_data:
        print("country", country)
        cursor.execute("INSERT INTO dim_country (country_name) VALUES (%s)", (country['country_name'],))
        conn.commit()

    for show in show_data:
        print("show", show)
        cursor.execute("INSERT INTO dim_show (tvmaze_id, name) VALUES (%s, %s)", (show['tvmaze_id'], show['show_name']))
        conn.commit()

    print(f"Inserted {cursor.rowcount} rows into dim_show and dim_country tables.")
    cursor.close()
    conn.close()



dag = DAG(
    'snowflake_schema_dag',
    start_date=datetime(2025, 5, 1),
    schedule_interval=None,
    catchup=False,
    on_failure_callback=task_fail_slack_alert,
    on_success_callback=dag_success_slack_alert,
)


start_task = EmptyOperator(
    task_id='start_task',
    dag=dag,
)



api_check = HttpSensor(
    task_id='check_api',
    http_conn_id='tvmaze_api',
    endpoint='shows',
    request_params={
       'page' : 1,
        'appid': Variable.get('API_KEY_TV_MAZE')
    },
    response_check=lambda response: 'name' in response.text,
    poke_interval=5,
    timeout=20,
    dag=dag,
)


extract_data = PythonOperator(
    task_id='extract_data',
    python_callable=extract_data_from_api,
    op_kwargs={'page': 20},
    provide_context=True,
    dag=dag,
)

transform_data_task = PythonOperator(
    task_id='transform_data',
    python_callable=transform_data,
    provide_context=True,
    dag=dag,
)


load_data_task = PythonOperator(
    task_id='load_data',
    python_callable=load_data_to_mysql,
    provide_context=True,
    dag=dag,
)

end_task = EmptyOperator(
    task_id='end_task',
    dag=dag,
)


start_task >> api_check >> extract_data >> transform_data_task >> load_data_task >> end_task