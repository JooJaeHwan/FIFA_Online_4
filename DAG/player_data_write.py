import requests
import pandas as pd
import io
from airflow.operators.dummy import DummyOperator
from airflow.operators.python import PythonOperator
from airflow import DAG
from airflow.hooks.S3_hook import S3Hook
from datetime import datetime

dag = DAG(
    dag_id = "player_data_write",
    description = "Player Data Crolling",
    start_date = datetime.now().date(),
    schedule_interval = "0 0 25 * *"
)


def _Season_Class_Crolling():
    season_class = {}
    headers = {'Authorization' : 'API 키'} 
    season_url = "https://static.api.nexon.co.kr/fifaonline4/latest/seasonid.json"
    data = requests.get(season_url, headers=headers)
    season = data.json()
    for s in season:
        season_class[s["seasonId"]] = s["className"].split(" (")[0]
    return season_class

def _player_data_write(**context):
    headers = {'Authorization' : 'API 키'} 
    player_url = "https://static.api.nexon.co.kr/fifaonline4/latest/spid.json"
    data = requests.get(player_url, headers=headers)
    player = data.json()
    player_data =[]
    season_class = context['ti'].xcom_pull(task_ids='season_class_crolling')
    for p in player:
        player_data.append({'spid' : p["id"], 'name' : p["name"], 'season_class_name' : season_class[str(int(p["id"])//1000000)]}) 	

    return player_data


def _upload_to_s3(**context):
    hook = S3Hook('fifaonline4')
    player_data = context['ti'].xcom_pull(task_ids='player_data_write')
    df = pd.DataFrame(player_data)  
    buffer = io.BytesIO()
    df.to_parquet(buffer, index=False)
    buffer.seek(0, 0)
    hook.load_file_obj(file_obj = buffer, key='data/player_data/player_data_df.parquet', bucket_name='fifaonline4', replace=True)

start = DummyOperator(task_id = 'start', dag = dag)

season_class_crolling = PythonOperator(
    task_id="season_class_crolling",
    python_callable=_Season_Class_Crolling,
    dag=dag
)

player_data_write = PythonOperator(
    task_id="player_data_write", 
    python_callable=_player_data_write, 
    dag=dag
)

upload = PythonOperator(
    task_id = 'upload',
    python_callable = _upload_to_s3,
    dag = dag
)

end = DummyOperator(task_id = 'end', dag = dag)


start >> season_class_crolling >> player_data_write >> upload >> end