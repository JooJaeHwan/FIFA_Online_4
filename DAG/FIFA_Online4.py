import requests
import pandas as pd
import io
import boto3
import findspark
import subprocess
from pendulum.tz.timezone import Timezone
from datetime import datetime, timedelta
from pyspark.sql import SparkSession
from pyspark.sql.functions import asc, desc
from airflow.operators.python import PythonOperator
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
from airflow import DAG
from airflow.operators.dummy import DummyOperator
from airflow.hooks.S3_hook import S3Hook


kst = Timezone('Asia/Seoul')

dag = DAG(
    dag_id = "FIFA_Online4",
    description = "FIFA Online 4 Match Detail Analysis",
    start_date = datetime(2022,12,26, tzinfo = kst),
    end_date = datetime(2022,12,30, tzinfo = kst),
    dagrun_timeout=timedelta(minutes=60),
    schedule_interval = '0 * * * *'
)

dag2 = DAG(
    dag_id = "Position_Count",
    start_date = datetime(2022,12,26, tzinfo = kst),
    end_date = datetime(2022,12,30, tzinfo = kst),
    dagrun_timeout=timedelta(minutes=300),
    schedule_interval = None
)

start = DummyOperator(task_id = 'start', dag = dag)


# 스파크 연동
def get_spark_session():
    findspark.init()
    app_name = "FIFA_Online4"
    master = "local[*]"
    spark = SparkSession.builder \
        .appName(app_name) \
        .master(master) \
        .getOrCreate()
 
    return spark

    
# 포지션 정보 가져오기
def _Position_Crolling():
    headers = {'Authorization' : 'API 키'}
    position_url = "https://static.api.nexon.co.kr/fifaonline4/latest/spposition.json"
    data = requests.get(position_url, headers=headers)
    position = data.json()
    position_list = []
    for p in position:
        position_list.append(p["desc"])
    return position_list

def _Match_Crolling(i, **context):
    match_list = [] 
    position_list = context["task_instance"].xcom_pull(task_ids='position_crolling')
    s3_client = boto3.client(service_name="s3",
                         aws_access_key_id="AWS 아이디",
                         aws_secret_access_key="AWS 키")

    obj = s3_client.get_object(Bucket="fifaonline4", Key="data/player_data/player_data_df.parquet")
    df = pd.read_parquet(io.BytesIO(obj["Body"].read()))


    headers = {'Authorization' : 'API 키'} 
    # Match_ID 받아오는 API 주소
    Match_ID_url = f'https://api.nexon.co.kr/fifaonline4/v1.0/matches?matchtype=50&offset={i*100}&limit=100&orderby=desc'
    data = requests.get(Match_ID_url, headers=headers)
    match_id = data.json()
    if len(match_id) == 0:
        return 0
    for mi in match_id:
        Match_url = f'https://api.nexon.co.kr/fifaonline4/v1.0/matches/{mi}'
        match_data = requests.get(Match_url, headers=headers)
        match = match_data.json()
        if match == {'message': '{matchid} could not found'}:
            continue
        match_id = match['matchId']
        match_date = match['matchDate'].split("T")[0]
        
        if match['matchInfo']== []:
            continue
        for j in range(len(match['matchInfo'])):
            seasonid = match['matchInfo'][j]['matchDetail']['seasonId']
            if len(match['matchInfo'][j]['player']) == 0:
                continue
            for player in match['matchInfo'][j]['player']:
                if player['spPosition'] == 28:
                    continue
                if len(df.loc[df['spid'] == player['spId']]) != 0:
                    name = df.loc[df['spid'] == player['spId']]['name'].to_list()
                    season= df.loc[df['spid'] == player['spId']]['season_class_name'].to_list()
                    match_list.append({'match_id' : match_id, 'match_date' : match_date, 'season_id' : seasonid, 'position_id' : position_list[player['spPosition']], 'player_name' : name[0], 'player_season' : season[0], 'spid' : player['spId']})
                else: continue
    if match_list == []:
        return 0
    file_path = f"hdfs://localhost:9000/user/jjwani/FIFA4/data/match/{datetime.now().date()}/temporary/match_{str(datetime.now().hour).zfill(2)}hour_{f'{i}'.zfill(2)}.parquet"
    # HDFS에 파일 저장
    spark = get_spark_session()
    
    match_df = spark.createDataFrame(match_list)   
    match_df.write.mode('overwrite').parquet(file_path)
    

def _hours_union():
    spark = get_spark_session()
    result = spark.read.parquet(f'hdfs://localhost:9000/user/jjwani/FIFA4/data/match/{datetime.now().date()}/temporary/match_{str(datetime.now().hour - 1).zfill(2)}hour_01.parquet')
    for i in range(2, 6):
        df = spark.read.parquet(f'hdfs://localhost:9000/user/jjwani/FIFA4/data/match/{datetime.now().date()}/temporary/match_{str(datetime.now().hour - 1).zfill(2)}hour_0{i}.parquet')
        result = result.union(df)
        file_path = f"hdfs://localhost:9000/user/jjwani/FIFA4/data/match/{datetime.now().date()}/temporary/match_{str(datetime.now().hour - 1).zfill(2)}hour.parquet"
        result.write.mode("overwrite").parquet(file_path)    

def _hours_delete():
    for i in range(1, 6):
        hour = str(datetime.now().hour - 1).zfill(2)
        if hour == '-1':
            return "00시"
        args = f"hdfs dfs -rm -r /user/jjwani/FIFA4/data/match/{datetime.now().date()}/temporary/match_{hour}hour_0{i}.parquet"
        proc = subprocess.Popen(args, stdout=subprocess.PIPE, stderr=subprocess.PIPE, shell=True)
        s_output, s_err = proc.communicate()

def _union():
    spark = get_spark_session()
    args = f"hdfs dfs -ls /user/jjwani/FIFA4/data/match/{datetime.now().date() - timedelta(days=1)}| awk '{{print $8}}'"
    proc = subprocess.Popen(args, stdout=subprocess.PIPE, stderr=subprocess.PIPE, shell=True)
    s_output, s_err = proc.communicate()
    all_dart_dirs = s_output.decode('ascii').split()
    if f'/user/jjwani/FIFA4/data/match/{datetime.now().date() - timedelta(days=1)}/match_union_{datetime.now().date() - timedelta(days=1)}.parquet' not in all_dart_dirs:
        args2 = f"hdfs dfs -ls /user/jjwani/FIFA4/data/match/{datetime.now().date() - timedelta(days=1)}/temporary| awk '{{print $8}}'"
        proc2 = subprocess.Popen(args2, stdout=subprocess.PIPE, stderr=subprocess.PIPE, shell=True)
        s_output2, s_err = proc2.communicate()
        all_dart_dirs2 = s_output2.decode('ascii').split()
        result = spark.read.parquet(f'hdfs://localhost:9000{all_dart_dirs2[0]}')
        for dir in all_dart_dirs2[1:]:
            df = spark.read.parquet(f'hdfs://localhost:9000{dir}')
            result = result.union(df)
        file_path = f"hdfs://localhost:9000/user/jjwani/FIFA4/data/match/{datetime.now().date() - timedelta(days=1)}/match_union_{datetime.now().date() - timedelta(days=1)}.parquet"
        result.write.mode("overwrite").parquet(file_path)    
        spark.stop()
        return all_dart_dirs
    else:
        spark.stop()
        return "이미 합쳐짐"


def _delete(**context):
    result = context["task_instance"].xcom_pull(task_ids='union')
    if result != "이미 합쳐짐" and result != "전날 데이터 없음":
        args = f"hdfs dfs -rm -r /user/jjwani/FIFA4/data/match/{datetime.now().date() - timedelta(days=1)}/temporary"
        proc = subprocess.Popen(args, stdout=subprocess.PIPE, stderr=subprocess.PIPE, shell=True)
        s_output, s_err = proc.communicate()
        return print(s_output)
    else:print('이미 합쳐짐')


position_crolling = PythonOperator(
task_id="position_crolling",
python_callable=_Position_Crolling,
dag=dag
)

hours_union = PythonOperator(
task_id="hours_union",
python_callable=_hours_union,
dag=dag
)

hours_delete = PythonOperator(
task_id="hours_delete",
python_callable=_hours_delete,
dag=dag
)

union = PythonOperator(
task_id="union",
python_callable=_union,
dag=dag
)

delete = PythonOperator(
task_id="delete",
python_callable=_delete,
dag=dag
)


trigger_dag = TriggerDagRunOperator(
    task_id="trigger_dag",
    trigger_dag_id="Position_Count",
    dag=dag
)

end = DummyOperator(task_id = 'end', dag = dag)

for i in range(1, 6):
    


    player_data_write = PythonOperator(
        task_id=f"match_crolling_{f'{i}'.zfill(2)}",
        python_callable=_Match_Crolling,
        op_kwargs={"i":i},
        dag=dag
    )

    

    start >> position_crolling >> player_data_write >> hours_union >> hours_delete >> union >> delete >> trigger_dag >> end

position_dic = {1:"GK", 2:"RWB", 3:"RB", 4:"RCB", 5:"CB", 6:"SW", 7:"LCB", 8:"LB", 9:"LWB",
                10:"LDM", 11:"CDM", 12:"RDM", 13:"LCM", 14:"LM", 15:"CM", 16:"RM", 17:"RCM", 18:"LAM",
                19:"CAM", 20:"RAM", 21:"LW", 22:"LF", 23:"CF", 24:"RF", 25:"RW", 26:"LS", 27:"ST", 28:"RS"}




def _upload_to_s3(position):
    hook = S3Hook('fifaonline4')
    spark = get_spark_session()
    args = f"hdfs dfs -ls /user/jjwani/FIFA4/data/match/{datetime.now().date() - timedelta(days=1)}/match_union_{datetime.now().date() - timedelta(days=1)}.parquet| awk '{{print $8}}'"
    proc = subprocess.Popen(args, stdout=subprocess.PIPE, stderr=subprocess.PIPE, shell=True)
    s_output, s_err = proc.communicate()
    all_dart_dirs = s_output.decode('ascii').split()
    if all_dart_dirs == []:
        return print('데이터 없음')
    df = spark.read.parquet(f"hdfs://localhost:9000/user/jjwani/FIFA4/data/match/{datetime.now().date() - timedelta(days=1)}/match_union_{datetime.now().date() - timedelta(days=1)}.parquet")
    positon_df = df.groupBy(["player_name","player_season", "position_id"]).count().where(df.position_id == f"{position}").orderBy(desc("count"))
    positon_df = positon_df.toPandas()
    buffer = io.BytesIO()
    positon_df.to_parquet(buffer, index=False)
    buffer.seek(0, 0)
    hook.load_file_obj(file_obj = buffer, key=f'data/position/{position}/{datetime.now().date() - timedelta(days=1)}.parquet', bucket_name='fifaonline4', replace=True)

for i in range(1, 29):
    count_data_upload = PythonOperator(
    task_id=f"count_data_upload_{position_dic[i]}",
    python_callable=_upload_to_s3,
    op_kwargs={"position":position_dic[i]},
    dag=dag2
    )

    count_data_upload