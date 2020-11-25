from airflow import DAG
from airflow.operators.bash_operator import BashOperator
from airflow.operators.python_operator import PythonOperator
from datetime import datetime, timedelta
import pandas as pd

default_args = {
    "owner": "Lucas Mari",
    "depends_on_past": False,
    "start_date": datetime(2020,11, 25, 18),
    "email": ["airflow@airflow.com"],
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=2)
}

def calculate_mean_age():
    df = pd.read_csv('~/data/train.csv')
    return df.Age.mean()

def print_age(**context):
    value = context['task_instance'].xcom_pull(task_ids='calcula-idade-media')
    print(f"A idade média no titanic era {value} anos.")

dag = DAG("treino-02",
          description="Extrai dados do Titanic da internet e calcula a idade média", 
          default_args=default_args, 
          schedule_interval="@once")

step_get_data = BashOperator(
                task_id = "get-data",
                bash_command = "curl https://raw.githubusercontent.com/A3Data/hermione/master/hermione/file_text/train.csv -o ~/data/train.csv",
                dag=dag
)

step_caculo_idade_media = PythonOperator(
                          task_id = "calcula-idade-media",
                          python_callable = calculate_mean_age,
                          dag=dag
)
step_print_idade = PythonOperator(
                          task_id = "mostra-idade",
                          python_callable = print_age,
                          provide_context = True,
                          dag=dag
)

step_get_data >> step_caculo_idade_media >> step_print_idade 