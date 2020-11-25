from airflow import DAG
from airflow.operators.bash_operator import BashOperator
from airflow.operators.python_operator import PythonOperator, BranchPythonOperator
from datetime import datetime, timedelta
import pandas as pd
import random

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
    df = pd.read_csv('/usr/local/airflow/data/train.csv')
    return df.Age.mean()

def print_age(**context):
    value = context['task_instance'].xcom_pull(task_ids='calcula-idade-media')
    print(f"A idade média no titanic era {value} anos.")

def sorteia_h_m():
    return random.choice(['male','female'])

def MouF(**context):
    value = context['task_instance'].xcom_pull(task_ids='escolhe-h-m')
    if value == 'male':
        return 'branch_homem'
    if value == 'female':
        return 'branch_mulher'

def mean_homem():
    df = pd.read_csv('/usr/local/airflow/data/train.csv')
    df = df.loc[df['Sex']=='Male']
    print(f'A média de idade dos homens no Titanic: {df.Age.mean()}')

def mean_mulher():
    df = pd.read_csv('/usr/local/airflow/data/train.csv')
    df = df.loc[df['Sex']=='Female']
    print(f'A média de idade das mulheres no Titanic: {df.Age.mean()}')

dag = DAG("treino-03",
          description="Extrai dados do Titanic da internet e calcula a idade média para homens ou mulheres", 
          default_args=default_args, 
          schedule_interval="@once")

step_get_data = BashOperator(
                task_id = "get-data",
                bash_command = "curl https://raw.githubusercontent.com/A3Data/hermione/master/hermione/file_text/train.csv -o /usr/local/airflow/data/train.csv",
                dag=dag
)

branch_homem = PythonOperator(
                          task_id="branch_homem",
                          python_callable = mean_homem,
                          dag=dag
                          )

branch_mulher = PythonOperator(
                          task_id="branch_mulher",
                          python_callable = mean_mulher,
                          dag=dag
                          )
                          
step_sorteio = PythonOperator(
                        task_id='escolhe-h-m',
                        python_callable = sorteia_h_m,
                        dag=dag
                        )
male_female = BranchPythonOperator(
                        task_id = 'condicional',
                        python_callable = MouF,
                        provide_context = True,
                        dag=dag
                        )

step_get_data >> step_sorteio >> male_female >> [branch_homem, branch_mulher]
