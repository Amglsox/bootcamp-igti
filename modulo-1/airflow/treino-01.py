from airflow import DAG
from airflow.operators.bash_operator import BashOperator
from airflow.operators.python_operator import PythonOperator
from datetime import datetime, timedelta


default_args = {
    "owner": "Lucas Mari",
    "depends_on_past": False,
    "start_date": datetime(2020,11, 25, 14,40),
    "email": ["airflow@airflow.com"],
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=2),
    # 'queue': 'bash_queue',
    # 'pool': 'backfill',
    # 'priority_weight': 10,
    # 'end_date': datetime(2016, 1, 1),
}

def say_hello():
    print("Hello World - Python")

dag = DAG("treino-01", description="Básico de Bash e Python", default_args=default_args, schedule_interval=timedelta(minutes=1))

hello_bash = BashOperator(task_id="bash_basic", 
                          bash_command='echo "Hello World Bash"', 
                          dag=dag)

hello_python = PythonOperator(task_id="python_basic", 
                            python_callable=say_hello, 
                            dag=dag)


hello_bash >> hello_python