from airflow import DAG
from airflow.operators.bash_operator import BashOperator
from airflow.operators.python_operator import PythonOperator, BranchPythonOperator
from datetime import datetime, timedelta
import pandas as pd
import zipfile
import sqlalchemy
#constantes

data_path = '/usr/local/airflow/data/microdados_enade_2019/2019/3.DADOS/'
arquivo = data_path + 'microdados_enade_2019.txt'

def aplica_filtros():
    cols = ['CO_GRUPO','TP_SEXO', 'NU_IDADE', 'NT_GER', 'NT_FG', 
            'NT_CE', 'QE_I01', 'QE_I02', 'QE_I04', 'QE_I05', 'QE_I08']
    enade = pd.read_csv(arquivo,sep=';',decimal=',',usecols=cols)
    enade = enade.loc[(enade.NU_IDADE > 20) &
                      (enade.NU_IDADE < 40) &
                      (enade.NT_GER > 0)]
    enade.to_csv(data_path + 'enade_filtrado.csv', index=False)

def unzip_file():
    with zipfile.ZipFile('/usr/local/airflow/data/microdados_enade_2019.zip','r') as zipped:
        zipped.extractall('/usr/local/airflow/data/')

# Idade Centralizada na media
def constroi_idade_centralizada():
    idade = pd.read_csv(data_path + 'enade_filtrado.csv', usecols=['NU_IDADE'])
    idade['idadecent'] = idade.NU_IDADE - idade.NU_IDADE.mean()
    idade[['idadecent']].to_csv(data_path + 'idadecent.csv', index=False)

# Idade Centralizada ao Quadrado
def constroi_idade_cent_quad():
    idadecent = pd.read_csv(data_path + 'idadecent.csv')
    idadecent['idade2'] = idadecent.idadecent ** 2
    idadecent[['idade2']].to_csv(data_path + 'idadequadrado.csv', index=False)

def constroi_est_civil():
    filtro = pd.read_csv(data_path + 'enade_filtrado.csv', usecols=['QE_I01'])
    filtro['estcivil'] = filtro.QE_I01.replace({
        'A': 'Solteiro',
        'B': 'Casado',
        'C': 'Separado',
        'D': 'Viuvo',
        'E': 'Outro'
    })
    filtro[['estcivil']].to_csv(data_path + 'estcivil.csv', index=False)

def constroi_cor():
    filtro = pd.read_csv(data_path + 'enade_filtrado.csv', usecols=['QE_I02'])
    filtro['cor'] = filtro.QE_I02.replace({
        'A': 'Branca',
        'B': 'Preta',
        'C': 'Amarela',
        'D': 'Parda',
        'E': 'Indigena',
        'F': "",
        ' ': ""
    })
    filtro[['cor']].to_csv(data_path + 'cor.csv', index=False)

# Task de Join

def join_data():
    filtro = pd.read_csv(data_path + 'enade_filtrado.csv')
    idadecent = pd.read_csv(data_path + 'idadecent.csv')
    idadeaoquadrado = pd.read_csv(data_path + 'idadequadrado.csv')
    estcivil = pd.read_csv(data_path + 'estcivil.csv')
    cor = pd.read_csv(data_path + 'cor.csv')

    final = pd.concat([
        filtro, idadecent, idadeaoquadrado, estcivil, cor
    ], axis=1)
    final.to_csv(data_path + 'enade_tratado.csv',index=False)
    print(final)

def escreve_dw():
    final = pd.read_csv(data_path + 'enade_tratado.csv')
    engine = sqlalchemy.create_engine("mssql+pyodbc://SA:Admin@127.0.0.1/enade?driver=ODBC+Driver+17+for+SQL+Server")
    final.to_sql("tratado",con=engine, index=False, if_exists='append')

default_args = {
    "owner": "Lucas Mari",
    "depends_on_past": False,
    "start_date": datetime(2020,11, 25, 20),
    "email": ["airflow@airflow.com"],
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=2)
}

dag = DAG("treino-05",
          description="Paralelismos", 
          default_args=default_args,
          schedule_interval = None
)

start_preprocessing = BashOperator(
    task_id = 'start_processing',
    bash_command = 'echo "Start!!"',
    dag = dag
)

step_get_data = BashOperator(
                task_id = "get-data",
                bash_command = "curl http://download.inep.gov.br/microdados/Enade_Microdados/microdados_enade_2019.zip -o /usr/local/airflow/data/microdados_enade_2019.zip",
                dag=dag
)

step_unzip = PythonOperator(
    task_id = 'unzip_data',
    python_callable=unzip_file,
    dag=dag
)
step_filtro = PythonOperator(
    task_id = 'aplica_filtro',
    python_callable=aplica_filtros,
    dag=dag
)

step_idade_centralizada = PythonOperator(
    task_id = 'constroi_idade_centralizada',
    python_callable=constroi_idade_centralizada,
    dag=dag
)

step_idade_quad = PythonOperator(
    task_id = 'constroi_idade_ao_quadrado',
    python_callable=constroi_idade_cent_quad,
    dag=dag
)

step_estado_civil = PythonOperator(
    task_id = 'constroi_estado_civil',
    python_callable=constroi_est_civil,
    dag=dag
)

step_cor = PythonOperator(
    task_id = 'constroi_cor_da_pele',
    python_callable=constroi_cor,
    dag=dag
)
step_join = PythonOperator(
    task_id = 'join_data',
    python_callable=join_data,
    dag=dag
)
step_write = PythonOperator(
    task_id = 'write_dw',
    python_callable=escreve_dw,
    dag=dag
)


start_preprocessing >> step_get_data >> step_unzip >> step_filtro 
step_filtro >> [step_idade_centralizada, step_estado_civil, step_cor]
step_idade_quad.set_upstream(step_idade_centralizada)

step_join.set_upstream([
    step_idade_quad, step_estado_civil, step_cor
    ])

step_join >> step_write