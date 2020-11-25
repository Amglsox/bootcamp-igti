from datetime import datetime, timedelta
import pendulum
import prefect
from prefect import task, Flow
from prefect.schedules import IntervalSchedule, CronSchedule
import pandas as pd
from io import BytesIO
import zipfile
import requests
import pyodbc
import sqlalchemy

schedule = CronSchedule(
    cron = "*/10 * * * *",
    start_date=pendulum.datetime(2020, 11, 25, 18, 40, tz="America/Sao_Paulo")
)

@task
def get_raw_data():
    url = "http://download.inep.gov.br/microdados/Enade_Microdados/microdados_enade_2019.zip" 
    filebytes = BytesIO(requests.get(url).content)

    myzip = zipfile.ZipFile(filebytes)
    myzip.extractall()
    path = './microdados_enade_2019/2019/3.DADOS/'
    return path

@task
def aplica_filtros(path):
    cols = ['CO_GRUPO','TP_SEXO', 'NU_IDADE', 'NT_GER', 'NT_FG', 
            'NT_CE', 'QE_I01', 'QE_I02', 'QE_I04', 'QE_I05', 'QE_I08']
    enade = pd.read_csv(path + 'microdados_enade_2019.txt',sep=';',decimal=',',usecols=cols)
    enade = enade.loc[(enade.NU_IDADE > 20) &
                      (enade.NU_IDADE < 40) &
                      (enade.NT_GER > 0)]
    return enade

@task
def constroi_idade_centralizada(df):
    idade = df[['NU_IDADE']]
    idade['idadecent'] = idade.NU_IDADE - idade.NU_IDADE.mean()
    return idade[['idadecent']]

@task
def constroi_idade_cent_quad(df):
    idadecent = df.copy()
    idadecent['idade2'] = idadecent.idadecent ** 2
    return idadecent[['idade2']]
@task
def constroi_est_civil(df):
    filtro = df[['QE_I01']]
    filtro['estcivil'] = filtro.QE_I01.replace({
        'A': 'Solteiro',
        'B': 'Casado',
        'C': 'Separado',
        'D': 'Viuvo',
        'E': 'Outro'
    })
    return filtro[['estcivil']]

@task
def constroi_cor(df):
    filtro = df[['QE_I02']]
    filtro['cor'] = filtro.QE_I02.replace({
        'A': 'Branca',
        'B': 'Preta',
        'C': 'Amarela',
        'D': 'Parda',
        'E': 'Indigena',
        'F': "",
        ' ': ""
    })
    return filtro[['cor']]

@task
def join_data(df, idadecent, idadequadrado, cor, estcivil):
    final = pd.concat([df, idadecent, idadequadrado, cor, estcivil], axis=1)
    final = final[['CO_GRUPO','TP_SEXO','cor','estcivil','idadecent','idade2']]
    logger = prefect.context.get("logger")
    logger.info(final.head().to_json())
    final.to_csv('enade_tratado.csv', index=False)
    return final

@task
def escreve_dw(df):
    engine = sqlalchemy.create_engine("mssql+pyodbc://SA:Admin@127.0.0.1/enade?driver=ODBC+Driver+17+for+SQL+Server")
    df.to_sql("tratado",con=engine, index=False, if_exists='append', method='multi')

with Flow("Enade_dw", schedule) as flow:
    path = get_raw_data()
    filtro = aplica_filtros(path)
    idadecent = constroi_idade_centralizada(filtro)
    idadequadrado = constroi_idade_cent_quad(idadecent)
    estcivil = constroi_est_civil(filtro)
    cor = constroi_cor(filtro)
    j = join_data(filtro, idadecent, idadequadrado, cor, estcivil)
    escreve_dw(j)

flow.register(project_name="project-name", idempotency_key=flow.serialized_hash())
flow.run_agent(token="toke-generate")
