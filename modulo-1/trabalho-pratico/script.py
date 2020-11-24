#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Sun Nov 22 23:16:40 2020

@author: lucas
"""


import os
from pyspark.sql.types import *
from pyspark.sql.functions import *
from pyspark.sql import SparkSession

global basePath

spark = SparkSession.builder.appName("Teste")\
    .config('spark.driver.extraClassPath', '/home/lucas/Documentos/github/Projetos/enem/postgresql-42.2.17.jar') \
    .config('spark.driver.memory', '4g') \
    .config('spark.executor.memory','1g')\
    .master("local[*]").getOrCreate()

def write_jdbc(df):
    df.write\
    .format("jdbc") \
    .option("url", "jdbc:postgresql://localhost:5432/dm_enem") \
    .option("dbtable", "public.tb_enem") \
    .option("user", "admin") \
    .option("password", "Amglsox123@") \
    .mode("overwrite")\
    .save()

def read_file(file):
    return spark.read.format('csv')\
    .option("charset", "iso-8859-1")\
    .option("header", "true")\
    .option("delimiter", ";")\
    .option("inferSchema", "True")\
    .load(file)\
    .createOrReplaceTempView('df_enem')

def read_metadata(basePath):
    df_tipos = spark.read.format('csv')\
    .option("charset", "iso-8859-1")\
    .option("header", "true")\
    .option("delimiter", ",")\
    .option("inferSchema", "False")\
    .load(basePath+'/raw/tipos-enem.csv')
    rdd_tipos = df_tipos.rdd.map(lambda x: (x.Coluna,x.Tipo))
    lista = list(map(lambda x: "CAST(COALESCE({}, {}) AS {}) AS {}".format(x[0],-3,x[1],x[0]) if x[1] == 'Long' else "UPPER(CAST(COALESCE({}, '{}') AS {})) AS {}".format(x[0],'N/A',x[1],x[0]), rdd_tipos.collect()))
    lista = list(filter(lambda x: x != 'CAST(COALESCE(IN_TEMPO_ADICIONAL, -3) AS Long) AS IN_TEMPO_ADICIONAL',lista))
    lista = ','.join(lista)
    return lista


def write_parquet(df, zone):
    df.write.mode('overwrite').parquet(basePath+'/{}/'.format(zone))

def read_parquet(zone):
    return spark.read.option("basePath",  basePath+"/"+zone).parquet(basePath+'/{}/NU_ANO=2018'.format(zone), 
                                                                     basePath+'/{}/NU_ANO=2019'.format(zone)).repartition(3000)
def read_parquet(zone):
    return spark.read.option("basePath",  basePath+"/"+zone).parquet(basePath+"/"+zone+"/*.parquet")

#def main():
basePath = os.getcwd()

path = [basePath,'MICRODADOS*.csv']
sep = '/'
pathRaw = sep.join(path)
read_file(pathRaw)
spark.sql("SELECT * FROM df_enem ").createOrReplaceTempView('df_enem')
df_minas = spark.sql("SELECT * FROM df_enem where SG_UF_RESIDENCIA ='MG'")
df_minas.count()
df_minas.printSchema()

df_filter = spark.sql('SELECT * FROM df_enem WHERE NU_ANO <> -3')
df_filter.rdd.getNumPartitions()
df_filter = df_filter.repartition(3000)
df_filter.rdd.getNumPartitions()

df_filter.write.mode("overwrite").partitionBy("NU_ANO").parquet(path[0]+'/{}'.format('staged'))
colunas_dropar = ['TX_RESPOSTAS_CN','TX_RESPOSTAS_CH','TX_RESPOSTAS_LC','TX_RESPOSTAS_MT','TX_GABARITO_CN','TX_GABARITO_CH','TX_GABARITO_LC','TX_GABARITO_MT','CO_PROVA_CN','CO_PROVA_CH','CO_PROVA_LC','CO_PROVA_MT','Q004']
df_staged_enem = read_parquet('staged')
df_curated_enem = df_staged_enem.select("*",
                       when(df_staged_enem.TP_COR_RACA==0, lit('N DECLARADO'))\
                      .when(df_staged_enem.TP_COR_RACA==1, lit('BRANCA'))\
                      .when(df_staged_enem.TP_COR_RACA==2, lit('PRETA'))\
                      .when(df_staged_enem.TP_COR_RACA==3, lit('PARDA'))\
                      .when(df_staged_enem.TP_COR_RACA==4, lit('AMARELA'))\
                      .when(df_staged_enem.TP_COR_RACA==5, lit('INDIGENA')).alias('NM_COR_RACA'),
                      
                      when(df_staged_enem.TP_ESTADO_CIVIL==0, lit('N INFORMADO'))\
                      .when(df_staged_enem.TP_ESTADO_CIVIL==1, lit('SOLTEIRA(O)'))\
                      .when(df_staged_enem.TP_ESTADO_CIVIL==2, lit('CASADO'))\
                      .when(df_staged_enem.TP_ESTADO_CIVIL==3, lit('DIVORCIADO'))\
                      .when(df_staged_enem.TP_ESTADO_CIVIL==4, lit('VIÚVA(O)')).alias('NM_ESTADO_CIVIL'),
                      
                      when(df_staged_enem.TP_NACIONALIDADE==0, lit('N INFORMADO'))\
                      .when(df_staged_enem.TP_NACIONALIDADE==1, lit('BRASILEIRA(O)'))\
                      .when(df_staged_enem.TP_NACIONALIDADE==2, lit('BRASILEIRA(O) NATURALIZADO'))\
                      .when(df_staged_enem.TP_NACIONALIDADE==3, lit('ESTRAGEIRA(O)'))\
                      .when(df_staged_enem.TP_NACIONALIDADE==4, lit('BRASILEIRA(O) NATA(O), NASCIDA(O) NO EXTERIOR')).alias('NM_NACIONALIDADE'),
                      
                      when(df_staged_enem.TP_ST_CONCLUSAO==1, lit('CONCLUIU ENS. MEDIO'))\
                      .when(df_staged_enem.TP_ST_CONCLUSAO==2, lit('CURSANDO ENS. MEDIO E CONCLUSÃO EM 2019'))\
                      .when(df_staged_enem.TP_ST_CONCLUSAO==3, lit('CURSANDO ENS. MEDIO E CONCLUSÃO APÓS 2019'))\
                      .when(df_staged_enem.TP_ST_CONCLUSAO==4, lit('NÃO CONCLUI E NÃO ESTOU CURSANDO ENS. MEDIO')).alias('NM_SITUACAO_ENS_MEDIO'),
                       
                       when(df_staged_enem.TP_ANO_CONCLUIU==1, lit('2018'))\
                        .when(df_staged_enem.TP_ANO_CONCLUIU==2, lit('2017'))\
                        .when(df_staged_enem.TP_ANO_CONCLUIU==3, lit('2016'))\
                        .when(df_staged_enem.TP_ANO_CONCLUIU==4, lit('2015'))\
                        .when(df_staged_enem.TP_ANO_CONCLUIU==5, lit('2014'))\
                        .when(df_staged_enem.TP_ANO_CONCLUIU==6, lit('2013'))\
                        .when(df_staged_enem.TP_ANO_CONCLUIU==7, lit('2012'))\
                        .when(df_staged_enem.TP_ANO_CONCLUIU==8, lit('2011'))\
                        .when(df_staged_enem.TP_ANO_CONCLUIU==9, lit('2010'))\
                        .when(df_staged_enem.TP_ANO_CONCLUIU==10,lit('2009'))\
                        .when(df_staged_enem.TP_ANO_CONCLUIU==11,lit('2008'))\
                        .when(df_staged_enem.TP_ANO_CONCLUIU==12,lit('2007'))\
                        .when(df_staged_enem.TP_ANO_CONCLUIU==13,lit('Antes de 2006'))\
                        .when(df_staged_enem.TP_ANO_CONCLUIU==0,lit('NÃO INFORMADO')).alias('NM_ANO_CONCLUSAO'),
                        
                     when(df_staged_enem.TP_DEPENDENCIA_ADM_ESC==1, lit('FEDERAL'))\
                      .when(df_staged_enem.TP_DEPENDENCIA_ADM_ESC==2, lit('ESTADUAL'))\
                      .when(df_staged_enem.TP_DEPENDENCIA_ADM_ESC==3, lit('MUNICIPAL'))\
                      .when(df_staged_enem.TP_DEPENDENCIA_ADM_ESC==4, lit('PRIVADA')).alias('NM_DEPENDENCIA_ADM_ESC'),
                     
                        when(df_staged_enem.TP_LOCALIZACAO_ESC==1, lit('URBANA'))\
                        .when(df_staged_enem.TP_LOCALIZACAO_ESC==2, lit('RURAL')).otherwise('N/A').alias('NM_LOCALIZACAO_ESC'),
                     ).distinct()
write_parquet(df_curated_enem,'curated')
df_curated_enem = read_parquet('curated')

write_jdbc(df_curated_enem)


df_curated_enem.printSchema()