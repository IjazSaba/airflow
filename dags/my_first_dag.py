from datetime import datetime, timedelta

import requests
from airflow import DAG
from airflow.operators.python import PythonOperator
import pandas as pd


with DAG(
       'my_first_dag',
       default_args={
           'depends_on_past': False,
           'email': ['airflow@example.com'],
           'email_on_failure': False,
           'email_on_retry': False,
           'retries': 1,
           'retry_delay': timedelta(minutes=5),
       },
       description='A first DAG',
       schedule_interval=None,
       start_date=datetime(2021, 1, 1),
       catchup=False,
       tags=['example'],
) as dag:
   dag.doc_md = """
       This is my first DAG in airflow.
       I can write documentation in Markdown here with **bold text** or __bold text__.
   """


   def taskLoadDataSource1():
       url = 'https://static.data.gouv.fr/resources/donnees-hospitalieres-relatives-a-lepidemie-de-covid-19/20220615-190218/covid-hosp-txad-age-fra-2022-06-15-19h02.csv'
       response = urlopen(url)
       csvfile = csv.reader(codecs.iterdecode(response, 'utf-8'))

       for row in csvfile:
           print(row)

       """
       url = 'https://data.iledefrance.fr/explore/dataset/les_salles_de_cinemas_en_ile-de-france/download?format=csv&timezone=Europe/Berlin&use_labels_for_header=false'
       req =  requests.get(url)
       csv_file = open('/home/saba/airflow/resources/data1.csv','wb')
       #csv_file = open('C:/Users/sabai/OneDrive/Bureau/2A - ISEP/S2/BDD/Big Data/data1.csv','wb')

       csv_file.write(url)
       csv_file.close()
       return 1
       """

   def taskLoadDataSource2():
       df = pd.read_csv('//wsl.localhost/Ubuntu-18.04/home/saba/airflow/resources/data1.csv', sep=',')
       df.head()

   t1 = PythonOperator(
       task_id='taskLoadDataSource1',
       python_callable=taskLoadDataSource1
   )

   t2 = PythonOperator(
       task_id='taskLoadDataSource2',
       python_callable=taskLoadDataSource2
   )

   t1 >> t2


