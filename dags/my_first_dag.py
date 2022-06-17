from datetime import datetime, timedelta, date

import requests as req
from airflow import DAG
from airflow.operators.python import PythonOperator
import os
from pyspark.sql import SparkSession

from pyspark.context import SparkContext
from pyspark.sql.session import SparkSession
sc = SparkContext.getOrCreate()
spark = SparkSession(sc)


HOME = os.path.expanduser('~')
DATALAKE_ROOT_FOLDER = HOME + "/airflow/"

current_day = date.today().strftime("%Y%m%d")

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
       url1 = 'https://www.data.gouv.fr/fr/datasets/r/5ac33ad1-6782-4618-9a51-293f9c2db1d4'
       file = req.get(url1, allow_redirects=True)
       TARGET_PATH = DATALAKE_ROOT_FOLDER + "raw/source1/"
       if not os.path.exists(TARGET_PATH):
           os.makedirs(TARGET_PATH)
       open(TARGET_PATH + 'dataSource1.csv', 'wb').write(file.content)
       file.close()


   def taskLoadDataSource2():
       TARGET_PATH = DATALAKE_ROOT_FOLDER + "/raw/source2/imdb/MovieRating/"
       if not os.path.exists(TARGET_PATH):
           os.makedirs(TARGET_PATH)

       url = 'https://datasets.imdbws.com/title.ratings.tsv.gz'
       r = req.get(url, allow_redirects=True)
       open(TARGET_PATH + 'title.ratings.tsv.gz', 'wb').write(r.content)


   def taskFormattedDataSource1():
       #read raw csv file
       RAW_PATH = DATALAKE_ROOT_FOLDER + "raw/source1/"
       TARGET_PATH = DATALAKE_ROOT_FOLDER + "formatted/source1/"
       df = spark.read.csv(RAW_PATH + "dataSource1.csv")
       df.write.parquet(TARGET_PATH + "dataSource1.parquet")



   t1 = PythonOperator(
       task_id='taskLoadDataSource1',
       python_callable=taskLoadDataSource1
   )

   t2 = PythonOperator(
       task_id='taskLoadDataSource2',
       python_callable=taskLoadDataSource2
   )

   t3 = PythonOperator(
       task_id='taskFormattedDataSource1',
       python_callable=taskFormattedDataSource1
   )

   t1 >> t2
   t2 >> t3



