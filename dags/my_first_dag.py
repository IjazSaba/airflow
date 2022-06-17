from datetime import datetime, timedelta, date

import requests as req
from airflow import DAG
from airflow.operators.python import PythonOperator
import os
from pyspark.context import SparkContext
from pyspark.sql.session import SparkSession
import pandas as pd
from lib.CombinedData import combine_data


HOME = os.path.expanduser('~')
DATALAKE_ROOT_FOLDER = HOME + "/airflow/"

current_day = date.today().strftime("%Y%m%d")

sc = SparkContext.getOrCreate()
spark = SparkSession(sc)


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
       TARGET_PATH = DATALAKE_ROOT_FOLDER + "raw/source2/imdb/MovieRating/" + current_day + "/"
       if not os.path.exists(TARGET_PATH):
           os.makedirs(TARGET_PATH)

       url = 'https://datasets.imdbws.com/title.ratings.tsv.gz'
       r = req.get(url, allow_redirects=True)
       open(TARGET_PATH + 'title.ratings.tsv.gz', 'wb').write(r.content)


   def taskFormattedDataSource1():
       RAW_PATH = DATALAKE_ROOT_FOLDER + "raw/source1/"
       TARGET_PATH = DATALAKE_ROOT_FOLDER + "formatted/source1/"
       if not os.path.exists(TARGET_PATH):
           os.makedirs(TARGET_PATH)

       df = spark.read.csv(RAW_PATH + "dataSource1.csv")
       df.write.mode("overwrite").parquet(TARGET_PATH + "dataSource1")


   def taskFormattedDataSource2(file_name, current_day):
       RATING_PATH = DATALAKE_ROOT_FOLDER + "raw/source2/imdb/MovieRating/" + current_day + "/" + file_name
       FORMATTED_RATING_FOLDER = DATALAKE_ROOT_FOLDER + "formatted/source2/imdb/MovieRatings/" + current_day + "/"
       if not os.path.exists(FORMATTED_RATING_FOLDER):
           os.makedirs(FORMATTED_RATING_FOLDER)
       df = pd.read_csv(RATING_PATH, sep='\t')
       parquet_file_name = file_name.replace(".tsv.gz", ".snappy.parquet")
       df.to_parquet(FORMATTED_RATING_FOLDER + parquet_file_name)


   def task5():
       combine_data(current_day=date.today().strftime("%Y%m%d"))


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

   t4 = PythonOperator(
       task_id='taskFormattedDataSource2',
       python_callable=taskFormattedDataSource2,
       op_args=['title.ratings.tsv.gz', current_day]
   )

   t5 = PythonOperator(
       task_id='task5',
       python_callable=task5
   )


   t1 >> t3
   t2 >> t4
   [t3,t4] >> t5


