from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.python import PythonOperator
import os.path
from pyspark.sql import SparkSession
from shutil import move, rmtree



with DAG(
       'projet_BigData',
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


   def task1_extract_data_from_oracle():
       spark = SparkSession \
           .builder \
           .appName("CCF with Dataframe and PySpark") \
           .config("spark.sql.analyzer.failAmbiguousSelfJoin", "false") \
           .config("spark.driver.extraClassPath", "file:/C:/spark/spark-3.1.3-bin-hadoop3.2/ojdbc11.jar") \
           .getOrCreate()

       spark.sparkContext._jsc.hadoopConfiguration().set("mapreduce.fileoutputcommitter.marksuccessfuljobs", "false")
       spark.sparkContext._jsc.hadoopConfiguration().set("parquet.enable.summary-metadata", "false")

       table_list = ('SUIVIE_DES_VENTES')

       user = "system"
       password = "oracle"
       exportFilePath = "\\wsl.localhost\\Ubuntu-18.04\\home\\saba\\export"
       urlConnection = f"jdbc:oracle:thin:@localhost:49161:xe"
       schema = "projet_BigData"

       for tbl in table_list:
           print(f"{schema}.{tbl}")
           jdbcDF = spark.read.format("jdbc") \
               .option("url", urlConnection) \
               .option("dbtable", f"{schema}.{tbl}") \
               .option("user", user) \
               .option("password", password) \
               .option("driver", "oracle.jdbc.driver.OracleDriver") \
               .load()

           writeParquet(jdbcDF, exportFilePath, tbl)

   def task2():
       print("Hello Airflow - This is Task 2")

   t1 = PythonOperator(
       task_id='task1_extract_data_from_oracle',
       python_callable=task1_extract_data_from_oracle,
   )

   t2 = PythonOperator(
       task_id='task2',
       python_callable=task2
   )

   t1 >> t2


def writeParquet(df, targetFilePath, tableName):
    targetFileName = tableName.replace("[", "").replace("]", "")
    filepath = os.path.join(targetFilePath, targetFileName)
    df.coalesce(1).write.parquet(filepath)
    df.localCheckpoint()
    files = os.listdir(filepath)

    for file in files:
        if file.endswith(".csv"):
            os.rename(os.path.join(filepath, file), os.path.join(filepath, targetFileName + ".csv"))
            move(os.path.join(filepath, targetFileName + ".csv"), targetFilePath)
    rmtree(filepath)

    return