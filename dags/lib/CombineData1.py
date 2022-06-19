from datetime import date
import os
from pyspark import SparkContext
from pyspark.sql.session import SparkSession

HOME = os.path.expanduser('~')
DATALAKE_ROOT_FOLDER = HOME + "/airflow/"

current_day = date.today().strftime("%Y%m%d")

sc = SparkContext.getOrCreate()
spark = SparkSession(sc)

def combine_data1(current_day):
    # combine data for data source 2
   RATING_PATH = DATALAKE_ROOT_FOLDER + "formatted/source1/" + current_day + "/dataSource1/"
   USAGE_OUTPUT_FOLDER_STATS = DATALAKE_ROOT_FOLDER + "usage/cinemaAnalysis/" + current_day + "/"
   if not os.path.exists(USAGE_OUTPUT_FOLDER_STATS):
       os.makedirs(USAGE_OUTPUT_FOLDER_STATS)
   df_ratings = spark.read.parquet(RATING_PATH)
   df_ratings.registerTempTable("ratings")

   # Check content of the DataFrame df_ratings:
   print(df_ratings.show())

   stats_df = spark.sql("SELECT COUNT(semaines_d_activite_2020) AS max_rating FROM ratings")

   # Check content of the DataFrame stats_df and save it:
   print(stats_df.show())
   stats_df.write.save(USAGE_OUTPUT_FOLDER_STATS + "res.snappy.parquet", mode="overwrite")
