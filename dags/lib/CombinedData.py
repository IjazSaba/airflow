from datetime import date
import os
from pyspark import SparkContext
from pyspark.sql.session import SparkSession

HOME = os.path.expanduser('~')
DATALAKE_ROOT_FOLDER = HOME + "/airflow/"

current_day = date.today().strftime("%Y%m%d")

sc = SparkContext.getOrCreate()
spark = SparkSession(sc)

def combine_data(current_day):
    #combine data for data source 1
   RATING_PATH_cinema = DATALAKE_ROOT_FOLDER + "formatted/source1/dataSource1/"
   USAGE_OUTPUT_FOLDER_STATS_cinema = DATALAKE_ROOT_FOLDER + "usage/cinemaAnalysis/cinemaStatistics/"
   USAGE_OUTPUT_FOLDER_BEST_cinema = DATALAKE_ROOT_FOLDER + "usage/cinemaAnalysis/WeekActivity/"
   if not os.path.exists(USAGE_OUTPUT_FOLDER_STATS_cinema):
       os.makedirs(USAGE_OUTPUT_FOLDER_STATS_cinema)
   if not os.path.exists(USAGE_OUTPUT_FOLDER_BEST_cinema):
       os.makedirs(USAGE_OUTPUT_FOLDER_BEST_cinema)

   df_ratings_cinema = spark.read.parquet(RATING_PATH_cinema)
   df_ratings_cinema.registerTempTable("ratings_cinema")

   stats_df_cinema = spark.sql("SELECT AVG(semaines_d_activite_2020) AS avg_weekActivity,"
                               "       MAX(semaines_d_activite_2020) AS max_weekActivity,"
                               "       MIN(desemaines_d_activite_2020p) AS min_weekActivity,"
                               "       COUNT(semaines_d_activite_2020) AS count_weekActivity"
                               "    FROM ratings_cinema")

   weekAcitivity_df = spark.sql("SELECT tconst, dep"
                                "    FROM ratings_cinema"
                                "    GROUP BY dep")


   stats_df_cinema.write.save(USAGE_OUTPUT_FOLDER_STATS_cinema + "res.snappy.parquet", mode="overwrite")
   weekAcitivity_df.write.save(USAGE_OUTPUT_FOLDER_BEST_cinema + "res.snappy.parquet", mode="overwrite")



    # combine data for data source 2
   RATING_PATH_Movie = DATALAKE_ROOT_FOLDER + "formatted/source2/imdb/MovieRatings/" + current_day + "/"
   USAGE_OUTPUT_FOLDER_STATS_Movies = DATALAKE_ROOT_FOLDER + "usage/movieAnalysis/MovieStatistics/" + current_day + "/"
   USAGE_OUTPUT_FOLDER_BEST_Movies = DATALAKE_ROOT_FOLDER + "usage/movieAnalysis/MovieTop10/" + current_day + "/"
   if not os.path.exists(USAGE_OUTPUT_FOLDER_STATS_Movies):
       os.makedirs(USAGE_OUTPUT_FOLDER_STATS_Movies)
   if not os.path.exists(USAGE_OUTPUT_FOLDER_BEST_Movies):
       os.makedirs(USAGE_OUTPUT_FOLDER_BEST_Movies)

   df_ratings = spark.read.parquet(RATING_PATH_Movie)
   df_ratings.registerTempTable("ratings_movies")

   # Check content of the DataFrame df_ratings:
   print(df_ratings.show())

   stats_df = spark.sql("SELECT AVG(averageRating) AS avg_rating,"
                             "       MAX(averageRating) AS max_rating,"
                             "       MIN(averageRating) AS min_rating,"
                             "       COUNT(averageRating) AS count_rating"
                             "    FROM ratings LIMIT 10")
   top10_df = spark.sql("SELECT tconst, averageRating"
                             "    FROM ratings"
                             "    WHERE numVotes > 50000 "
                             "    ORDER BY averageRating DESC"
                             "    LIMIT 10")

   # Check content of the DataFrame stats_df and save it:
   print(stats_df.show())
   stats_df.write.save(USAGE_OUTPUT_FOLDER_STATS_Movies + "res.snappy.parquet", mode="overwrite")

   # Check content of the DataFrame top10_df  and save it:
   print(top10_df.show())
   top10_df.write.save(USAGE_OUTPUT_FOLDER_BEST_Movies + "res.snappy.parquet", mode="overwrite")