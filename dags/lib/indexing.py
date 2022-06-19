import elasticsearch
import pandas as pd
import pyarrow.parquet as pq
import parquet
import os
from elasticsearch import Elasticsearch, helpers

client = Elasticsearch(hosts=["http://localhost:9200"])
docs = []

# TODO: Here you should read your file line by line, for each line, parse them,
#  create a python dictionary and add this dictionary to the array docs.
#  If you think it’s useful, you could add new column that you generate from others columns.
HOME = os.path.expanduser('~')
DATALAKE_ROOT_FOLDER = HOME + "/airflow/"
"""
#PATH = DATALAKE_ROOT_FOLDER + "usage/movieAnalysis/MovieStatistics/20220619/res.snappy.parquet/"
PATH = DATALAKE_ROOT_FOLDER + "formatted/source2/imdb/MovieRatings/20220619/title.ratings.snappy.parquet"
df = pq.read_table(source=PATH).to_pandas()

def indexation():
    with open(PATH) as f:
        docs = parquet.DictReader(f)
        helpers.bulk(client, docs, index="imbd", doc_type="_doc", pipeline='create_index')
"""

