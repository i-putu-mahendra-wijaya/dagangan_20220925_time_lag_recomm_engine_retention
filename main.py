import os
import pandas as pd
from dotenv import load_dotenv
import yaml
from yaml.loader import SafeLoader
from project_custom_package.BigQueryFetcher import BigQueryFetcher

# load env variables
load_dotenv()

# load model configuration
with open("./model_config.yaml") as ref:
    model_config = yaml.load(ref, Loader=SafeLoader)
    print(model_config)

query_directory_path = model_config["QUERY_DIRECTORY_PATH"]


def fetch_trx_histories_from_gbq() -> pd.DataFrame:
    trx_histories_sql_path = os.path.join(query_directory_path, "trx_histories.sql")

    with open(trx_histories_sql_path) as query_file:
        trx_histories_sql = query_file.read()

    trx_histories_query_string = f"""{trx_histories_sql}"""
    trx_histories_bqclient = BigQueryFetcher(trx_histories_query_string)
    trx_histories = trx_histories_bqclient.fetch_data()

    return trx_histories


trx_histories = fetch_trx_histories_from_gbq()

trx_histories.head()
