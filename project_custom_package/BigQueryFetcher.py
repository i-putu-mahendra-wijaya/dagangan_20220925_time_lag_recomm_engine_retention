from google.cloud import bigquery
from dotenv import load_dotenv
import os


class BigQueryFetcher:

    def __init__(self, query_string):
        load_dotenv()
        self.result_df = None
        os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = os.getenv("GOOGLE_APPLICATION_CREDENTIALS")
        self.bqclient = bigquery.Client()
        self.query_string = query_string

    def fetch_data(self):
        self.result_df = (
            self.bqclient.query(self.query_string)
            .result()
            .to_dataframe()
        )

        return self.result_df
