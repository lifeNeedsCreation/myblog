from google.cloud import bigquery
import os
BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))


class BigQuery(object):
    def __init__(self, bucket):
        self.bucket = bucket
        self.credential_path = self.config()
        

    def config(self):
        if self.bucket == "buzzbreak":
            credential_path = BASE_DIR + "/config/buzzbreak_bigquery_config.json"
        elif self.bucket == "katkat":
            credential_path = BASE_DIR + "/config/katkat_bigquery_config.json"
        return credential_path

    def get_client(self):
        return bigquery.Client.from_service_account_json(self.credential_path)


buzzbreak_bigquery_client = BigQuery("buzzbreak").get_client()
katkat_bigquery_client = BigQuery("katkat").get_client()




