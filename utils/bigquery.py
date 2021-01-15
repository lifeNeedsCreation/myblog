from google.cloud import bigquery
import os
BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))


class BigQuery(object):
    def __init__(self, project, bucket):
        self.project = project
        self.bucket = bucket
        self.auth = self.config()
        

    def config(self):
        if self.bucket == "buzzbreak":
            AUTH_JSON_FILE_PATH = BASE_DIR + "/config/buzzbreak_bigquery_config.json"
        elif self.bucket == "katkat":
            AUTH_JSON_FILE_PATH = BASE_DIR + "/config/katkat_bigquery_config.json"
        return AUTH_JSON_FILE_PATH

    def get_client(self):
        return bigquery.Client.from_service_account_json(self.auth)


buzzbreak_bigquery_client = BigQuery("buzzbreak-model-240306", "buzzbreak").get_client()
katkat_bigquery_client = BigQuery("katkat-model-240306", "katkat").get_client()




