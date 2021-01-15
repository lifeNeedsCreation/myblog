from google.cloud import bigquery
import os
BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))


class BigQuery(object):
    def __init__(self, project, bucket):
        self.project = project
        self.bucket = bucket
        self.config(self.bucket)

    def config(self, bucket):
        if bucket == "buzzbreak":
            AUTH_JSON_FILE_PATH = BASE_DIR + "/config/buzzbreak_bigquery_config.json"
        elif bucket == "katkat":
            AUTH_JSON_FILE_PATH = BASE_DIR + "/config/katkat_bigquery_config.json"
        self.get_client(AUTH_JSON_FILE_PATH)


    def get_client(self, auth_json_file_path):
        return bigquery.Client.from_service_account_json(auth_json_file_path)


buzzbreak_bigquery_client = BigQuery("buzzbreak-model-240306", "buzzbreak")
katkat_bigquery_client = BigQuery("katkat-model-240306", "katkat")




