from google.cloud import bigquery
import os
BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))


class BigQuery(object):
    # max is 100 per project
    BQ_THREAD_DRY_RUN_LIMIT = 20
    BQ_QUERY_SLEEP_SECONDS = 1
    AUTH_JSON_FILE_PATH = BASE_DIR + '/bigquery_config.json'

    def __init__(self):
        pass

    def get_client(self):
        return bigquery.Client.from_service_account_json(self.AUTH_JSON_FILE_PATH)


bigquery_client = BigQuery().get_client()
