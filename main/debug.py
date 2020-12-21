from utils.bigquery import bigquery_client
import google.auth
from google.cloud import bigquery
from google.cloud import bigquery_storage


def download_all_user_genders():
    credentials, your_project_id = google.auth.default(scopes=["https://www.googleapis.com/auth/cloud-platform"])
    bqclient = bigquery.Client(credentials=credentials, project=your_project_id, )
    bqstorageclient = bigquery_storage.BigQueryReadClient(credentials=credentials)

    query = """
    SELECT user, gender FROM recommendation.user_gender
    """
    df = bqclient.query(query).result().to_dataframe(bqstorage_client=bqstorageclient)
    print(type(df))
    df['gender'] = df['gender'].map({None: 0, 'male': 1, 'female': 2})
    return df

def get_all_user_genders():
    query = """
        SELECT
          user,
          gender
        FROM
          recommendation.user_gender
        """
    df = bigquery_client.query(query).to_dataframe()
    print(type(df))
    df['gender'] = df['gender'].map({None: 0, 'male': 1, 'female': 2})
    return df


if __name__ == "__main__":
    # download_all_user_genders()
    get_all_user_genders()
    # bq_job = bigquery_client.query("select distinct media_source from buzzbreak-model-240306.partiko.account_profiles").to_dataframe()
    # # for index, row in bq_job.iterrows():
    # #     res = row["media_source"]
    # #     if not res:
    # #         print("######################")
    # rows_to_insert = [
    #     {"object_id": "", "account_id": "123", "key": "test", "updated_at": "2020-11-12 00:00:00",
    #      "value": "a"}
    # ]
    # errors = bigquery_client.insert_rows_json("partiko.memories", rows_to_insert,
    #                                           row_ids=[None] * len(rows_to_insert))
    # if errors == []:
    #     print("sss")
    # else:
    #     print("Encountered errors while inserting rows: {}".format(errors))
    # if bigquery_client:
    #     bigquery_client.close()