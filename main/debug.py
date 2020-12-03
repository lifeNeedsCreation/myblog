from utils.bigquery import bigquery_client
if __name__ == "__main__":
    bq_job = bigquery_client.query("select distinct media_source from buzzbreak-model-240306.partiko.account_profiles").to_dataframe()
    for index, row in bq_job.iterrows():
        res = row["media_source"]
        if not res:
            print("######################")
    if bigquery_client:
        bigquery_client.close()