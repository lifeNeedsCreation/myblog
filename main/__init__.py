# import os
# from google.cloud import bigquery
#
# DIR = os.path.dirname(os.path.abspath(__file__))
#
#
# class BigQuery:
#
#     def __init__(self, project="buzzbreak-model-240306", bucket="buzzbreak", region="us-east1", logger=None, **kwargs):
#         self.project = project
#         self.bucket = bucket
#         self.region = region
#         self.config(kwargs.get('user', None))
#         self.logger = logger
#
#     def config(self, user):
#         if user:
#             credential_path = DIR + '/../config/googlecloud.json'
#             os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = credential_path
#             self.client = bigquery.Client()
#         else:
#             self.client = bigquery.Client(project=self.project)
#
#     def run_query(self, query=""):
#         bq = bigquery.Client(project=self.project)
#         df = bq.query(query).to_dataframe()
#         return df
#
#     def run_sql(self, query):
#         return self.client.query(query)
#
#     def show_tables(self, dataset_id):
#         return self.client.list_tables(dataset_id)
#
#     def delete_table(self, dataset_id, table_id):
#         dataset_ref = self.client.dataset(dataset_id)
#         table_ref = dataset_ref.table(table_id)
#         self.client.delete_table(table_ref)
#         return True
#
#     def create_table(self, dataset_id, table_id, schema, time_partitioning_field=None):
#         dataset_ref = self.client.dataset(dataset_id)
#         table_ref = dataset_ref.table(table_id)
#         table = bigquery.Table(table_ref, schema=schema)
#         if time_partitioning_field:
#             table.time_partitioning = bigquery.TimePartitioning(
#                 type_=bigquery.TimePartitioningType.DAY,
#                 field=time_partitioning_field
#             )
#         table = self.client.create_table(table)
#         return table
#
#     def upload(self, data, dataset_id, table_id, schema, skip_leading_rows=0, is_file=True):
#         dataset_ref = self.client.dataset(dataset_id)
#         table_ref = dataset_ref.table(table_id)
#         job_config = bigquery.LoadJobConfig()
#         job_config.source_format = bigquery.SourceFormat.CSV
#         job_config.write_disposition = bigquery.WriteDisposition.WRITE_APPEND
#         job_config.skip_leading_rows = skip_leading_rows
#         job_config.schema = schema
#         job_config.autodetect = True
#         if is_file:
#             with open(data, "rb") as source_file:
#                 job = self.client.load_table_from_file(
#                     source_file,
#                     table_ref,
#                     job_config=job_config
#                 )
#         else:
#             job = self.client.load_table_from_dataframe(
#                 data,
#                 table_ref,
#                 job_config=job_config
#             )
#         return job
#
#     def upload_csv_to_bq(
#             self,
#             file_path,
#             dataset_id,
#             table_id,
#             documents: list,
#             datas_type: dict,
#             skip_leading_rows=0,
#             location='US'
#     ):
#         dataset_ref = self.client.dataset(dataset_id)
#         table_ref = dataset_ref.table(table_id)
#         job_config = bigquery.LoadJobConfig()
#         schema_list = []
#         for docu in documents:
#             docu_style = datas_type[docu]
#             docu_set = bigquery.SchemaField(docu, docu_style, mode="NULLABLE")
#             schema_list.append(docu_set)
#         job_config.schema = schema_list
#         job_config.source_format = bigquery.SourceFormat.CSV
#         job_config.write_disposition = bigquery.WriteDisposition.WRITE_APPEND
#         job_config.skip_leading_rows = skip_leading_rows
#         job_config.autodetect = True
#         with open(file_path, "rb") as source_file:
#             job = self.client.load_table_from_file(
#                 source_file,
#                 table_ref,
#                 location=location,
#                 job_config=job_config,
#             )
#         return job
#
#     @staticmethod
#     def schema_field(name, field_type, mode="NULLABLE", description=None):
#         return bigquery.SchemaField(name, field_type, mode, description)
#
#     def operation_record(self, job, table):
#         self.logger.info('bq --format=prettyjson show -j {}'.format(job.job_id))
#         job.result()
#         self.logger.info(f'{job.statement_type} {job.num_dml_affected_rows} rows in {table}.')
#
#
# def main():
#     bq_client = BigQuery(user='ccp')
#
#
# if __name__ == '__main__':
#     main()
