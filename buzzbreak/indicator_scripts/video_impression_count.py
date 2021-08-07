import datetime
from utils.bigquery import buzzbreak_bigquery_client
from utils.mysql import buzzbreak_mysql_client
from utils.utils import read_sql


class VideoImpressionCount(object):
    """
    : param table_name：计算结果存的表
    """
    # 构造函数，初始化数据
    def __init__(self, start_time, table_name, logger=None):
        self.start_time = start_time
        self.table_name = table_name
        self.logger = logger
        self.fields = ["country_code", "date", "impression_num"]

    # 查询 BigQuery，并解析组装数据
    def get_data(self, sql):
        result = buzzbreak_bigquery_client.query(sql).to_dataframe()
        self.logger.info("result: {}\n".format(result))
        dict_info = {field: [] for field in self.fields}
        for index, row in result.iterrows():
            for field in self.fields:
                dict_info[field].append(row[field])
        return dict_info

    # 组装查询 sql，并将统计计算结果存入 mysql
    def compute_data(self, path):
        start_time = self.start_time.strftime("%Y-%m-%d")
        sql = read_sql(path)
        params = {"start_time": start_time}
        query = sql.format(**params)
        video_impression_count_data = self.get_data(query)
        if video_impression_count_data[self.fields[0]]:
            # 结果数据存入数据库
            values = ""
            for field in self.fields:
                values += field + ", "
            values += "create_time"
            insert_sql = f"INSERT INTO {self.table_name} ({values}) VALUES "
            now_time_utc = datetime.datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S")
            for i in range(len(video_impression_count_data[self.fields[0]])):
                insert_sql += "("
                for field in self.fields:
                    insert_sql += f"'{video_impression_count_data[field][i]}', "
                insert_sql += f"'{now_time_utc}'),"
            insert_sql = insert_sql[:-1]
            buzzbreak_mysql_client.execute_sql(insert_sql)