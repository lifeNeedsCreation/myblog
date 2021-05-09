import datetime
from utils.bigquery import buzzbreak_bigquery_client
from utils.mysql import buzzbreak_mysql_client
from utils.utils import read_sql


class VideoCtrWithDeviceModelRank(object):
    """
    : param start_time: 指标计算的开始时间
    : param end_time：指标计算的结束时间
    : param country_code: 指标计算的国家
    : param experiments: 指标计算的实验名称
    : param table_name：计算结果存的表
    """
    # 构造函数，初始化数据
    def __init__(self, start_time, end_time, country_code, experiments, table_name, logger=None):
        self.start_time = start_time
        self.end_time = end_time
        self.country_code = country_code
        self.experiments = experiments
        self.table_name = table_name
        self.logger = logger
        self.fields = ["country_code", "placement", "experiment", "strategy", "device_model", "date", "click_num", "impression_num", "impression_union_num", "ctr", "ctr_union"]

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
        end_time = self.end_time.strftime("%Y-%m-%d")
        sql = read_sql(path)
        params = {"start_time": start_time, "end_time": end_time, "country_code": self.country_code, "experiments": self.experiments}
        query = sql.format(**params)
        video_ctr_with_device_model_rank_data = self.get_data(query)
        if video_ctr_with_device_model_rank_data[self.fields[0]]:
            # 结果数据存入数据库
            values = ""
            n = 5000
            for field in self.fields:
                values += field + ", "
            values += "create_time"
            insert_sql = f"INSERT INTO {self.table_name} ({values}) VALUES "
            now_time_utc = datetime.datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S")
            for i in range(len(video_ctr_with_device_model_rank_data[self.fields[0]])):
                insert_sql += "("
                for field in self.fields:
                    insert_sql += f'"{video_ctr_with_device_model_rank_data[field][i]}", '
                insert_sql += f"'{now_time_utc}'),"
                if i % n == 0:
                    insert_sql1 = insert_sql + value_sql[:-1]
                    buzzbreak_mysql_client.execute_sql(insert_sql1)
                    value_sql = ''
                elif (i - len(video_ctr_with_device_model_rank_data[self.fields[0]])) == 0:
                    insert_sql2 = insert_sql + value_sql[:-1]
                    buzzbreak_mysql_client.execute_sql(insert_sql2)