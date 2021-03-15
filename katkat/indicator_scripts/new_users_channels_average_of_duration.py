import datetime
from utils.bigquery import katkat_bigquery_client
from utils.mysql import katkat_mysql_client
from utils.utils import read_sql


class NewUsersChannelsAverageOfDuration:
    # 构造函数， 初始化数据
    """
        start_time:指标计算的开始时间
        end_time：指标计算的结束时间
        channel：指视频不同的频道
        table_name：计算结果存的表
    """

    def __init__(self, start_time, end_time, channel, table_name, logger=None):
        self.start_time = start_time
        self.end_time = end_time
        self.channel = channel
        self.table_name = table_name
        self.logger = logger
        self.fields = ["country_code", "channel", "date", "channel_users_count", "channel_duration_sum", "channel_duration_avg"]

    # 查询bigquery，并解析组装数据
    def get_data(self, sql):
        df_result = katkat_bigquery_client.query(sql).to_dataframe()
        self.logger.info("result: \n{}".format(df_result))
        dict_info = {field: [] for field in self.fields}
        for index, row in df_result.iterrows():
            for field in self.fields:
                dict_info[field].append(row[field])
        return dict_info

    # 组装查询sql，并将统计计算结果存入mysql
    def compute_data(self, path):
        start_time = self.start_time.strftime("%Y-%m-%d %H:%M:%S")
        end_time = self.end_time.strftime("%Y-%m-%d %H:%M:%S")
        sql = read_sql(path)
        params = {"start_time": start_time, "end_time": end_time, "channel": self.channel}
        query = sql.format(**params)
        channel_duration = self.get_data(query)
        if channel_duration[self.fields[0]]:
            # 结果数据存入数据库
            values = ""
            for field in self.fields:
                values += field + ", "
            values += "create_time"
            insert_sql = f"INSERT INTO {self.table_name} ({values}) VALUES "
            now_time_utc = datetime.datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S")
            for i in range(len(channel_duration[self.fields[0]])):
                insert_sql += "("
                for field in self.fields:
                    insert_sql += f"'{channel_duration[field][i]}', "
                insert_sql += f"'{now_time_utc}'),"
            insert_sql = insert_sql[:-1]
            katkat_mysql_client.execute_sql(insert_sql)
        else:
            self.logger.info("start_time={}, end_time={} insert tabel {} fail due to query result is empty".format(self.start_time, self.end_time, self.table_name))
