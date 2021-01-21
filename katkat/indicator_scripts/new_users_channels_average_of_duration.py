import datetime
from utils.bigquery import katkat_bigquery_client
from utils.mysql import katkat_mysql_client


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
        dict_info = {field: [] for field in self.fields}
        for index, row in df_result.iterrows():
            for field in self.fields:
                dict_info[field].append(row[field])
        return dict_info

    # 组装查询sql，并将统计计算结果存入mysql
    def compute_data(self):
        start_time = self.start_time.strftime("%Y-%m-%d %H:%M:%S")
        end_time = self.end_time.strftime("%Y-%m-%d %H:%M:%S")
        query = \
            f'''
            with
            accounts as (select * from input.accounts where name is not null and created_at >= "{start_time}" and created_at < "{end_time}"),

            user_time as (select * from stream_events.user_time where created_at >= "{start_time}" and created_at < "{end_time}" and json_extract_scalar(data, "$.page") in ({self.channel})),

            account_user_time as (select id, country_code, json_extract_scalar(data, "$.page") as channel, safe_cast(json_extract_scalar(data, "$.duration_in_seconds") as numeric) as duration_in_seconds, user_time.created_at from accounts inner join user_time on id = account_id),

            account_count as (select country_code, channel, extract(date from created_at) as date, count(distinct id) as channel_users_count from account_user_time group by country_code, channel, date),

            duration_in_seconds_sum as (select country_code, channel, extract(date from created_at) as date, sum(duration_in_seconds) as channel_duration_sum from account_user_time group by country_code, channel, date)

            select a.country_code, a.channel, a.date, channel_users_count, channel_duration_sum, round(ifnull(channel_duration_sum, 0) / channel_users_count, 4) as channel_duration_avg from account_count as a inner join duration_in_seconds_sum as d on a.country_code = d.country_code and a.channel = d.channel and a.date = d.date    
            '''
        user_time_data = self.get_data(query)
        # 结果数据存入数据库
        cursor = katkat_mysql_client.cursor()
        values = ""
        for field in self.fields:
            values += field + ", "
        values += "create_time"
        print("values", values)
        insert_sql = f"INSERT INTO {self.table_name} ({values}) VALUES"
        print("insert_sql", insert_sql)
        now_time_utc = datetime.datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S")
        insert_sql = ""
        for i in range(len(user_time_data.keys())):
            insert_sql += "("
            for field in self.fields:
                insert_sql += f"'{user_time_data[field][i]}', "
            insert_sql += f"'{now_time_utc}'),"
        insert_sql = insert_sql[:-1]
        print("insert_sql", insert_sql)

        # try:
        #     # 执行sql语句
        #     cursor.execute(insert_sql)
        #     # 提交到数据库执行
        #     katkat_mysql_client.commit()
        #     self.logger.info("start_time={}, end_time={} insert tabel {} success".format(self.start_time, self.end_time, self.table_name))
        # except:
        #     self.logger.exception("start_time={}, end_time={} insert tabel {} err msg".format(self.start_time, self.end_time, self.table_name))
        #     # 如果发生错误则回滚
        #     katkat_mysql_client.rollback()
        # if cursor:
        #     cursor.close()
