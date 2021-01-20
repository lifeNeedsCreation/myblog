import datetime
from utils.bigquery import katkat_bigquery_client
from utils.mysql import katkat_mysql_client


class NewUsersChannelsRetention(object):
    """
    : param start_time: 指标计算的开始时间
    : param end_time：指标计算的结束时间
    : param channel: 指视频不同的频道
    : param table_name：计算结果存的表
    """
    # 构造函数，初始化数据
    def __init__(self, start_time, end_time, channel, table_name, logger=None):
        self.start_time = start_time
        self.end_time = end_time
        self.channel = channel
        self.table_name = table_name
        self.logger = logger

    # 查询 BigQuery，并解析组装数据
    def get_data(self, sql):
        result = katkat_bigquery_client.query(sql).to_dataframe()
        fields = [
            'country_code',
            'initial_date',
            'retention_date',
            'initial_channel',
            'retention_channel',
            'date_diff',
            'initial_users',
            'retention_users',
            'retention_rate',
        ]
        dict_info = {field: [] for field in fields}
        for index, row in result.iterrows():
            for field in fields:
                dict_info[field].append(row[field])
        return dict_info

    # 组装查询 sql，并将统计计算结果存入 mysql
    def compute_data(self):
        start_time = self.start_time.strftime("%Y-%m-%d %H:%M:%S")
        end_time = self.end_time.strftime("%Y-%m-%d %H:%M:%S")
        fields = [
            'country_code',
            'initial_date',
            'retention_date',
            'initial_channel',
            'retention_channel',
            'date_diff',
            'initial_users',
            'retention_users',
            'retention_rate',
        ]
        query = \
            f'''
            with
            accounts as (select * from input.accounts where name is not null),

            account_profiles as (select * from partiko.account_profiles where mac_address is not null),

            account as (select id, country_code, created_date from (select distinct id,country_code, extract(date from created_at) as created_date from accounts) inner join (select distinct account_id from (select mac_address, min(created_at) as created_at from account_profiles group by mac_address) as a inner join (select account_id, mac_address, created_at from account_profiles) as b on a.mac_address = b.mac_address and a.created_at = b.created_at) on id = account_id),

            watch_channels as (select * from stream_events.video_impression where created_at > timestamp_sub(timestamp"{start_time}", interval 30 day) and created_at < "{end_time}" and json_extract_scalar(data, "$.placement") in ({self.channel})),

            channel_target_time as (select distinct account_id, extract(date from created_at) as date, json_extract_scalar(data, "$.placement") as channel from watch_channels where created_at >= "{start_time}" and created_at < "{end_time}"),

            channel_one_month as (select distinct account_id, extract(date from created_at) as date, json_extract_scalar(data, "$.placement") as channel from watch_channels),

            initial_channels as (select distinct id, country_code, date as initial_date, channel as initial_channel from account inner join channel_one_month on id = account_id where created_date = date),

            retention_channels as (select distinct id, country_code, initial_date, date as retention_date, initial_channels.initial_channel as initial_channel, channel_target_time.channel as retention_channel, date_diff(date, initial_date, day) as date_diff from initial_channels inner join channel_target_time on id = account_id),

            initial_channels_count as (select count(distinct id) as initial_users, country_code, initial_date, initial_channel from initial_channels group by country_code, initial_date),

            retention_channels_count as (select count(distinct id) as retention_users, country_code, initial_date, retention_date, initial_channel, retention_channel, date_diff from retention_channels group by country_code, initial_date, retention_date, initial_channel, retention_channel, date_diff)

            select i.country_code, i.initial_date, retention_date, i.initial_channel, retention_channel, date_diff, initial_users, ifnull(retention_users, 0) as retention_users, round(ifnull(retention_users, 0) / initial_users, 4) as retention_rate from initial_channels_count as i left join retention_channels_count as r on i.country_code = r.country_code and i.initial_date = r.initial_date and i.initial_channel = r.initial_channel where date_diff is not null
            '''
        retention_data = self.get_data(query)
        # 结果数据存入数据库
        cursor = katkat_mysql_client.cursor()
        values = "country_code, initial_date, retention_date, initial_channel, retention_channel, date_diff, initial_users, retention_users, retention_rate, create_time"
        insert_sql = f"INSERT INTO {self.table_name} ({values}) VALUES "
        now_time_utc = datetime.datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S")
        for i in range(len(retention_data['country_code'])):
            insert_sql += "("
            for filed in fields:
                insert_sql += f"""'{retention_data[filed][i]}', """
            insert_sql += f"""'{now_time_utc}'),"""
        insert_sql = insert_sql[:-1]
        try:
            # 执行 sql 语句
            cursor.execute(insert_sql)
            # 提交到数据库
            katkat_mysql_client.commit()
            self.logger.info("start_time={}, end_time={} insert tabel {} success".format(self.start_time, self.end_time, self.table_name))
        except:
            self.logger.exception("start_time={}, end_time={} insert tabel {} err msg".format(self.start_time, self.end_time, self.table_name))
            # 如果发生错误则回滚
            katkat_mysql_client.rollback()
        if cursor:
            cursor.close()