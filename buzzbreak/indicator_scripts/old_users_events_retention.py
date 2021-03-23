import datetime
from utils.bigquery import buzzbreak_bigquery_client
from utils.mysql import buzzbreak_mysql_client


class OldUsersEventsRetention(object):
    """
    : param start_time: 指标计算的开始时间
    : param end_time：指标计算的结束时间
    : param table_name：计算结果存的表
    """
    # 构造函数，初始化数据
    def __init__(self, start_time, end_time, table_name, logger=None):
        self.start_time = start_time
        self.end_time = end_time
        self.table_name = table_name
        self.logger = logger

    # 查询 BigQuery，并解析组装数据
    def get_data(self, sql):
        result = buzzbreak_bigquery_client.query(sql).to_dataframe()
        self.logger.info("result: {}\n".format(result))
        fields = [
            'country_code',
            'initial_date',
            'retention_date',
            'initial_event',
            'retention_event',
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
            'initial_event',
            'retention_event',
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

            app_open as (select * from stream_events.app_open where created_at > timestamp_sub(timestamp'{start_time}', interval 30 day) and created_at < '{end_time}'),

            tab_impression as (select * from stream_events.tab_impression where created_at > timestamp_sub(timestamp'{start_time}', interval 30 day) and created_at < '{end_time}'),

            account as (select id, country_code, created_date from (select distinct id,country_code, extract(date from created_at) as created_date from accounts) inner join (select distinct account_id from (select mac_address, min(created_at) as created_at from account_profiles group by mac_address) as a inner join (select account_id, mac_address,created_at from account_profiles) as b on a.mac_address = b.mac_address and a.created_at = b.created_at) on id = account_id),

            events_target_time as (select * from ((select distinct account_id, extract(date from created_at) as date, json_extract_scalar(data,'$.tab') as event from tab_impression where created_at > '{start_time}' and created_at < '{end_time}') union all (select distinct account_id, extract(date from created_at) as date, 'app_open' as event from app_open where created_at > '{start_time}' and created_at < '{end_time}'))),

            events_one_month as (select * from ((select distinct account_id, extract(date from created_at) as date,json_extract_scalar(data,'$.tab') as event from tab_impression) union all (select distinct account_id, extract(date from created_at) as date, 'app_open' as event from app_open))),

            initial_events as (select distinct id, country_code, date as initial_date, event from account inner join events_one_month on id = account_id where created_date < date),

            retention_events as (select distinct id, country_code, initial_date, date as retention_date, initial_events.event as initial_event, events_target_time.event as retention_event, date_diff(date, initial_date, day) as date_diff from initial_events inner join events_target_time on id = account_id),
            
            initial_event_count as (select count(distinct id) as initial_users,country_code, initial_date, event as initial_event from initial_events group by country_code, initial_date, initial_event),

            retention_event_count as (select count(distinct id) as retention_users,country_code, initial_date, retention_date, initial_event, retention_event, date_diff from retention_events group by country_code, initial_date,retention_date, initial_event, retention_event, date_diff)

            select i.country_code, i.initial_date, retention_date, date_diff, i.initial_event, retention_event, initial_users, ifnull(retention_users, 0) as retention_users, round(ifnull(retention_users, 0)/initial_users, 4) as retention_rate from initial_event_count as i left join retention_event_count as r on i.country_code=r.country_code and i.initial_date = r.initial_date and i.initial_event = r.initial_event where date_diff is not null and date_diff > 0 and i.initial_event = retention_event
            '''
        retention_data = self.get_data(query)
        # 结果数据存入数据库
        values = "country_code, initial_date, retention_date, initial_event, retention_event, date_diff, initial_users, retention_users, retention_rate, create_time"
        insert_sql = f"INSERT INTO {self.table_name} ({values}) VALUES "
        now_time_utc = datetime.datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S")
        value_sql = ''
        n = 5000
        self.logger.info("count {}".format(len(retention_data['country_code'])))
        for i in range(1, len(retention_data['country_code'])+1):
            value_sql += "("
            for filed in fields:
                value_sql += f"""'{retention_data[filed][i-1]}', """
            value_sql += f"""'{now_time_utc}'),"""
            if i % n == 0:
                insert_sql1 = insert_sql + value_sql[:-1]
                buzzbreak_mysql_client.execute_sql(insert_sql1)
                value_sql = ''
            elif (i - len(retention_data['country_code'])) == 0:
                insert_sql2 = insert_sql + value_sql[:-1]
                buzzbreak_mysql_client.execute_sql(insert_sql2) 
        