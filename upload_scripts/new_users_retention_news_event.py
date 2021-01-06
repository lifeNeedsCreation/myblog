import datetime
from utils.bigquery import bigquery_client
from utils.mysql import mysql_client


class NewUsersRetentionNewsEvent:
    # 构造函数， 初始化数据
    """
        start_time:指标计算的开始时间
        end_time：指标计算的结束时间
        country_code：需要计算的国家
        indicator_dimension：需要计算的实验组的维度
        table_name：计算结果存的表
    """

    def __init__(self, start_time, end_time, country_code, indicator_dimension, table_name, logger=None):
        self.start_time = start_time
        self.end_time = end_time
        self.country_code = country_code
        self.indicator_dimension = indicator_dimension
        self.table_name = table_name
        self.logger = logger

    # 查询bigquery，并解析组装数据
    def get_data(self, sql):
        df_result = bigquery_client.query(sql).to_dataframe()
        fields = [
            'country_code',
            'key',
            'value',
            'created_date',
            'date',
            'initial_event',
            'retention_event',
            'date_diff',
            'initial_users',
            'retention_users',
            'retention_rate'
        ]
        dict_info = {field: [] for field in fields}
        for index, row in df_result.iterrows():
            for field in fields:
                if field in ['created_date', 'date']:
                    dict_info[field].append(datetime.datetime.strptime(str(row[field]), '%Y-%m-%d'))
                elif field in ['retention_rate']:
                    dict_info[field].append(float(row[field]))
                elif field in ['date_diff', 'initial_users', 'retention_users']:
                    dict_info[field].append(int(row[field]))
                else:
                    dict_info[field].append(row[field])
        return dict_info

    # 组装查询sql，并将统计计算结果存入mysql
    def compute_data(self):
        start_time = self.start_time.strftime("%Y-%m-%d %H:%M:%S")
        end_time = self.end_time.strftime("%Y-%m-%d %H:%M:%S")
        query = \
            f'''
            with
            app_open as (select * from stream_events.app_open),
            news_click as (select * from stream_events.news_click),
            news_read as (select * from stream_events.news_read),
            accounts as (select * from input.accounts where name is not null and country_code in ({self.country_code})),
            memories as (select * from partiko.memories where key like 'experiment_%' and value in ({self.indicator_dimension})),
            account as (select distinct id,country_code,key,value,extract(date from updated_at) as created_date from (select distinct id,country_code,created_at from accounts) inner join (select distinct account_id,key,value,updated_at from memories) on id=account_id and extract(date from created_at)=extract(date from updated_at)),
            app_open_target_time as (select * from app_open where created_at > '{start_time}' and created_at < '{end_time}'),
            news_click_target_time as (select * from news_click where created_at > '{start_time}' and created_at < '{end_time}'),
            news_read_target_time as (select * from news_read where created_at > '{start_time}' and created_at < '{end_time}'),
            open_event_target_time as (select distinct account_id,extract(date from created_at) as date from app_open_target_time),
            news_event_target_time as (select distinct account_id,date from ((select distinct account_id,extract(date from created_at) as date from news_click_target_time) union all (select distinct account_id,extract(date from created_at) as date from news_read_target_time))),
            news_click_one_month as (select * from news_click where created_at > timestamp_sub(timestamp'{start_time}', interval 30 day) and created_at < '{end_time}'),
            news_read_one_month as (select * from news_read where created_at > timestamp_sub(timestamp'{start_time}', interval 30 day) and created_at < '{end_time}'),
            news_event_one_month as (select distinct account_id,date from ((select distinct account_id,extract(date from created_at) as date from news_click_one_month) union all (select distinct account_id,extract(date from created_at) as date from news_read_one_month))),
            initial_with_news_event as (select account.* from account inner join news_event_one_month on id=account_id and created_date=date),
            retention_with_open_event as (select distinct open_event_target_time.* from account inner join open_event_target_time on id=account_id),
            retention_with_news_event as (select distinct news_event_target_time.* from account inner join news_event_target_time on id=account_id),
            initial_with_news_event_count as (select count(distinct id) as initial_users,country_code,key,value,created_date from initial_with_news_event group by country_code,key,value,created_date),
            retention_with_open_event_count as (select count(distinct id) as retention_users,country_code,key,value,created_date,date,date_diff from (select initial_with_news_event.*,date,date_diff(date,created_date,day) as date_diff from retention_with_open_event inner join initial_with_news_event on account_id=id) group by country_code,key,value,created_date,date,date_diff),
            retention_with_news_event_count as (select count(distinct id) as retention_users,country_code,key,value,created_date,date,date_diff from (select initial_with_news_event.*,date,date_diff(date,created_date,day) as date_diff from retention_with_news_event inner join initial_with_news_event on account_id=id) group by country_code,key,value,created_date,date,date_diff)
            select
            distinct country_code,key,value,created_date,initial_event,retention_event,date,date_diff,initial_users,retention_users,retention_rate
            from (
            (select i.country_code,i.key,i.value,i.created_date,'news_event' as initial_event,'open_event' as retention_event,date,date_diff,initial_users,ifnull(retention_users,0) as retention_users,round(ifnull(retention_users,0)/initial_users, 4) as retention_rate from initial_with_news_event_count as i inner join retention_with_open_event_count as r on i.country_code=r.country_code and i.key=r.key and i.value=r.value and i.created_date=r.created_date)
            union all
            (select i.country_code,i.key,i.value,i.created_date,'news_event' as initial_event,'news_event' as retention_event,date,date_diff,initial_users,ifnull(retention_users,0) as retention_users,round(ifnull(retention_users,0)/initial_users, 4) as retention_rate from initial_with_news_event_count as i inner join retention_with_news_event_count as r on i.country_code=r.country_code and i.key=r.key and i.value=r.value and i.created_date=r.created_date)
            )
            '''
        retention_data = self.get_data(query)
        # 结果数据存入数据库
        cursor = mysql_client.cursor()
        values = "treatment_name, date_diff, country_code, dimension, retention_rate, initial_users, retention_users," \
                 "initial_date, retention_date, initial_event, retention_event, create_time"
        insert_sql = f"INSERT INTO {self.table_name} ({values}) VALUES"
        now_time_utc = datetime.datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S")
        for i in range(len(retention_data['key'])):
            insert_sql += f"""('{retention_data["key"][i]}', '{retention_data["date_diff"][i]}', '{retention_data["country_code"][i]}', '{retention_data["value"][i]}', '{retention_data["retention_rate"][i]}', '{retention_data["initial_users"][i]}', '{retention_data["retention_users"][i]}', '{retention_data["created_date"][i]}', '{retention_data["date"][i]}', '{retention_data["initial_event"][i]}', '{retention_data["retention_event"][i]}', '{now_time_utc}'),"""
        insert_sql = insert_sql[:-1]
        try:
            # 执行sql语句
            cursor.execute(insert_sql)
            # 提交到数据库执行
            mysql_client.commit()
            self.logger.info("start_time={}, end_time={} insert tabel {} success".format(self.start_time, self.end_time, self.table_name))
        except:
            self.logger.exception("start_time={}, end_time={} insert tabel {} err msg".format(self.start_time, self.end_time, self.table_name))
            # 如果发生错误则回滚
            mysql_client.rollback()
        if cursor:
            cursor.close()
