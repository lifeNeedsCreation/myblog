import datetime
from utils.bigquery import buzzbreak_bigquery_client
from utils.mysql import buzzbreak_mysql_client


class PartikoMemoriesUserTimeAverageOfDuration:
    # 构造函数， 初始化数据
    """
        start_time:指标计算的开始时间
        end_time：指标计算的结束时间
        indicator_dimension：需要计算的实验组的维度
        table_name：计算结果存的表
    """

    def __init__(self, start_time, end_time, indicator_dimension, table_name, logger=None):
        self.start_time = start_time
        self.end_time = end_time
        self.indicator_dimension = indicator_dimension
        self.table_name = table_name
        self.logger = logger

    # 查询bigquery，并解析组装数据
    def get_data(self, sql):
        df_result = buzzbreak_bigquery_client.query(sql).to_dataframe()
        fields = [
            'country_code',
            'key',
            'value',
            'date',
            'page',
            'open_users_count',
            'avg_duration'
        ]
        dict_info = {field: [] for field in fields}
        for index, row in df_result.iterrows():
            for field in fields:
                if field in ['date']:
                    dict_info[field].append(datetime.datetime.strptime(str(row[field]), '%Y-%m-%d'))
                elif field in ['avg_duration']:
                    dict_info[field].append(float(row[field]))
                elif field in ['open_users_count']:
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
            memories as (select * from partiko.memories where value in ({self.indicator_dimension})),
            accounts as (select * from input.accounts where name is not null),
            user_time as (select * from stream_events.user_time where created_at>'{start_time}' and created_at<'{end_time}'),
            app_open as (select * from stream_events.app_open where created_at>'{start_time}' and created_at<'{end_time}'),
            experiment_accounts as (select distinct id,country_code,key,value,updated_at from accounts inner join memories on id=account_id),
            experiment_user_time as (select id,country_code,key,value,json_extract_scalar(data,'$.page') as page,safe_cast(json_extract_scalar(data,'$.duration_in_seconds') as numeric) as duration_in_seconds,created_at from experiment_accounts inner join user_time on id=account_id where extract(date from created_at)>=extract(date from updated_at)),
            experiment_app_open as (select distinct id,country_code,key,value,extract(date from created_at) as date from experiment_accounts inner join app_open on id=account_id where extract(date from created_at)>=extract(date from updated_at))
            select a.country_code,a.key,a.value,a.date,page,open_users_count,round(ifnull(duration_sum,0)/open_users_count,4) as avg_duration from (select country_code,key,value,date,count(distinct id) as open_users_count from experiment_app_open group by country_code,key,value,date) as a left join (select country_code,key,value,page,extract(date from created_at) as date,sum(duration_in_seconds) as duration_sum from experiment_user_time group by country_code,key,value,page,date) as u on a.country_code=u.country_code and a.key=u.key and a.value=u.value and a.date=u.date
            '''
        user_time_data = self.get_data(query)
        # 结果数据存入数据库
        values = "country_code, treatment_name, dimension, date, page, open_users_count, avg_duration, create_time"
        insert_sql = f"INSERT INTO {self.table_name} ({values}) VALUES"
        now_time_utc = datetime.datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S")
        for i in range(len(user_time_data['country_code'])):
            insert_sql += f"""('{user_time_data["country_code"][i]}', '{user_time_data["key"][i]}', '{user_time_data["value"][i]}', '{user_time_data["date"][i]}', '{user_time_data["page"][i]}', '{user_time_data["open_users_count"][i]}', '{user_time_data["avg_duration"][i]}', '{now_time_utc}'),"""
        insert_sql = insert_sql[:-1]
        buzzbreak_mysql_client.execute_sql(insert_sql)
        
