import datetime
from utils.bigquery import buzzbreak_bigquery_client
from utils.mysql import buzzbreak_mysql_client


class PartikoMemoriesAverageOfInvites:
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
            'open_users_count',
            'referees_count',
            'avg_referees_count'
        ]
        dict_info = {field: [] for field in fields}
        for index, row in df_result.iterrows():
            for field in fields:
                if field in ['date']:
                    dict_info[field].append(datetime.datetime.strptime(str(row[field]), '%Y-%m-%d'))
                elif field in ['avg_referees_count']:
                    dict_info[field].append(float(row[field]))
                elif field in ['open_users_count', 'referees_count']:
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
            referrals as (select * from partiko.referrals where created_at>'{start_time}' and created_at<'{end_time}'),
            app_open as (select * from stream_events.app_open where created_at>'{start_time}' and created_at<'{end_time}'),
            experiment_accounts as (select distinct id,country_code,key,value,updated_at as experiment_timestamp from accounts inner join memories on id=account_id),
            experiment_referrals as (select distinct id,referee_account_id,country_code,key,value,experiment_timestamp,created_at from (select distinct referrer_account_id, referee_account_id, created_at from referrals) inner join experiment_accounts on referrer_account_id=id where created_at>experiment_timestamp),
            experiment_app_open as (select distinct id,country_code,key,value,date from (select distinct account_id,extract(date from created_at) as date from app_open) inner join experiment_accounts on account_id=id where date>=extract(date from experiment_timestamp))
            select o.country_code,o.key,o.value,o.date,open_users_count,ifnull(referees_count,0) as referees_count,round(ifnull(referees_count, 0)/open_users_count,4) as avg_referees_count from (select country_code,key,value,extract(date from created_at) as date,count(distinct referee_account_id) as referees_count from experiment_referrals group by country_code,key,value,date) as r right join (select country_code,key,value,date,count(distinct id) as open_users_count from experiment_app_open group by country_code,key,value,date) as o on r.country_code=o.country_code and r.key=o.key and r.value=o.value and r.date=o.date
            '''
        referrals_data = self.get_data(query)
        # 结果数据存入数据库
        cursor = buzzbreak_mysql_client.cursor()
        values = "country_code, treatment_name, dimension, date, open_users_count, referees_count, avg_referees_count, create_time"
        insert_sql = f"INSERT INTO {self.table_name} ({values}) VALUES"
        now_time_utc = datetime.datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S")
        for i in range(len(referrals_data['country_code'])):
            insert_sql += f"""('{referrals_data["country_code"][i]}', '{referrals_data["key"][i]}', '{referrals_data["value"][i]}', '{referrals_data["date"][i]}', '{referrals_data["open_users_count"][i]}', '{referrals_data["referees_count"][i]}', '{referrals_data["avg_referees_count"][i]}', '{now_time_utc}'),"""
        insert_sql = insert_sql[:-1]
        try:
            # 执行sql语句
            cursor.execute(insert_sql)
            # 提交到数据库执行
            buzzbreak_mysql_client.commit()
            self.logger.info("start_time={}, end_time={} insert tabel {} success".format(self.start_time, self.end_time, self.table_name))
        except:
            self.logger.exception("start_time={}, end_time={} insert tabel {} err msg".format(self.start_time, self.end_time, self.table_name))
            # 如果发生错误则回滚
            buzzbreak_mysql_client.rollback()
        if cursor:
            cursor.close()