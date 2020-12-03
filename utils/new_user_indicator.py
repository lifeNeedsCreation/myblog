
from utils.bigquery import bigquery_client
from utils.mysql import mysql_client
import datetime


class NewUserIndicator(object):

    # 构造函数， 初始化数据
    """
        start_time:指标计算的开始时间
        end_time：指标计算的结束时间
        limit_time: 新用户行为的时间上限
        country_code：需要计算的国家
        behavior_table: 用户行为表
        indicator_table: 指标结果数据存储表
    """
    def __init__(self, start_time, end_time, limit_time, country_code, behavior_table, indicator_table):
        self.start_time = start_time
        self.end_time = end_time
        self.limit_time = limit_time
        self.country_code = country_code
        self.behavior_table = behavior_table
        self.indicator_table = indicator_table

    # 查询bigquery，并解析组装数据
    def get_data(self, sql):
        res_num = {}
        bq_job = bigquery_client.query(sql).to_dataframe()
        for index, row in bq_job.iterrows():
            placement = row["placement"]
            key = row["key"]
            country_code = row["country_code"]
            value = row["value"]
            num = row["num"]
            if placement and key and country_code and value:
                res_num[placement + "&&" + key + "&&" + country_code + "&&" + value] = num
        return res_num

    def get_big_query_sql_user_profile(self, table, field):
        query_str = ""
        if field == "login":
            pass
        else:
            query_str = "select distinct accounts.id, accounts.created_at, accounts.country_code, profiles." + field + " from " \
                        "(select id, created_at, country_code from buzzbreak-model-240306.input.accounts where name is not null and created_at>='" + self.start_time.strftime("%Y-%m-%d %H:%M:%S") +"' and created_at<'" + self.end_time.strftime("%Y-%m-%d %H:%M:%S") + "' and country_code in (" + self.country_code + ")) as accounts"\
                        " LEFT JOIN " + table + " as profiles on accounts.id=profiles.account_id where account_id is not null"
        return query_str

    def get_big_query_sql_user_behavior(self, sql, field):
        if self.behavior_table == "buzzbreak-model-240306.partiko.point_transactions":
            sql = "select distinct id,country_code," + field + " from (" + sql + ") as accounts_pro LEFT JOIN (select account_id, created_at, json_extract_scalar(data, '$.purpose') as purpose from " + self.behavior_table + " where created_at>='" + self.start_time.strftime("%Y-%m-%d %H:%M:%S") + "' and created_at<'" + self.limit_time.strftime("%Y-%m-%d %H:%M:%S") + "') as behavior on accounts_pro.id=behavior.account_id"
        elif self.behavior_table == "buzzbreak-model-240306.partiko.referrals":
            sql = "select distinct id,country_code," + field + " from (" + sql + ") as accounts_pro LEFT JOIN (select referrer_account_id, created_at from " + self.behavior_table + " where created_at>='" + self.start_time.strftime("%Y-%m-%d %H:%M:%S") + "' and created_at<'" + self.limit_time.strftime("%Y-%m-%d %H:%M:%S") + "') as behavior on accounts_pro.id=behavior.referrer_account_id"
        else:
            sql = "select distinct id,country_code," + field + " from (" + sql + ") as accounts_pro LEFT JOIN (select account_id, created_at from " + self.behavior_table + " where created_at>='" + self.start_time.strftime("%Y-%m-%d %H:%M:%S") + "' and created_at<'" + self.limit_time.strftime("%Y-%m-%d %H:%M:%S") + "') as behavior on accounts_pro.id=behavior.account_id"
        return sql

    # 组装查询sql，并将统计计算结果存入mysql
    def compute_data(self):
        # 维度:media source
        big_query_sql = self.get_big_query_sql_user_profile("buzzbreak-model-240306.partiko.account_profiles", "media_source")
        big_query_sql = self.get_big_query_sql_user_behavior(big_query_sql, "media_source")
        if self.behavior_table == "buzzbreak-model-240306.partiko.point_transactions":
            big_query_sql_yes = "select country_code,media_source,count(*) as num from (" + big_query_sql + " where behavior.purpose in ('immersive_video','read_news')) as result group by country_code, media_source"
            big_query_sql_no = "select country_code,media_source,count(*) as num from (" + big_query_sql + " where behavior.purpose not in ('immersive_video','read_news')) as result group by country_code, media_source"
        elif self.behavior_table == "buzzbreak-model-240306.partiko.referrals":
            big_query_sql_yes = "select country_code,media_source,count(*) as num from (" + big_query_sql + " where behavior.referrer_account_id is not null) as result group by country_code, media_source"
            big_query_sql_no = "select country_code,media_source,count(*) as num from (" + big_query_sql + " where behavior.referrer_account_id is null) as result group by country_code, media_source"
        else:
            big_query_sql_yes = "select country_code,media_source,count(*) as num from (" + big_query_sql + " where behavior.account_id is not null) as result group by country_code, media_source"
            big_query_sql_no = "select country_code,media_source,count(*) as num from (" + big_query_sql + " where behavior.account_id is null) as result group by country_code, media_source"
        data_yes = self.get_data(big_query_sql_yes)
        data_no = self.get_data(big_query_sql_no)
        # 维度:gender_input
        big_query_sql = self.get_big_query_sql_user_profile("buzzbreak-model-240306.partiko.account_profiles", "gender_input")
        # 维度:login渠道
        # big_query_sql = self.get_big_query_sql_user_profile("buzzbreak-model-240306.input.accounts", "login")
        # 维度:有无phone number
        # big_query_sql = self.get_big_query_sql_user_profile("buzzbreak-model-240306.input.accounts", "phone_No")
        # 结果数据存入数据库
        cursor = mysql_client.cursor()
        inser_sql = "INSERT INTO " + self.table_name + " (treatment_name, placement, country_code, dimension, ctr, ctr_union, start_time, end_time, create_time) VALUES"
        now_time_utc = datetime.datetime.utcnow()
        flag = False
        for key in impression_data_union.keys():
            click_num = click_data.get(key, 0)
            impression_num = impression_data.get(key, 0)
            if impression_num <= 0:
                continue
            impression_num_union = impression_data_union.get(key)
            if impression_num_union <= 0:
                continue
            temp_data = key.split("&&")
            if len(temp_data) < 4:
                continue
            inser_sql = inser_sql + " ('" + temp_data[1] + "','" + temp_data[0] + "','" + temp_data[2] + "','" + temp_data[3] + "'," + str(round(click_num/impression_num, 5)) + "," + str(round(click_num/impression_num_union, 5)) + ",'" + self.start_time.strftime("%Y-%m-%d %H:%M:%S") + "','" + self.end_time.strftime("%Y-%m-%d %H:%M:%S") + "','" + now_time_utc.strftime("%Y-%m-%d %H:%M:%S") + "'),"
            flag = True

        if flag:
            inser_sql = inser_sql[:len(inser_sql)-1]
            try:
                # 执行sql语句
                cursor.execute(inser_sql)
                # 提交到数据库执行
                mysql_client.commit()
            except:
                # 如果发生错误则回滚
                mysql_client.rollback()
        if cursor:
            cursor.close()





