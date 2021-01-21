
from utils.bigquery import buzzbreak_bigquery_client
from utils.mysql import buzzbreak_mysql_client
import datetime


class PushRetentionData(object):

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
        res_num = {}
        bq_job = buzzbreak_bigquery_client.query(sql).to_dataframe()
        for index, row in bq_job.iterrows():
            key = row["key"]
            country_code = row["country_code"]
            value = row["value"]
            num = row["num"]
            if key and country_code and value:
                res_num[key + "&&" + country_code + "&&" + value] = num
        return res_num

    # 组装查询sql，并将统计计算结果存入mysql
    def compute_data(self):
        today_open_app_num = self.get_data("select key, country_code, value,count(*) as num from "
                                   "(select distinct account_id,country_code,key,value from "
                                   "(select open_app_accout_info.account_id, created_at, country_code, key, value, updated_at from "
                                   "(select account_id, open_app.created_at, country_code from "
                                   "(select account_id,created_at from buzzbreak-model-240306.stream_events.app_open where created_at>='" + self.start_time.strftime("%Y-%m-%d %H:%M:%S") + "' and created_at<'" + self.end_time.strftime("%Y-%m-%d %H:%M:%S") + "') as open_app "
                                   " LEFT JOIN buzzbreak-model-240306.input.accounts as acounts ON open_app.account_id = acounts.id where name is not null and country_code in (" + self.country_code + ")) as open_app_accout_info"
                                   " LEFT JOIN (select account_id, key, value, updated_at from buzzbreak-model-240306.partiko.memories where key like 'experiment%'  and value in (" + self.indicator_dimension + ")) as memories ON open_app_accout_info.account_id = memories.account_id where key is not null and value is not null) as result"
                                   " where created_at>=updated_at) as result1 group by key, country_code, value")

        y_start_time = self.start_time - datetime.timedelta(days=1)
        y_end_time = self.start_time

        yesterday_open_app_num = self.get_data("select key, country_code, value,count(*) as num from "
                                   "(select distinct account_id,country_code,key,value from "
                                   "(select open_app_accout_info.account_id, created_at, country_code, key, value, updated_at from "
                                   "(select account_id, open_app.created_at, country_code from "
                                   "(select account_id,created_at from buzzbreak-model-240306.stream_events.app_open where created_at>='" + y_start_time.strftime("%Y-%m-%d %H:%M:%S") + "' and created_at<'" + y_end_time.strftime("%Y-%m-%d %H:%M:%S") + "') as open_app "
                                   " LEFT JOIN buzzbreak-model-240306.input.accounts as acounts ON open_app.account_id = acounts.id where name is not null and country_code in (" + self.country_code + ")) as open_app_accout_info"
                                   " LEFT JOIN (select account_id, key, value, updated_at from buzzbreak-model-240306.partiko.memories where key like 'experiment%'  and value in (" + self.indicator_dimension + ")) as memories ON open_app_accout_info.account_id = memories.account_id where key is not null and value is not null) as result"
                                   " where created_at>=updated_at) as result1 group by key, country_code, value")

        open_app_num = self.get_data("select key, country_code, value,count(*) as num from "
                                     "(select distinct account_id,country_code,key,value from "
                                     "(select open_app_accout_info.account_id, created_at, country_code, key, value, updated_at from "
                                     "(select account_id, open_app_res.created_at, country_code from "
                                     "(select open_app1.account_id,open_app1.created_at from (select account_id,created_at from buzzbreak-model-240306.stream_events.app_open where created_at>='" + y_start_time.strftime("%Y-%m-%d %H:%M:%S") + "' and created_at<'" + y_end_time.strftime("%Y-%m-%d %H:%M:%S") + "') as open_app1 where EXISTS(select account_id,created_at from "
                                     "(select account_id,created_at from buzzbreak-model-240306.stream_events.app_open where created_at>='" + self.start_time.strftime("%Y-%m-%d %H:%M:%S") + "' and created_at<'" + self.end_time.strftime("%Y-%m-%d %H:%M:%S") + "') as open_app2 where open_app2.account_id=open_app1.account_id)) as open_app_res"
                                     " LEFT JOIN buzzbreak-model-240306.input.accounts as acounts ON open_app_res.account_id = acounts.id where name is not null and country_code in (" + self.country_code + ")) as open_app_accout_info"
                                     " LEFT JOIN (select account_id, key, value, updated_at from buzzbreak-model-240306.partiko.memories where key like 'experiment%'  and value in (" + self.indicator_dimension + ")) as memories ON open_app_accout_info.account_id = memories.account_id where key is not null and value is not null) as result"
                                     " where created_at>=updated_at) as result1 group by key, country_code, value")
        # 结果数据存入数据库
        cursor = buzzbreak_mysql_client.cursor()
        inser_sql = "INSERT INTO " + self.table_name + " (treatment_name, country_code, dimension, retention, people_num,y_people_num , start_time, end_time, create_time) VALUES"
        now_time_utc = datetime.datetime.utcnow()
        flag = False
        for key in open_app_num.keys():
            today_num = today_open_app_num.get(key, 0)
            yesterday_num = yesterday_open_app_num.get(key, 0)
            open_num = open_app_num.get(key)
            temp_data = key.split("&&")
            inser_sql = inser_sql + " ('" + temp_data[0] + "','" + temp_data[1] + "','" + temp_data[2] + "'," + str(round(open_num/yesterday_num, 5)) + "," + str(today_num) + "," + str(yesterday_num) + ",'" + self.start_time.strftime("%Y-%m-%d %H:%M:%S") + "','" + self.end_time.strftime("%Y-%m-%d %H:%M:%S") + "','" + now_time_utc.strftime("%Y-%m-%d %H:%M:%S") + "'),"
            flag = True

        if flag:
            inser_sql = inser_sql[:len(inser_sql)-1]
            try:
                # 执行sql语句
                cursor.execute(inser_sql)
                # 提交到数据库执行
                buzzbreak_mysql_client.commit()
                self.logger.info("start_time={}, end_time={} insert tabel {} success".format(self.start_time, self.end_time, self.table_name))
            except:
                self.logger.exception("start_time={}, end_time={} insert tabel {} err msg".format(self.start_time, self.end_time, self.table_name))
                # 如果发生错误则回滚
                buzzbreak_mysql_client.rollback()
        if cursor:
            cursor.close()





