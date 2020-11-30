from utils.bigquery import bigquery_client
from utils.mysql import mysql_client
import datetime


class CTRData(object):

    # 构造函数， 初始化数据
    def __init__(self, start_time, end_time, country_code, placement, indicator_dimension):
        self.start_time = start_time
        self.end_time = end_time
        self.country_code = country_code
        self.placement = placement
        self.indicator_dimension = indicator_dimension

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


    def compute_data(self):
        click_data = self.get_data("select placement,key,country_code,value,count(*) as num from (select distinct account_id,placement,news_id,country_code,key,value from (select click_accout_info.account_id,click_accout_info.created_at,click_accout_info.placement,click_accout_info.news_id,click_accout_info.country_code,memories.key,memories.value,memories.updated_at  from (select click_account.account_id,click_account.created_at,click_account.placement,click_account.news_id,click_account.country_code  from (select click_data.account_id, click_data.created_at, click_data.placement, click_data.news_id,acounts.name, acounts.country_code from (select account_id, created_at, json_extract_scalar(data, '$.placement') as placement, json_extract_scalar(data, '$.news_post.id') as news_id "
                                   "from buzzbreak-model-240306.stream_events.news_click where created_at>='" + self.start_time.strftime("%Y-%m-%d %H:%M:%S")
                                   + "' and created_at<'" + self.end_time.strftime("%Y-%m-%d %H:%M:%S") +"') as click_data LEFT JOIN  (select id,name,country_code from buzzbreak-model-240306.input.accounts) as acounts "
                                    "ON click_data.account_id = acounts.id) as click_account where click_account.name is not null and click_account.country_code in ('ID', 'PH', 'TH', 'AR') and click_account.placement in ('home_tab_for_you','news_detail_activity')) as click_accout_info"
                                    " LEFT JOIN (select account_id,key,value,updated_at from buzzbreak-model-240306.partiko.memories where key like 'experiment%' and value in ('control', 'treatment')) as memories ON click_accout_info.account_id = memories.account_id) as result "
                                    "where created_at>=updated_at and key is not null and value is not null) as result1 group by placement,key,country_code,value")


        impression_data = self.get_data("select placement,key,country_code,value,count(*) as num from (select distinct account_id,placement,news_id,country_code,key,value from (select impression_accout_info.account_id,impression_accout_info.created_at,impression_accout_info.placement,impression_accout_info.news_id,impression_accout_info.country_code,memories.key,memories.value,memories.updated_at  from (select impression_account.account_id,impression_account.created_at,impression_account.placement,impression_account.news_id,impression_account.country_code  from (select impression_data.account_id, impression_data.created_at, impression_data.placement, impression_data.news_id,acounts.name, acounts.country_code from (select account_id, created_at, json_extract_scalar(data, '$.placement') as placement, json_extract_scalar(data, '$.id') as news_id "
                                   "from buzzbreak-model-240306.stream_events.news_impression where created_at>='" + self.start_time.strftime("%Y-%m-%d %H:%M:%S")
                                   + "' and created_at<'" + self.end_time.strftime("%Y-%m-%d %H:%M:%S") +"') as impression_data LEFT JOIN  (select id,name,country_code from buzzbreak-model-240306.input.accounts) as acounts "
                                    "ON impression_data.account_id = acounts.id) as impression_account where impression_account.name is not null and impression_account.country_code in ('ID', 'PH', 'TH', 'AR') and impression_account.placement in ('home_tab_for_you','news_detail_activity')) as impression_accout_info"
                                    " LEFT JOIN (select account_id,key,value,updated_at from buzzbreak-model-240306.partiko.memories where key like 'experiment%' and value in ('control', 'treatment')) as memories ON impression_accout_info.account_id = memories.account_id) as result "
                                    "where created_at>=updated_at and key is not null and value is not null) as result1 group by placement,key,country_code,value")


        impression_data_union = self.get_data("select placement,key,country_code,value,count(*) as num from (select distinct account_id,placement,news_id,country_code,key,value from (select impression_accout_info.account_id,impression_accout_info.created_at,impression_accout_info.placement,impression_accout_info.news_id,impression_accout_info.country_code,memories.key,memories.value,memories.updated_at  from (select impression_account.account_id,impression_account.created_at,impression_account.placement,impression_account.news_id,impression_account.country_code  from (select impression_data.account_id, impression_data.created_at, impression_data.placement, impression_data.news_id,acounts.name, acounts.country_code from (select account_id, created_at, json_extract_scalar(data, '$.placement') as placement, json_extract_scalar(data, '$.id') as news_id "
                                   "from buzzbreak-model-240306.stream_events.news_impression where created_at>='" + self.start_time.strftime("%Y-%m-%d %H:%M:%S")
                                   + "' and created_at<'" + self.end_time.strftime("%Y-%m-%d %H:%M:%S") +"') as impression_data LEFT JOIN  (select id,name,country_code from buzzbreak-model-240306.input.accounts) as acounts "
                                    "ON impression_data.account_id = acounts.id) as impression_account where impression_account.name is not null and impression_account.country_code in ('ID', 'PH', 'TH', 'AR') and impression_account.placement in ('home_tab_for_you','news_detail_activity')) as impression_accout_info"
                                    " LEFT JOIN (select account_id,key,value,updated_at from buzzbreak-model-240306.partiko.memories where key like 'experiment%' and value in ('control', 'treatment')) as memories ON impression_accout_info.account_id = memories.account_id) as result_data "
                                    "where created_at>=updated_at and key is not null and value is not null UNION DISTINCT " + "select distinct account_id,placement,news_id,country_code,key,value from (select click_accout_info.account_id,click_accout_info.created_at,click_accout_info.placement,click_accout_info.news_id,click_accout_info.country_code,memories.key,memories.value,memories.updated_at  from (select click_account.account_id,click_account.created_at,click_account.placement,click_account.news_id,click_account.country_code  from (select click_data.account_id, click_data.created_at, click_data.placement, click_data.news_id,acounts.name, acounts.country_code from (select account_id, created_at, json_extract_scalar(data, '$.placement') as placement, json_extract_scalar(data, '$.news_post.id') as news_id "
                                   "from buzzbreak-model-240306.stream_events.news_click where created_at>='" + self.start_time.strftime("%Y-%m-%d %H:%M:%S")
                                   + "' and created_at<'" + self.end_time.strftime("%Y-%m-%d %H:%M:%S") +"') as click_data LEFT JOIN  (select id,name,country_code from buzzbreak-model-240306.input.accounts) as acounts "
                                    "ON click_data.account_id = acounts.id) as click_account where click_account.name is not null and click_account.country_code in ('ID', 'PH', 'TH', 'AR') and click_account.placement in ('home_tab_for_you','news_detail_activity')) as click_accout_info"
                                    " LEFT JOIN (select account_id,key,value,updated_at from buzzbreak-model-240306.partiko.memories where key like 'experiment%' and value in ('control', 'treatment')) as memories ON click_accout_info.account_id = memories.account_id) as result_data1 "
                                    "where created_at>=updated_at and key is not null and value is not null) as result group by placement,key,country_code,value"
                                    )
        # 结果数据存入数据库
        cursor = mysql_client.cursor()
        inser_sql = "INSERT INTO day_ctr(treatment_name, placement, country_code, dimension, ctr, ctr_union, start_time, end_time, create_time) VALUES"
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




