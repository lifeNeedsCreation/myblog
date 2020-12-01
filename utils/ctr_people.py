from utils.bigquery import bigquery_client
from utils.mysql import mysql_client
import datetime


class CTRPeopleData(object):

    # 构造函数， 初始化数据
    """
        start_time:指标计算的开始时间
        end_time：指标计算的结束时间
        country_code：需要计算的国家
        placement：app上需要计算的位置
        indicator_dimension：需要计算的实验组的维度
        table_name：计算结果存的表
    """
    def __init__(self, start_time, end_time, country_code, placement, indicator_dimension, table_name):
        self.start_time = start_time
        self.end_time = end_time
        self.country_code = country_code
        self.placement = placement
        self.indicator_dimension = indicator_dimension
        self.table_name = table_name

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

    # 组装查询sql，并将统计计算结果存入mysql
    def compute_data(self):
        click_data = self.get_data("select placement, key, country_code, value,count(*) as num from "
                                   "(select distinct account_id,placement,news_id,country_code,key,value from "
                                   "(select click_accout_info.account_id, created_at, placement, news_id, country_code, key, value, updated_at from "
                                   "(select account_id, click_data.created_at, placement, news_id, country_code from "
                                   "(select account_id, created_at, json_extract_scalar(data, '$.placement') as placement, json_extract_scalar(data, '$.news_post.id') as news_id from buzzbreak-model-240306.stream_events.news_click where created_at>='" + self.start_time.strftime("%Y-%m-%d %H:%M:%S") + "' and created_at<'" + self.end_time.strftime("%Y-%m-%d %H:%M:%S") + "') as click_data "
                                   " LEFT JOIN buzzbreak-model-240306.input.accounts as acounts ON click_data.account_id = acounts.id where name is not null and country_code in (" + self.country_code + ") and placement in (" + self.placement + ")) as click_accout_info"
                                   " LEFT JOIN (select account_id, key, value, updated_at from buzzbreak-model-240306.partiko.memories where key like 'experiment%' and value in (" + self.indicator_dimension + ")) as memories ON click_accout_info.account_id = memories.account_id where key is not null and value is not null) as result "
                                   "where created_at>=updated_at) as result1 group by placement, key, country_code, value")


        impression_data = self.get_data("select placement, key, country_code, value,count(*) as num from "
                                        "(select distinct account_id,placement,news_id,country_code,key,value from "
                                        "(select impression_accout_info.account_id, created_at, placement, news_id, country_code, key, value, updated_at from "
                                        "(select account_id, impression_data.created_at, placement, news_id, country_code  from "
                                        "(select account_id, created_at, json_extract_scalar(data, '$.placement') as placement, json_extract_scalar(data, '$.id') as news_id from buzzbreak-model-240306.stream_events.news_impression where created_at>='" + self.start_time.strftime("%Y-%m-%d %H:%M:%S") + "' and created_at<'" + self.end_time.strftime("%Y-%m-%d %H:%M:%S") + "') as impression_data"
                                        " LEFT JOIN buzzbreak-model-240306.input.accounts as acounts ON impression_data.account_id = acounts.id where name is not null and country_code in (" + self.country_code + ") and placement in (" + self.placement + ")) as impression_accout_info"
                                        " LEFT JOIN (select account_id, key, value, updated_at from buzzbreak-model-240306.partiko.memories where key like 'experiment%' and value in (" + self.indicator_dimension + ")) as memories ON impression_accout_info.account_id = memories.account_id where key is not null and value is not null) as result "
                                        "where created_at>=updated_at) as result1 group by placement, key, country_code, value")


        impression_data_union = self.get_data("select placement, key, country_code, value, count(*) as num from "
                                              "(select distinct account_id, placement, news_id, country_code, key, value from "
                                              "(select impression_accout_info.account_id, created_at, placement, news_id, country_code, key, value, updated_at from "
                                              "(select account_id, impression_data.created_at, placement, news_id, country_code  from "
                                              "(select account_id, created_at, json_extract_scalar(data, '$.placement') as placement, json_extract_scalar(data, '$.id') as news_id from buzzbreak-model-240306.stream_events.news_impression where created_at>='" + self.start_time.strftime("%Y-%m-%d %H:%M:%S") + "' and created_at<'" + self.end_time.strftime("%Y-%m-%d %H:%M:%S") + "') as impression_data"
                                              " LEFT JOIN buzzbreak-model-240306.input.accounts as acounts ON impression_data.account_id = acounts.id where name is not null and country_code in (" + self.country_code + ") and placement in (" + self.placement + ")) as impression_accout_info"
                                              " LEFT JOIN (select account_id, key, value, updated_at from buzzbreak-model-240306.partiko.memories where key like 'experiment%' and value in (" + self.indicator_dimension + ")) as memories ON impression_accout_info.account_id = memories.account_id where key is not null and value is not null) as result_data "
                                              "where created_at>=updated_at UNION DISTINCT "
                                              "select distinct account_id, placement, news_id, country_code, key, value from "
                                              "(select click_accout_info.account_id, created_at, placement, news_id, country_code, key, value, updated_at from "
                                              "(select account_id, click_data.created_at, placement, news_id, country_code from "
                                              "(select account_id, created_at, json_extract_scalar(data, '$.placement') as placement, json_extract_scalar(data, '$.news_post.id') as news_id from buzzbreak-model-240306.stream_events.news_click where created_at>='" + self.start_time.strftime("%Y-%m-%d %H:%M:%S") + "' and created_at<'" + self.end_time.strftime("%Y-%m-%d %H:%M:%S") + "') as click_data"
                                              " LEFT JOIN buzzbreak-model-240306.input.accounts as acounts ON click_data.account_id = acounts.id where name is not null and country_code in (" + self.country_code + ") and placement in (" + self.placement + ")) as click_accout_info"
                                              " LEFT JOIN (select account_id, key, value, updated_at from buzzbreak-model-240306.partiko.memories where key like 'experiment%' and value in (" + self.indicator_dimension + ")) as memories ON click_accout_info.account_id = memories.account_id where key is not null and value is not null) as result_data1 "
                                              "where created_at>=updated_at) as result group by placement, key, country_code, value")
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





