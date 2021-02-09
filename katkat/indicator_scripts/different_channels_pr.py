from utils.bigquery import katkat_bigquery_client
from utils.mysql import katkat_mysql_client
import datetime


class DifferentChannelsPRData(object):

    # 构造函数， 初始化数据
    """
        start_time:指标计算的开始时间
        end_time：指标计算的结束时间
        channel: 指APP不同分类下的视频页面
        table_name：计算结果存的表
    """
    def __init__(self, start_time, end_time, channel, table_name, logger=None):
        self.start_time = start_time
        self.end_time = end_time
        self.channel = channel
        self.table_name = table_name
        self.logger = logger

    # 查询bigquery，并解析组装数据
    def get_data(self, sql):
        res_num = {}
        bq_job = katkat_bigquery_client.query(sql).to_dataframe()
        for index, row in bq_job.iterrows():
            placement = row["placement"]
            num = row["num"]
            if placement:
                res_num[placement] = num
        return res_num

    # 组装查询sql，并将统计计算结果存入mysql
    def compute_data(self):
        start_time = self.start_time.strftime("%Y-%m-%d %H:%M:%S")
        end_time = self.end_time.strftime("%Y-%m-%d %H:%M:%S")
        home_channels_num_sql = \
            f"""
                with 
                account_home_tab_for_you_and_home as (
                select distinct account_id, json_extract_scalar(data, "$.placement") as placement from katkat-298407.stream_events.video_impression where created_at >= "{start_time}" and created_at < "{end_time}" and json_extract_scalar(data, "$.placement") = "home_tab_for_you" 
                union distinct
                select distinct account_id, json_extract_scalar(data, "$.placement") as placement from katkat-298407.stream_events.video_impression where created_at >= "{start_time}" and created_at < "{end_time}" and json_extract_scalar(data, "$.placement") = "home_tab_home")

                select "home_tab_for_you_and_home" as placement, count(*) as num from account_home_tab_for_you_and_home
            """
        home_channels_num = self.get_data(home_channels_num_sql)


        different_channels_num_sql = \
            f"""
                with
                account_all_pages as (select distinct account_id, json_extract_scalar(data, "$.placement") as placement from katkat-298407.stream_events.video_impression where created_at >= "{start_time}" and created_at < "{end_time}" and json_extract_scalar(data, "$.placement") in ({self.channel}))

                select placement, count(*) as num from account_all_pages group by placement
            """
        different_channels_num = self.get_data(different_channels_num_sql)

        # 结果数据存入数据库
        insert_sql = "INSERT INTO " + self.table_name + " (placement, different_channels_num, home_channels_num, pr, start_time, end_time, create_time) VALUES"
        now_time_utc = datetime.datetime.utcnow()
        flag = False
        home_num = home_channels_num.get("home_tab_for_you_and_home", 0)
        if home_num != 0:
            for key in different_channels_num.keys():
                different_num = different_channels_num.get(key, 0)           
                insert_sql = insert_sql + " ('" + key + "'," + str(different_num) + "," + str(home_num) + "," + str(round(different_num/home_num, 5)) + ",'" + self.start_time.strftime("%Y-%m-%d %H:%M:%S") + "','" + self.end_time.strftime("%Y-%m-%d %H:%M:%S") + "','" + now_time_utc.strftime("%Y-%m-%d %H:%M:%S") + "'),"
                flag = True

            if flag:
                insert_sql = insert_sql[:len(insert_sql)-1]
                katkat_mysql_client.execute_sql(insert_sql)
        else:
            self.logger.exception("err msg home_num = 0")




