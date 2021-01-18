from utils.bigquery import katkat_bigquery_client
from utils.mysql import katkat_mysql_client
import datetime


class DIFFERENTPAGESPRData(object):

    # 构造函数， 初始化数据
    """
        start_time:指标计算的开始时间
        end_time：指标计算的结束时间
        table_name：计算结果存的表
    """
    def __init__(self, start_time, end_time, category, table_name, logger=None):
        self.start_time = start_time
        self.end_time = end_time
        self.category = category
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
        home_pages_num_sql = \
            f"""
                with 
                account_home_tab_for_you_and_home as (
                select distinct account_id, json_extract_scalar(data, "$.placement") as placement from katkat-298407.stream_events.video_impression where created_at >= "{start_time}" and created_at < "{end_time}" and json_extract_scalar(data, "$.placement") = "home_tab_for_you" 
                union distinct
                select distinct account_id, json_extract_scalar(data, "$.placement") as placement from katkat-298407.stream_events.video_impression where created_at >= "{start_time}" and created_at < "{end_time}" and json_extract_scalar(data, "$.placement") = "home_tab_home")

                select "home_tab_for_you_and_home" as placement, count(*) as num from account_home_tab_for_you_and_home
            """
        home_pages_num = self.get_data(home_pages_num_sql)


        different_pages_num_sql = \
            f"""
                with
                account_all_pages as (select distinct account_id, json_extract_scalar(data, "$.placement") as placement from katkat-298407.stream_events.video_impression where created_at >= "{start_time}" and created_at < "{end_time}" and json_extract_scalar(data, "$.placement") in ({self.category}))

                select placement, count(*) as num from account_all_pages group by placement
            """
        different_pages_num = self.get_data(different_pages_num_sql)

        # 结果数据存入数据库
        cursor = katkat_mysql_client.cursor()
        inser_sql = "INSERT INTO " + self.table_name + " (placement, different_pages_num, home_pages_num, pr, start_time, end_time, create_time) VALUES"
        now_time_utc = datetime.datetime.utcnow()
        print("home_tab_for_you_and_home", home_tab_for_you_and_home)
        flag = False
        for key in different_pages_num.keys():
            different_num = different_pages_num.get(key, 0)
            home_num = home_pages_num.get("home_tab_for_you_and_home")
            
            inser_sql = inser_sql + " ('" + key + "'," + str(different_num) + "," + str(home_num) + "," + str(round(different_num/home_num, 5)) + ",'" + self.start_time.strftime("%Y-%m-%d %H:%M:%S") + "','" + self.end_time.strftime("%Y-%m-%d %H:%M:%S") + "','" + now_time_utc.strftime("%Y-%m-%d %H:%M:%S") + "'),"
            flag = True

        if flag:
            inser_sql = inser_sql[:len(inser_sql)-1]
            try:
                # 执行sql语句
                cursor.execute(inser_sql)
                # 提交到数据库执行
                katkat_mysql_client.commit()
                self.logger.info("start_time={}, end_time={} insert tabel {} success".format(self.start_time, self.end_time, self.table_name))
            except:
                self.logger.exception("start_time={}, end_time={} insert tabel {} err msg".format(self.start_time, self.end_time, self.table_name))
                # 如果发生错误则回滚
                katkat_mysql_client.rollback()
        if cursor:
            cursor.close()





