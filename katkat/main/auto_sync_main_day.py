import os
import sys
import time
import getopt
import datetime

BASE_DIR = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.append(BASE_DIR)
sys.path.append(DIR)

from utils import constants
from utils.bigquery import katkat_bigquery_client
from utils.mysql import katkat_mysql_client
from utils.mongo import katkat_mongo_client
from utils.logger import Logger
from utils.utils import get_previous_start_time, get_today_start_time, getRestSeconds
from utils.constants import MYSQL_INSERT_SUCCCESS, MYSQL_INSERT_FAIL

# 常规指标
from indicator_scripts import different_channels_pr
from indicator_scripts import new_users_channels_average_of_duration
from indicator_scripts import new_users_channels_retention
from indicator_scripts import new_users_same_channels_retention
from indicator_scripts import cash_out

# 新用户指标
NEW_USER_KIND = {
    
}
# 常规指标
KIND = {    
    "different_channels_pr": "different_channels_pr",   # 不同channel渗透率
    "new_users_channels_average_of_duration": "new_users_channels_average_of_duration",    # 新用户不同channel的平均时长
    "new_users_channels_retention": "new_users_channels_retention",  # 新用户不同channel的留存
    "new_users_same_channels_retention": "new_users_same_channels_retention",  # 新用户相同channel的留存
    "cash_out": "cash_out",  # # 统计打钱，按国家和天
}

class AutoSyncMainDay:
    def __init__(self, logger):
        self.country_code = "'" + "','".join(constants.COUNTRY_CODE) + "'"
        self.placement = "'" + "','".join(constants.PLACEMENT) + "'"
        self.video_placement = "'" + "','".join(constants.VIDEO_PLACEMENT) + "'"
        self.video_kind_placement = "'" + "','".join(constants.VIDEO_KIND_PLACEMENT) + "'"
        self.indicator_dimension = "'" + "','".join(constants.INDICATOR_DIMENSION) + "'"
        self.channel = "'" + "','".join(constants.KATKAT_VIDEO_CHANNEL) + "'"
        self.logger = logger

    def work_on(self, start_time, end_time):

        # 常规指标
        for key, value in KIND.items():
            indicator_start_time = datetime.datetime.now()
            self.logger.info("sync {} indicator start".format(key))
            if key == "different_channels_pr":
                different_channels_pr.DifferentChannelsPRData(start_time, end_time, self.channel, value, logger).compute_data()
                
            elif key == "new_users_channels_average_of_duration":
                new_users_channels_average_of_duration.NewUsersChannelsAverageOfDuration(start_time, end_time, self.channel, value, logger).compute_data("{}/SQL/{}.sql".format(DIR, value))
                
            elif key == "new_users_channels_retention":
                new_users_channels_retention.NewUsersChannelsRetention(start_time, end_time, self.channel, value, logger).compute_data("{}/SQL/{}.sql".format(DIR, value))
                
                
            elif key == "new_users_same_channels_retention":
                new_users_same_channels_retention.NewUsersSameChannelsRetention(start_time, end_time, self.channel, value, logger).compute_data("{}/SQL/{}.sql".format(DIR, value))

            elif key == "cash_out":
                cash_out.CashOut(start_time, end_time, value, logger).compute_data("{}/SQL/{}.sql".format(DIR, value))
            

        indicator_end_time = datetime.datetime.now()
        use_time = indicator_end_time - indicator_start_time
        self.logger.info("sync {} indicator end use {} seconds".format(key, use_time))

if __name__ == "__main__":
    logger = Logger("Auto Sync Main Day", os.path.join(DIR, 'logs/auto_sync_main_day.log'))
    sync_tables = ["input.accounts", "partiko.memories", "partiko.account_profiles", "partiko.point_transactions", "partiko.withdraw_transactions", "partiko.referrals"]
    sync_tables_str = "'" + "', '".join(sync_tables) + "'"
    fields = ["table_name", "updated_at"]
    while True:
        sql_mysql = "select table_name, max(updated_at) FROM {} group by table_name order by field(table_name, {})".format("main_day_involed_bigquery_tables", sync_tables_str)
        katkat_mysql_client.execute_sql(sql_mysql)
        mysql_res = katkat_mysql_client.cursor.fetchall()
        if not mysql_res:
            logger.alert("Sync katkat indicator scripts by day fail due to mysql_res is None.")
            sys.exit(0)
        logger.info("mysql_res: {}".format(mysql_res))
        mongo_sql = [{"$match": {"project": "buzzbreak"}}, {"$match": {"bigquery_table": {"$in": sync_tables}}}, {"$match": {"status": "success"}}, {"$group": {"_id": "$bigquery_table", "created_at": {"$last": "$created_at"}}}, {"$sort":{"created_at":-1}}]
        mongo_res = katkat_mongo_client.find_sync_tables(mongo_sql)
        mongo_dic = {}
        for doc in mongo_res:
            mongo_dic[doc["_id"]] = doc["created_at"]
        logger.info("mongo_res: {}".format(mongo_dic))
        date_diff = []
        for log in mysql_res:
            mysql_time = log[1]
            mongo_time = mongo_dic[log[0]]
            mysql_date = get_today_start_time(mysql_time)
            mongo_date = get_today_start_time(mongo_time)
            date_diff.append((mongo_date - mysql_date).days)
        logger.info("date_diff: {}".format(date_diff))
        now_time_utc = datetime.datetime.utcnow()
        start_time = get_previous_start_time(now_time_utc)
        end_time = get_today_start_time(now_time_utc)
        condition = 0
        date_diff_res = [i == 1 for i in date_diff]
        logger.info("date_diff_res: {}".format(date_diff_res))
        if all(date_diff_res):
            condition = 1
        for i in date_diff:
            if i > 1:
                condition = 2
                break
        logger.info("contion: {}".format(condition))
        if condition == 1:
            sync_start_time = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
            logger.info("sync indicator scripts by day {} start!".format(sync_start_time))
            AutoSyncMainDay(logger).work_on(start_time, end_time)
            katkat_mysql_client.insert_dict("main_day_involed_bigquery_tables", mongo_dic)
            sync_end_time = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
            logger.info("sync indicator scripts by day {} complete!".format(sync_end_time))
            sleep_time = getRestSeconds(datetime.datetime.utcnow()) + 60*60*1
            time.sleep(sleep_time)
        elif condition == 2:
            logger.info("sync indicator scripts by day {} fail due to date_diff = {}".format(start_time.strftime("%Y-%m-%d"), date_diff))
            logger.alert("sync katkat indicator scripts by day {} fail due to date_diff = {}".format(start_time.strftime("%Y-%m-%d"), date_diff))
            sys.exit(0)
        elif condition == 0:
            if now_time_utc.hour < 2:
                logger.info("mongo sync log fail auto_sync_time={}".format(now_time_utc.strftime("%Y-%m-%d %H:%M:%S")))
                time.sleep(60*60*0.5)
            else:
                logger.info("auto_sync fail due to mongo sync log fail auto_sync_time={}".format(now_time_utc.strftime("%Y-%m-%d %H:%M:%S")))
                logger.alert("katkat auto_sync fail due to mongo sync log fail auto_sync_time={}".format(now_time_utc.strftime("%Y-%m-%d %H:%M:%S")))
                time.sleep(60*60*6)

