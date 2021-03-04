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
from utils.mysql import MySQL
from utils.mongo import Mongo
from utils.logger import Logger
from utils.utils import get_previous_start_time, get_today_start_time, getRestSeconds
from utils.constants import MYSQL_INSERT_SUCCCESS, MYSQL_INSERT_FAIL

# 常规指标
from indicator_scripts import different_channels_pr
from indicator_scripts import new_users_channels_average_of_duration
from indicator_scripts import new_users_video_watch_different_placement_average_of_duration
from indicator_scripts import new_users_video_watch_average
from indicator_scripts import new_users_channels_retention
from indicator_scripts import new_users_same_channels_retention
from indicator_scripts import cash_out
from indicator_scripts import all_users_ad_impression_avg
from indicator_scripts import all_users_video_watch_average
from indicator_scripts import all_users_video_watch_average_of_duration

# 新用户指标
NEW_USER_KIND = {
    
}
# 常规指标
KIND = {    
    "different_channels_pr": "different_channels_pr",   # 不同channel渗透率
    "new_users_channels_average_of_duration": "new_users_channels_average_of_duration",    # 新用户不同channel的平均时长
    "new_users_video_watch_different_placement_average_of_duration": "new_users_video_watch_different_placement_average_of_duration",     # 新用户不同placement的平均观看时长(video_watch)
    "new_users_video_watch_average": "new_users_video_watch_average",    # 新用户不同位置的平均观看次数
    "new_users_channels_retention": "new_users_channels_retention",  # 新用户不同channel的留存
    "new_users_same_channels_retention": "new_users_same_channels_retention",  # 新用户相同channel的留存
    "cash_out": "cash_out",  # 统计打钱，按国家和天
    "all_users_all_users_ad_impression_avg": "all_users_ad_impression_avg",   # 所有用户不同位置广告的平均曝光次数
    "all_users_video_watch_average": "all_users_video_watch_average",    # 所有用户不同位置的平均观看次数
    "all_users_video_watch_average_of_duration": "all_users_video_watch_average_of_duration",     # 新用户不同placement的平均观看时长(video_watch)
}

class AutoSyncMainDay:
    def __init__(self, logger):
        self.video_placement = "'" + "','".join(constants.KATKAT_VIDEO_PLACEMENT) + "'"
        self.ad_placement = "'" + "','".join(constants.KATKAT_AD_PLACEMENT) + "'"
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

            elif key == "new_users_video_watch_different_placement_average_of_duration":
                new_users_video_watch_different_placement_average_of_duration.NewUsersVideoWatchDifferentPlacementsAverageOfDuration(start_time, end_time, self.video_placement, "new_users_video_watch_different_placement_average_of_duration", logger).compute_data("{}/SQL/{}.sql".format(DIR, "new_users_video_watch_different_placement_average_of_duration"))

            elif key == "new_users_video_watch_average":
                new_users_video_watch_average.NewUsersVideoWatchAverage(start_time, end_time, self.video_placement, "new_users_video_watch_average", logger).compute_data("{}/SQL/{}.sql".format(DIR, "new_users_video_watch_average"))
                
            elif key == "new_users_channels_retention":
                new_users_channels_retention.NewUsersChannelsRetention(start_time, end_time, self.channel, value, logger).compute_data("{}/SQL/{}.sql".format(DIR, value))
                
                
            elif key == "new_users_same_channels_retention":
                new_users_same_channels_retention.NewUsersSameChannelsRetention(start_time, end_time, self.channel, value, logger).compute_data("{}/SQL/{}.sql".format(DIR, value))

            elif key == "cash_out":
                cash_out.CashOut(start_time, end_time, value, logger).compute_data("{}/SQL/{}.sql".format(DIR, value))

            elif key == "all_users_ad_impression_avg":
                all_users_ad_impression_avg.AdImpressionAvg(start_time, end_time, self.ad_placement, "all_users_ad_impression_avg", logger).compute_data("{}/SQL/{}.sql".format(DIR, "all_users_ad_impression_avg"))

            elif key == "all_users_video_watch_average":
                all_users_video_watch_average.AllUsersVideoWatchAverage(start_time, end_time, self.video_placement, "all_users_video_watch_average", logger).compute_data("{}/SQL/{}.sql".format(DIR, "all_users_video_watch_average"))

            elif key == "all_users_video_watch_average_of_duration":
                all_users_video_watch_average_of_duration.AllUsersVideoWatchAverageOfDuration(start_time, end_time, self.video_placement, "all_users_video_watch_average_of_duration", logger).compute_data("{}/SQL/{}.sql".format(DIR, "all_users_video_watch_average_of_duration"))

            indicator_end_time = datetime.datetime.now()
            indicator_use_time = indicator_end_time - indicator_start_time
            self.logger.info("sync {} indicator end use {} seconds".format(key, indicator_use_time))

if __name__ == "__main__":
    logger = Logger("KatKat Auto Sync Main Day", os.path.join(DIR, 'logs/auto_sync_main_day.log'), users=["teddy"])
    sync_tables = ["input.accounts", "partiko.account_profiles", "partiko.withdraw_transactions", "partiko.point_transactions"]
    sync_tables_str = "'" + "', '".join(sync_tables) + "'"
    fields = ["table_name", "updated_at"]
    while True:
        katkat_mysql_client = MySQL("MYSQL_KATKAT")
        katkat_mongo_client = Mongo("MONGO_ANALYTICS") 
        sql_mysql = "select table_name, max(updated_at) FROM {} group by table_name".format("main_day_involed_bigquery_tables")
        katkat_mysql_client.execute_sql(sql_mysql)
        mysql_res = katkat_mysql_client.cursor.fetchall()
        if not mysql_res:
            logger.alert("Sync katkat indicator scripts by day fail due to mysql_res is None.")
            sys.exit(0)
        logger.info("mysql_res: {}".format(mysql_res))
        mongo_sql = [{"$match": {"project": "katkat"}}, {"$match": {"bigquery_table": {"$in": sync_tables}}}, {"$match": {"status": "success"}}, {"$group": {"_id": "$bigquery_table", "created_at": {"$last": "$created_at"}}}, {"$sort":{"created_at":-1}}]
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
        time_format = "%Y-%m-%d %H:%M:%S"
        if condition == 1:
            sync_start_time = datetime.datetime.now()
            sync_start_time_str = sync_start_time.strftime(time_format)
            logger.info("katkat sync indicator scripts by day {} start!".format(sync_start_time_str))
            AutoSyncMainDay(logger).work_on(start_time, end_time)
            katkat_mysql_client.insert_dict("main_day_involed_bigquery_tables", mongo_dic)
            sync_end_time = datetime.datetime.now()
            sync_end_time_str = sync_end_time.strftime(time_format)
            use_time = sync_end_time - sync_start_time
            logger.info("katkat sync indicator scripts by day {} complete, use_time={}".format(sync_end_time_str, use_time))
            katkat_mysql_client.close_client()
            katkat_mongo_client.close_client()
            if use_time.total_seconds() > 60*60:
                logger.alert("katkat execute sync indicator scripts over one hour, please delay FinBI sync time")
            sleep_time = getRestSeconds(datetime.datetime.utcnow()) + 60*60*1 + 60*3
            time.sleep(sleep_time)
        elif condition == 2:
            logger.info("sync katkat indicator scripts by day {} fail due to date_diff = {}".format(start_time.strftime("%Y-%m-%d"), date_diff))
            logger.alert("sync katkat indicator scripts by day {} fail due to date_diff = {}".format(start_time.strftime("%Y-%m-%d"), date_diff))
            sys.exit(0)
        elif condition == 0:
            logger.info("mongo sync katkat log fail auto_sync_time={}".format(now_time_utc.strftime(time_format)))
            time.sleep(60*60*1)

