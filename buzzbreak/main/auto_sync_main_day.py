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
from utils.bigquery import buzzbreak_bigquery_client
from utils.mysql import buzzbreak_mysql_client
from utils.mongo import buzzbreak_mongo_client
from utils.logger import Logger
from utils.utils import get_previous_start_time, get_today_start_time, getRestSeconds
from utils.constants import MYSQL_INSERT_SUCCCESS, MYSQL_INSERT_FAIL

# 新用户指标
from indicator_scripts import new_user_indicator

# 其他指标
from indicator_scripts import ctr
from indicator_scripts import ctr_people
from indicator_scripts import news_ctr_notification_new_user
from indicator_scripts import news_ctr_notification_new_user_people
from indicator_scripts import news_ctr_notification_old_user
from indicator_scripts import news_ctr_notification_old_user_people
from indicator_scripts import video_ctr_notification_new_user
from indicator_scripts import video_ctr_notification_new_user_people
from indicator_scripts import video_ctr_notification_old_user
from indicator_scripts import video_ctr_notification_old_user_people
from indicator_scripts import new_user_news_click_average
from indicator_scripts import new_user_video_watch_average
from indicator_scripts import old_user_video_watch_average
from indicator_scripts import video_watch_average
from indicator_scripts import video_ctr
from indicator_scripts import video_ctr_people
from indicator_scripts import new_users_retention_news_event
from indicator_scripts import new_users_retention_tab_impression
from indicator_scripts import new_users_events_retention
from indicator_scripts import old_users_events_retention
from indicator_scripts import experiment_new_users_retention_tab_impression
from indicator_scripts import partiko_memories_new_users_events_retention
from indicator_scripts import partiko_memories_old_users_events_retention
from indicator_scripts import partiko_experiment_average_of_invites
from indicator_scripts import partiko_memories_average_of_invites
from indicator_scripts import new_users_partiko_memories_average_of_invites
from indicator_scripts import partiko_memories_user_time_average_of_duration
from indicator_scripts import partiko_memories_new_user_user_time_average_of_duration
from indicator_scripts import new_user_news_ctr_people
from indicator_scripts import new_user_video_ctr_people
from indicator_scripts import partiko_experiment_new_users_retention_tab_impression
from indicator_scripts import cash_out

# 新用户指标
NEW_USER_KIND = {
    "is_click_video": ["buzzbreak-model-240306.stream_events.video_click", "new_user_day_click_video"],   # 新用户点击视频指标
    "is_click_news": ["buzzbreak-model-240306.stream_events.news_click", "new_user_day_click_news"],     # 新用户点击新闻指标
    "is_get_integral": ["buzzbreak-model-240306.partiko.point_transactions", "new_user_day_integral"],   # 新用户积分指标
    "is_withdraw": ["buzzbreak-model-240306.partiko.withdraw_transactions", "new_user_day_withdraw"],   # 新用户提现指标
    "is_invite_friends": ["buzzbreak-model-240306.partiko.referrals", "new_user_day_invite_friends"],   # 新用户邀请好友指标
}
# 常规指标
KIND = {    
    "ctr": "day_news_ctr",   # 新闻ctr
    "ctr_people": "day_news_ctr_people",  # 新闻 click_user_ratio
    "news_ctr_notification_new_user": "day_news_ctr_notification_new_user",  # 新用户新闻push的ctr
    "news_ctr_notification_new_user_people": "day_news_ctr_notification_new_user_people",  # 新用户新闻push的ctr（人）
    "news_ctr_notification_old_user": "day_news_ctr_notification_old_user",  # 老用户新闻push的ctr
    "news_ctr_notification_old_user_people": "day_news_ctr_notification_old_user_people",  # 老用户新闻push的ctr（人）
    "video_ctr_notification_new_user": "day_video_ctr_notification_new_user",   # 新用户视频push的ctr
    "video_ctr_notification_new_user_people": "day_video_ctr_notification_new_user_people",   # 新用户视频push的ctr（人）
    "video_ctr_notification_old_user": "day_video_ctr_notification_old_user",   # 老用户视频push的ctr
    "video_ctr_notification_old_user_people": "day_video_ctr_notification_old_user_people",   # 老用户视频push的ctr（人）
    "new_user_news_click_average": "day_new_user_news_click_average",    # 新用户新闻平均点击率
    "new_user_video_watch_average": "day_new_user_video_watch_average",      # 新用户视频平均观看次数
    "old_user_video_watch_average": "day_old_user_video_watch_average",      # 老用户视频平均观看次数
    "video_watch_average": "day_video_watch_average",      # 所有用户视频平均观看次数
    "new_users_retention_news_event": "new_users_retention_news_event",   # 新闻用户留存率
    "video_ctr": "day_video_ctr",   # 视频ctr
    "video_ctr_people": "day_video_ctr_people",   # 视频 click_user_ratio
    "new_users_retention_tab_impression": "new_users_retention_tab_impression",    # tab_impression 新用户留存
    "experiment_new_users_retention_tab_impression": "experiment_new_users_retention_tab_impression",     # tab_impression 实验中新用户留存
    "new_users_events_retention": "new_users_events_retention",    # 新用户在app_open与tab_impression下的留存
    "old_users_events_retention": "old_users_events_retention",    # app_open与tab_impression 老用户留存
    "partiko_memories_new_users_events_retention": "partiko_memories_new_users_events_retention",       # app_open与tab_impression 实验中新用户留存
    "partiko_memories_old_users_events_retention": "partiko_memories_old_users_events_retention",       # app_open与tab_impression 实验中老用户留存
    "partiko_experiment_average_of_invites": "partiko_experiment_average_of_invites",     # partiko.experiment 实验中的 平均邀请人数
    "partiko_memories_average_of_invites": "partiko_memories_average_of_invites",     # partiko.memories 实验中的 平均邀请人数
    "new_users_partiko_memories_average_of_invites": "new_users_partiko_memories_average_of_invites",     # partiko.memories 实验中的 新用户平均邀请人数
    "partiko_memories_user_time_average_of_duration": "partiko_memories_user_time_average_of_duration",     # partiko.memories 实验中 用户在各个页面的停留时间
    "partiko_memories_new_user_user_time_average_of_duration":  "partiko_memories_new_user_user_time_average_of_duration",  # partiko.memories 实验中新用户在各个页面的停留时间
    "new_user_news_ctr_people": "day_new_user_news_ctr_people",  # 新用户 新闻 click_user_ratio
    "new_user_video_ctr_people": "day_new_user_video_ctr_people",  # 新用户 视频 click_user_ratio
    "partiko_experiment_new_users_retention_tab_impression": "partiko_experiment_new_users_retention_tab_impression",     # partiko.experiment 实验中 新用户在各个 tab 的留存
    "cash_out": "cash_out", # 统计打钱，按国家和天
}

class AutoSyncMainDay:
    def __init__(self, logger):
        self.country_code = "'" + "','".join(constants.COUNTRY_CODE) + "'"
        self.placement = "'" + "','".join(constants.PLACEMENT) + "'"
        self.video_placement = "'" + "','".join(constants.VIDEO_PLACEMENT) + "'"
        self.video_kind_placement = "'" + "','".join(constants.VIDEO_KIND_PLACEMENT) + "'"
        self.indicator_dimension = "'" + "','".join(constants.INDICATOR_DIMENSION) + "'"
        self.logger = logger

    def work_on(self, start_time, end_time, limit_time):
        # 新用户指标
        for i in NEW_USER_KIND.keys():
            self.logger.info("sync new user {} indicator start".format(i))
            indicator_start_time = datetime.datetime.now()
            new_user_indicator.NewUserIndicator(start_time, end_time, limit_time, self.country_code, NEW_USER_KIND.get(i)[0], NEW_USER_KIND.get(i)[1], self.logger).compute_data()
            indicator_end_time = datetime.datetime.now()
            use_time = indicator_end_time - indicator_start_time
            self.logger.info("sync new user {} indicator end use {} seconds".format(i, use_time))

        # 常规指标
        for key, value in KIND.items():
            indicator_start_time = datetime.datetime.now()
            self.logger.info("sync {} indicator start".format(key))
            if key == "ctr":
                ctr.CTRData(start_time, end_time, self.country_code, self.placement, self.indicator_dimension, value, self.logger).compute_data()
                
            elif key == "ctr_people":
                ctr_people.CTRPeopleData(start_time, end_time, self.country_code, self.placement, self.indicator_dimension, value, self.logger).compute_data()
                
            elif key == "news_ctr_notification_new_user":
                news_ctr_notification_new_user.NewsCtrNotificationNewUserData(start_time, end_time, self.country_code, self.indicator_dimension, value, self.logger).compute_data()
                
            elif key == "news_ctr_notification_new_user_people":
                news_ctr_notification_new_user_people.NewsCtrNotificationNewUserPeopleData(start_time, end_time, self.country_code, self.indicator_dimension, value, self.logger).compute_data()
                
            elif key == "news_ctr_notification_old_user":
                news_ctr_notification_old_user.NewsCtrNotificationOldUserData(start_time, end_time, self.country_code, self.indicator_dimension, value, self.logger).compute_data()
                
            elif key == "news_ctr_notification_old_user_people":
                news_ctr_notification_old_user_people.NewsCtrNotificationOldUserPeopleData(start_time, end_time, self.country_code, self.indicator_dimension, value, self.logger).compute_data()
                
            elif key == "video_ctr_notification_new_user":
                video_ctr_notification_new_user.VideoCtrNotificationNewUserData(start_time, end_time, self.country_code, self.indicator_dimension, value, self.logger).compute_data()
                
            elif key == "video_ctr_notification_new_user_people":
                video_ctr_notification_new_user_people.VideoCtrNotificationNewUserPeopleData(start_time, end_time, self.country_code, self.indicator_dimension, value, self.logger).compute_data()
                
            elif key == "video_ctr_notification_old_user":
                video_ctr_notification_old_user.VideoCtrNotificationOldUserData(start_time, end_time, self.country_code, self.indicator_dimension, value, self.logger).compute_data()
                
            elif key == "video_ctr_notification_old_user_people":
                video_ctr_notification_old_user_people.VideoCtrNotificationOldUserPeopleData(start_time, end_time, self.country_code, self.indicator_dimension, value, self.logger).compute_data()
                
            elif key == "new_user_news_click_average":
                new_user_news_click_average.NewUserNewsClickAverageData(start_time, end_time, self.country_code, self.placement, self.indicator_dimension, "day_new_user_news_click_average", logger).compute_data()

            elif key == "new_user_video_watch_average":
                new_user_video_watch_average.NewUserVideoWatchAverageData(start_time, end_time, self.country_code, self.video_placement, self.indicator_dimension, value, self.logger).compute_data()
                
            elif key == "old_user_video_watch_average":
                old_user_video_watch_average.OldUserVideoWatchAverageData(start_time, end_time, self.country_code, self.video_placement, self.indicator_dimension, value, self.logger).compute_data()
                
            elif key == "video_watch_average":
                video_watch_average.VideoWatchAverageData(start_time, end_time, self.country_code, self.video_placement, self.indicator_dimension, value, self.logger).compute_data()
                
            elif key == "new_users_retention_news_event":
                new_users_retention_news_event.NewUsersRetentionNewsEvent(start_time, end_time, self.country_code, self.indicator_dimension, value, self.logger).compute_data()
                
            elif key == "video_ctr":
                video_ctr.VideoCTRData(start_time, end_time, self.country_code, self.video_placement, self.indicator_dimension, "day_video_ctr", self.logger).compute_data()
                
            elif key == "video_ctr_people":
                video_ctr_people.VideoCTRPeopleData(start_time, end_time, self.country_code, self.video_placement, self.indicator_dimension, "day_video_ctr_people", self.logger).compute_data()
                
            elif key == "new_users_retention_tab_impression":
                new_users_retention_tab_impression.NewUsersRetentionTabImpression(start_time, end_time, 'new_users_retention_tab_impression', self.logger).compute_data()
                
            elif key == "experiment_new_users_retention_tab_impression":
                experiment_new_users_retention_tab_impression.ExperimentNewUsersRetentionTabImpression(start_time, end_time, self.indicator_dimension, value, self.logger).compute_data()
                
            elif key == "new_users_events_retention":
                new_users_events_retention.NewUsersEventsRetention(start_time, end_time, value, self.logger).compute_data()
                
            elif key == "old_users_events_retention":
                old_users_events_retention.OldUsersEventsRetention(start_time, end_time, value, self.logger).compute_data()
                
            elif key == "partiko_memories_new_users_events_retention":
                partiko_memories_new_users_events_retention.PartikoMemoriesNewUsersEventsRetention(start_time, end_time, self.indicator_dimension, value, self.logger).compute_data()
                
            elif key == "partiko_memories_old_users_events_retention":
                partiko_memories_old_users_events_retention.PartikoMemoriesOldUsersEventsRetention(start_time, end_time, self.indicator_dimension, value, self.logger).compute_data()
                
            elif key == "partiko_experiment_average_of_invites":
                partiko_experiment_average_of_invites.PartikoExperimentAverageOfInvites(start_time, end_time, self.indicator_dimension, value, self.logger).compute_data()
                
            elif key == "partiko_memories_average_of_invites":
                partiko_memories_average_of_invites.PartikoMemoriesAverageOfInvites(start_time, end_time, self.indicator_dimension, value, self.logger).compute_data()
                
            elif key == "new_users_partiko_memories_average_of_invites":
                new_users_partiko_memories_average_of_invites.NewUsersPartikoMemoriesAverageOfInvites(start_time, end_time, self.indicator_dimension, value, self.logger).compute_data()
                
            elif key == "partiko_memories_user_time_average_of_duration":
                partiko_memories_user_time_average_of_duration.PartikoMemoriesUserTimeAverageOfDuration(start_time, end_time, self.indicator_dimension, value, self.logger).compute_data()
                
            elif key == "partiko_memories_new_user_user_time_average_of_duration":
                partiko_memories_new_user_user_time_average_of_duration.PartikoMemoriesNewUserUserTimeAverageOfDuration(start_time, end_time, self.indicator_dimension, 'partiko_memories_new_user_user_time_average_of_duration', logger).compute_data("{}/SQL/{}.sql".format(DIR, "partiko_memories_new_user_user_time_average_of_duration"))

            elif key == "new_user_news_ctr_people":
                new_user_news_ctr_people.NewUserCTRPeopleData(start_time, end_time, self.country_code, self.placement, self.indicator_dimension, value, self.logger).compute_data()
                
            elif key == "new_user_video_ctr_people":
                new_user_video_ctr_people.NewUserVideoCTRPeopleData(start_time, end_time, self.country_code, self.video_placement, self.indicator_dimension, value, self.logger).compute_data()
                
            elif key == "partiko_experiment_new_users_retention_tab_impression":
                partiko_experiment_new_users_retention_tab_impression.PartikoExperimentNewUsersRetentionTabImpression(start_time, end_time, self.indicator_dimension, value, self.logger).compute_data()

            elif key == "cash_out":
                cash_out.CashOut(start_time, end_time, "cash_out", logger).compute_data("{}/SQL/{}.sql".format(DIR, "cash_out"))

            indicator_end_time = datetime.datetime.now()
            indicator_use_time = indicator_end_time - indicator_start_time
            self.logger.info("sync {} indicator end use {} seconds".format(key, indicator_use_time))

if __name__ == "__main__":
    logger = Logger("BuzzBreak Auto Sync Main Day", os.path.join(DIR, 'logs/auto_sync_main_day.log'), users=["teddy"])
    sync_tables = ["input.accounts", "partiko.memories", "partiko.account_profiles", "partiko.point_transactions", "partiko.withdraw_transactions", "partiko.referrals"]
    sync_tables_str = "'" + "', '".join(sync_tables) + "'"
    fields = ["table_name", "updated_at"]
    while True:
        sql_mysql = "select table_name, max(updated_at) FROM {} group by table_name order by field(table_name, {})".format("main_day_involed_bigquery_tables", sync_tables_str)
        buzzbreak_mysql_client.execute_sql(sql_mysql)
        mysql_res = buzzbreak_mysql_client.cursor.fetchall()
        if not mysql_res:
            logger.alert("Sync buzzbreak indicator scripts by day fail due to mysql_res is None.")
            sys.exit(0)
        logger.info("mysql_res: {}".format(mysql_res))
        mongo_sql = [{"$match": {"project": "buzzbreak"}}, {"$match": {"bigquery_table": {"$in": sync_tables}}}, {"$match": {"status": "success"}}, {"$group": {"_id": "$bigquery_table", "created_at": {"$last": "$created_at"}}}, {"$sort":{"created_at":-1}}]
        mongo_res = buzzbreak_mongo_client.find_sync_tables(mongo_sql)
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
            logger.info("sync indicator scripts by day {} start!".format(sync_start_time_str))
            # 新用户行为的时间上限
            limit_time = end_time + datetime.timedelta(hours=8)
            AutoSyncMainDay(logger).work_on(start_time, end_time, limit_time)
            buzzbreak_mysql_client.insert_dict("main_day_involed_bigquery_tables", mongo_dic)
            sync_end_time = datetime.datetime.now()
            sync_end_time_str = sync_end_time.strftime(time_format)
            use_time = sync_end_time - sync_start_time
            logger.info("buzzbreak sync indicator scripts by day {} complete, use_time={}".format(sync_end_time_str, use_time))
            if use_time.hours > 1:
                logger.alert("buzzbreak execute sync indicator scripts over one hour, please delay FinBI sync time")
            sleep_time = getRestSeconds(datetime.datetime.utcnow()) + 60*60*1
            time.sleep(sleep_time)
        elif condition == 2:
            logger.info("sync buzzbreak indicator scripts by day {} fail due to date_diff = {}".format(start_time.strftime("%Y-%m-%d"), date_diff))
            logger.alert("sync buzzbreak indicator scripts by day {} fail due to date_diff = {}".format(start_time.strftime("%Y-%m-%d"), date_diff))
            sys.exit(0)
        elif condition == 0:
            if now_time_utc.hour < 2:
                logger.info("mongo sync buzzbreak log fail auto_sync_time={}".format(now_time_utc.strftime("%Y-%m-%d %H:%M:%S")))
                time.sleep(60*60*0.5)
            else:
                logger.info("buzzbreak auto_sync fail due to mongo sync log fail auto_sync_time={}".format(now_time_utc.strftime("%Y-%m-%d %H:%M:%S")))
                logger.alert("buzzbreak auto_sync fail due to mongo sync log fail auto_sync_time={}".format(now_time_utc.strftime("%Y-%m-%d %H:%M:%S")))
                time.sleep(60*60*6)

