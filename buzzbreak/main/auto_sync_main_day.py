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
from utils.mysql import MySQL
from utils.mongo import Mongo
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
from indicator_scripts import partiko_memories_new_users_events_retention_with_impression
from indicator_scripts import partiko_memories_old_users_events_retention
from indicator_scripts import partiko_experiment_average_of_invites
from indicator_scripts import partiko_memories_average_of_invites
from indicator_scripts import new_users_partiko_memories_average_of_invites
from indicator_scripts import partiko_memories_user_time_average_of_duration
from indicator_scripts import partiko_memories_new_user_user_time_average_of_duration
from indicator_scripts import experiment_immersive_page_duration_avg
from indicator_scripts import new_user_news_ctr_people
from indicator_scripts import new_user_video_ctr_people
# from indicator_scripts import partiko_experiment_new_users_retention_tab_impression
from indicator_scripts import cash_out
from indicator_scripts import cashout_by_method
from indicator_scripts import cashout_by_created_at
from indicator_scripts import cashout_by_money
from indicator_scripts import video_ctr_recall
from indicator_scripts import video_ctr_dimension_recall
from indicator_scripts import video_ctr_by_dimension_recall
from indicator_scripts import video_ctr_recall_data_index0
from indicator_scripts import video_ctr_people_recall
from indicator_scripts import video_ctr_people_dimension_recall
from indicator_scripts import video_ctr_people_by_dimension_recall
from indicator_scripts import video_ctr_people_recall_data_index0
from indicator_scripts import video_ctr_rank
from indicator_scripts import video_ctr_people_rank
from indicator_scripts import video_watch_average_of_duration_recall
from indicator_scripts import video_watch_average_of_duration_by_dimension_recall
from indicator_scripts import video_watch_average_of_duration_rank
from indicator_scripts import immersive_retention_recall
from indicator_scripts import immersive_retention_by_dimension_recall
from indicator_scripts import immersive_retention_rank
from indicator_scripts import video_ctr_with_device_model_recall
from indicator_scripts import video_ctr_with_device_model_rank
from indicator_scripts import video_ctr_with_brand_recall
from indicator_scripts import video_ctr_with_brand_rank
from indicator_scripts import video_watch_average_by_dimension_recall
from indicator_scripts import video_watch_average_rank
from indicator_scripts import posts
from indicator_scripts import posts_user_count
from indicator_scripts import video_retention_recall
from indicator_scripts import video_retention_rank
from indicator_scripts import video_average_of_total_duration_recall
from indicator_scripts import video_average_of_total_duration_rank
from indicator_scripts import video_average_of_total_duration_by_dimension_recall
from indicator_scripts import video_ctr_without_experiments
from indicator_scripts import video_ctr_people_without_experiments
from indicator_scripts import video_watch_average_of_duration_without_experiments
from indicator_scripts import user_total_duration_average
from indicator_scripts import user_avg_cost
from indicator_scripts import short_video_ctr
from indicator_scripts import short_new_video_ctr
from indicator_scripts import short_video_completion_rate
from indicator_scripts import short_new_video_completion_rate
from indicator_scripts import one_minute_video_ctr
from indicator_scripts import one_minute_new_video_ctr
from indicator_scripts import one_minute_video_completion_rate
from indicator_scripts import one_minute_new_video_completion_rate
from indicator_scripts import five_minutes_video_ctr
from indicator_scripts import five_minutes_new_video_ctr
from indicator_scripts import five_minutes_video_completion_rate
from indicator_scripts import five_minutes_new_video_completion_rate
from indicator_scripts import push_ctr_without_experiments
from indicator_scripts import push_ctr_people_without_experiments
from indicator_scripts import notification_video_ctr_without_experiments
from indicator_scripts import notification_video_ctr_without_experiments_by_people
from indicator_scripts import notification_news_ctr_without_experiments
from indicator_scripts import notification_news_ctr_without_experiments_by_people
from indicator_scripts import new_videos_ctr
from indicator_scripts import immersive_video_watch_average_recall_by_model
from indicator_scripts import immersive_video_watch_average_of_duration_recall_by_model
from indicator_scripts import immersive_retention_recall_by_model
from indicator_scripts import immersive_video_watch_average_recall_by_bucket
from indicator_scripts import immersive_video_watch_average_of_duration_recall_by_bucket
from indicator_scripts import immersive_retention_recall_by_bucket
from indicator_scripts import immersive_video_watch_average_rough_rank_by_model
from indicator_scripts import immersive_video_watch_average_of_duration_rough_rank_by_model
from indicator_scripts import immersive_retention_rough_rank_by_model
from indicator_scripts import video_ctr_recall_by_model
from indicator_scripts import video_ctr_people_recall_by_model
from indicator_scripts import video_ctr_recall_by_bucket
from indicator_scripts import video_ctr_people_recall_by_bucket
from indicator_scripts import video_ctr_rough_rank_by_model
from indicator_scripts import video_ctr_people_rough_rank_by_model
from indicator_scripts import video_click_average_recall_by_model
from indicator_scripts import video_click_average_recall_by_bucket
from indicator_scripts import video_click_average_rough_rank_by_model
from indicator_scripts import video_retention_recall_by_model
from indicator_scripts import video_retention_recall_by_bucket
from indicator_scripts import video_retention_rough_rank_by_model
from indicator_scripts import thirty_seconds_new_user_app_open_retention
from indicator_scripts import three_minutes_new_user_app_open_retention
from indicator_scripts import five_minutes_new_user_app_open_retention
from indicator_scripts import ten_minutes_new_user_app_open_retention
from indicator_scripts import points_out_statistics
from indicator_scripts import points_out_purpose_statistics
from indicator_scripts import points_in_statistics
from indicator_scripts import new_user_invite_master
from indicator_scripts import new_user_invite_master_with_referrals
from indicator_scripts import new_user_invite_apprentice_retention
from indicator_scripts import new_user_retention_by_brand
from indicator_scripts import news_read_duration_avg
from indicator_scripts import news_read_duration_avg_by_count
from indicator_scripts import video_watch_duration_avg
from indicator_scripts import video_watch_duration_avg_by_count
from indicator_scripts import video_impression_duration_avg
from indicator_scripts import job_query_log_cost
from indicator_scripts import page_dau_and_penetration
from indicator_scripts import page_avg_time
from indicator_scripts import page_retention
from indicator_scripts import image_ctr_by_placement
from indicator_scripts import video_ctr_by_placement
from indicator_scripts import silient_user
from indicator_scripts import accounts_without_ad_click
from indicator_scripts import user_cash_value

# 新用户指标
NEW_USER_KIND = {
    # "is_click_video": ["buzzbreak-model-240306.stream_events.video_click", "new_user_day_click_video"],   # 新用户点击视频指标
    # "is_click_news": ["buzzbreak-model-240306.stream_events.news_click", "new_user_day_click_news"],     # 新用户点击新闻指标
    # "is_get_integral": ["buzzbreak-model-240306.partiko.point_transactions", "new_user_day_integral"],   # 新用户积分指标
    # "is_withdraw": ["buzzbreak-model-240306.partiko.withdraw_transactions", "new_user_day_withdraw"],   # 新用户提现指标
    # "is_invite_friends": ["buzzbreak-model-240306.partiko.referrals", "new_user_day_invite_friends"],   # 新用户邀请好友指标
}
# 常规指标
KIND = {    
    # "ctr": "day_news_ctr",   # 新闻ctr
    # "ctr_people": "day_news_ctr_people",  # 新闻 click_user_ratio
    # "news_ctr_notification_new_user": "day_news_ctr_notification_new_user",  # 新用户新闻push的ctr
    # "news_ctr_notification_new_user_people": "day_news_ctr_notification_new_user_people",  # 新用户新闻push的ctr（人）
    # "news_ctr_notification_old_user": "day_news_ctr_notification_old_user",  # 老用户新闻push的ctr
    # "news_ctr_notification_old_user_people": "day_news_ctr_notification_old_user_people",  # 老用户新闻push的ctr（人）
    "video_ctr_notification_new_user": "day_video_ctr_notification_new_user",   # 新用户视频push的ctr
    "video_ctr_notification_new_user_people": "day_video_ctr_notification_new_user_people",   # 新用户视频push的ctr（人）
    "video_ctr_notification_old_user": "day_video_ctr_notification_old_user",   # 老用户视频push的ctr
    "video_ctr_notification_old_user_people": "day_video_ctr_notification_old_user_people",   # 老用户视频push的ctr（人）
    # "new_user_news_click_average": "day_new_user_news_click_average",    # 新用户新闻平均点击率
    # "new_user_video_watch_average": "day_new_user_video_watch_average",      # 新用户视频平均观看次数
    # "old_user_video_watch_average": "day_old_user_video_watch_average",      # 老用户视频平均观看次数
    # "video_watch_average": "day_video_watch_average",      # 所有用户视频平均观看次数
    # "new_users_retention_news_event": "new_users_retention_news_event",   # 新闻用户留存率
    # "video_ctr": "day_video_ctr",   # 视频ctr
    # "video_ctr_people": "day_video_ctr_people",   # 视频 click_user_ratio
    # "new_users_retention_tab_impression": "new_users_retention_tab_impression",    # tab_impression 新用户留存
    # "experiment_new_users_retention_tab_impression": "experiment_new_users_retention_tab_impression",     # tab_impression 实验中新用户留存
    "new_users_events_retention": "new_users_events_retention",    # 新用户在app_open与tab_impression下的留存
    "old_users_events_retention": "old_users_events_retention",    # app_open与tab_impression 老用户留存
    "partiko_memories_new_users_events_retention": "partiko_memories_new_users_events_retention",       # app_open与tab_impression 实验中新用户留存
    "partiko_memories_new_users_events_retention_with_impression": "partiko_memories_new_users_events_retention_with_impression",   # 实验中新用户留存(去掉没有新闻视频曝光的用户)
    # "partiko_memories_old_users_events_retention": "partiko_memories_old_users_events_retention",       # app_open与tab_impression 实验中老用户留存
    # "partiko_experiment_average_of_invites": "partiko_experiment_average_of_invites",     # partiko.experiment 实验中的 平均邀请人数
    # "partiko_memories_average_of_invites": "partiko_memories_average_of_invites",     # partiko.memories 实验中的 平均邀请人数
    # "new_users_partiko_memories_average_of_invites": "new_users_partiko_memories_average_of_invites",     # partiko.memories 实验中的 新用户平均邀请人数
    # "partiko_memories_user_time_average_of_duration": "partiko_memories_user_time_average_of_duration",     # partiko.memories 实验中 用户在各个页面的停留时间
    # "partiko_memories_new_user_user_time_average_of_duration":  "partiko_memories_new_user_user_time_average_of_duration",  # partiko.memories 实验中新用户在各个页面的停留时间
    # "experiment_immersive_page_duration_avg":"experiment_immersive_page_duration_avg",   # 沉浸流页面用户平局停留时长
    # "new_user_news_ctr_people": "day_new_user_news_ctr_people",  # 新用户 新闻 click_user_ratio
    # "new_user_video_ctr_people": "day_new_user_video_ctr_people",  # 新用户 视频 click_user_ratio
    # "partiko_experiment_new_users_retention_tab_impression": "partiko_experiment_new_users_retention_tab_impression",     # partiko.experiment 实验中 新用户在各个 tab 的留存
    "cash_out": "cash_out", # 统计打钱，按国家和天
    "cashout_by_method": "cashout_by_method",   # 各国家打钱统计(按打钱方式)
    "cashout_by_created_at": "cashout_by_created_at",   # 各国家打钱统计(按打钱创建时间)
    "cashout_by_money": "cashout_by_money",   # 各国家打钱统计(按打钱金额)
    # "video_ctr_recall": "video_ctr_recall",  # 召回实验的视频ctr
    # "video_ctr_dimension_recall": "video_ctr_dimension_recall",  # 召回实验的视频ctr(按维度分组)
    # "video_ctr_by_dimension_recall": "video_ctr_by_dimension_recall",  # 召回实验的视频ctr(按实验维度分组)
    # "video_ctr_recall_data_index0": "video_ctr_recall_data_index0",  # 召回实验的视频ctr(data_index=0)
    # "video_ctr_people_recall": "video_ctr_people_recall",  # 召回实验的视频ctr(人)
    # "video_ctr_people_dimension_recall": "video_ctr_people_dimension_recall",  # 召回实验的视频ctr(人)
    # "video_ctr_people_by_dimension_recall": "video_ctr_people_by_dimension_recall",  # 召回实验的视频ctr(人，按实验维度分组)
    # "video_ctr_people_recall_data_index0": "video_ctr_people_recall_data_index0",  # 召回实验的视频ctr(人)(data_index=0)
    # "video_ctr_rank": "video_ctr_rank",  # Rank实验的视频ctr
    # "video_ctr_people_rank": "video_ctr_people_rank",  # Rank实验的视频ctr(人)
    # "video_watch_average_of_duration_recall": "video_watch_average_of_duration_recall", # 召回实验下所有用户的平均观看时长
    # "video_watch_average_of_duration_by_dimension_recall": "video_watch_average_of_duration_by_dimension_recall", # 召回实验下所有用户的平均观看时长(按实验维度分组)
    # "video_watch_average_of_duration_rank": "video_watch_average_of_duration_rank", # Rank实验下所有用户的平均观看时长
    # "immersive_retention_recall": "immersive_retention_recall",    # 沉浸流召回实验留存
    # "immersive_retention_by_dimension_recall": "immersive_retention_by_dimension_recall",    # 沉浸流召回实验留存(按实验维度分组)
    # "immersive_retention_rank": "immersive_retention_rank",    # 沉浸流Rank实验留存
    # "video_ctr_with_device_model_recall": "video_ctr_with_device_model_recall",  # 召回实验视频ctr（按机型）
    # "video_ctr_with_device_model_rank": "video_ctr_with_device_model_rank",  # 召回实验视频ctr（按机型）
    # "video_ctr_with_brand_recall": "video_ctr_with_brand_recall",  # 召回实验视频ctr（按品牌）
    # "video_ctr_with_brand_rank": "video_ctr_with_brand_rank",  # Rank实验视频ctr（按品牌）
    # "video_watch_average_by_dimension_recall": "video_watch_average_by_dimension_recall",  # 召回实验平均观看次数(按实验维度分组)
    # "video_watch_average_rank": "video_watch_average_rank",  # Rank实验平均观看次数(按实验策略分组)
    "posts": "posts",   # 发帖数量统计
    "posts_user_count": "posts_user_count",     # 发帖人数统计
    # "video_retention_recall": "video_retention_recall",     # 召回实验视频留存(按实验策略分组)
    # "video_retention_rank": "video_retention_rank",     # Rank实验视频留存(按实验策略分组)
    # "video_average_of_total_duration_recall": "video_average_of_total_duration_recall",     # 召回实验用户平均总时长
    # "video_average_of_total_duration_rank": "video_average_of_total_duration_rank",     # Rank实验用户平均总时长
    # "video_average_of_total_duration_by_dimension_recall": "video_average_of_total_duration_by_dimension_recall",       # 召回实验用户平均总时长(按实验维度分组)
    "video_ctr_without_experiments": "video_ctr_without_experiments",     # 视频ctr(按次数，不带实验)
    "video_ctr_people_without_experiments": "video_ctr_people_without_experiments",     # 视频ctr(按人数，不带实验)
    "video_watch_average_of_duration_without_experiments": "video_watch_average_of_duration_without_experiments",       # 用户平均观看时长(不带实验)
    "user_total_duration_average": "user_total_duration_average",   # 各国家用户平均使用app时间
    "user_avg_cost": "user_avg_cost",   # 用户平均成本
    "short_video_ctr": "short_video_ctr",   # 3分钟以下短视频点击率
    "short_new_video_ctr": "short_new_video_ctr",   # 3分钟以下短视频点击率(7天内的视频)
    "short_video_completion_rate": "short_video_completion_rate",   # 3分钟以下短视频完播率
    "short_new_video_completion_rate": "short_new_video_completion_rate",   # 3分钟以下短视频完播率(7天内的视频)
    "one_minute_video_ctr": "one_minute_video_ctr",   # 1分钟以下短视频点击率
    "one_minute_new_video_ctr": "one_minute_new_video_ctr",   # 1分钟以下短视频点击率(7天内的视频)
    "one_minute_video_completion_rate": "one_minute_video_completion_rate",   # 1分钟以下短视频完播率
    "one_minute_new_video_completion_rate": "one_minute_new_video_completion_rate",   # 1分钟以下短视频完播率(7天内的视频)
    "five_minutes_video_ctr": "five_minutes_video_ctr",   # 5分钟以下短视频点击率
    "five_minutes_new_video_ctr": "five_minutes_new_video_ctr",   # 5分钟以下短视频点击率(7天内的视频)
    "five_minutes_video_completion_rate": "five_minutes_video_completion_rate",   # 5分钟以下短视频完播率
    "five_minutes_new_video_completion_rate": "five_minutes_new_video_completion_rate",   # 5分钟以下短视频完播率(7天内的视频)
    "push_ctr_without_experiments": "push_ctr_without_experiments",     # 推送的ctr(次数)
    "push_ctr_people_without_experiments": "push_ctr_people_without_experiments",     # 推送的ctr(人数)
    "notification_video_ctr_without_experiments": "notification_video_ctr_without_experiments",    # 视频推送的ctr(次数)
    "notification_video_ctr_without_experiments_by_people": "notification_video_ctr_without_experiments_by_people",    # 视频推送的ctr(人数)
    "notification_news_ctr_without_experiments": "notification_news_ctr_without_experiments",    # 新闻推送的ctr(次数)
    "notification_news_ctr_without_experiments_by_people": "notification_news_ctr_without_experiments_by_people",    # 新闻推送的ctr(人数)
    "new_videos_ctr": "new_videos_ctr",    # 新视频的ctr(2天内的视频)
    # "immersive_video_watch_average_recall_by_model": "immersive_video_watch_average_recall_by_model",      # 召回实验平均观看次数(按模型统计)
    # "immersive_video_watch_average_of_duration_recall_by_model": "immersive_video_watch_average_of_duration_recall_by_model",      # 召回实验平均观看时长(按模型统计)
    # "immersive_retention_recall_by_model": "immersive_retention_recall_by_model",   # 召回实验留存(按模型统计)
    "immersive_video_watch_average_recall_by_bucket": "immersive_video_watch_average_recall_by_bucket",      # 召回实验平均观看次数(按桶统计)
    "immersive_video_watch_average_of_duration_recall_by_bucket": "immersive_video_watch_average_of_duration_recall_by_bucket",      # 召回实验平均观看时长(按桶统计)
    "immersive_retention_recall_by_bucket": "immersive_retention_recall_by_bucket",   # 召回实验留存(按桶统计)
    "immersive_video_watch_average_rough_rank_by_model": "immersive_video_watch_average_rough_rank_by_model",      # 粗排实验平均观看次数(按模型统计)
    "immersive_video_watch_average_of_duration_rough_rank_by_model": "immersive_video_watch_average_of_duration_rough_rank_by_model",      # 粗排实验平均观看时长(按模型统计)
    "immersive_retention_rough_rank_by_model": "immersive_retention_rough_rank_by_model",   # 粗排实验留存(按模型统计)
    # "video_ctr_recall_by_model": "video_ctr_recall_by_model",     # 召回实验视频次数ctr(按模型统计)
    # "video_ctr_people_recall_by_model": "video_ctr_people_recall_by_model",     # 召回实验视频人数ctr(按模型统计)
    "video_ctr_recall_by_bucket": "video_ctr_recall_by_bucket",     # 召回实验视频次数ctr(按桶统计)
    "video_ctr_people_recall_by_bucket": "video_ctr_people_recall_by_bucket",     # 召回实验视频人数ctr(按桶统计)
    "video_ctr_rough_rank_by_model": "video_ctr_rough_rank_by_model",     # 粗排实验视频次数ctr(按模型统计)
    "video_ctr_people_rough_rank_by_model": "video_ctr_people_rough_rank_by_model",     # 粗排实验视频人数ctr(按模型统计)
    # "video_click_average_recall_by_model": "video_click_average_recall_by_model",       # 召回实验视频平均点击次数(按模型统计)
    "video_click_average_recall_by_bucket": "video_click_average_recall_by_bucket",      # 召回实验视频平均点击次数(按桶统计)
    "video_click_average_rough_rank_by_model": "video_click_average_rough_rank_by_model",       # 粗排实验视频平均点击次数(按模型统计)
    # "video_retention_recall_by_model": "video_retention_recall_by_model",   # 召回实验视频留存(按模型统计)
    "video_retention_recall_by_bucket": "video_retention_recall_by_bucket",   # 召回实验视频留存(按桶统计)
    "video_retention_rough_rank_by_model": "video_retention_rough_rank_by_model",   # 粗排实验视频留存(按模型统计)
    # "thirty_seconds_new_user_app_open_retention": "thirty_seconds_new_user_app_open_retention",   # 使用时长30秒到3分钟的新用户app_open留存
    # "three_minutes_new_user_app_open_retention": "three_minutes_new_user_app_open_retention",   # 使用时长3到5分钟的新用户app_open留存
    # "five_minutes_new_user_app_open_retention": "five_minutes_new_user_app_open_retention",   # 使用时长5到10分钟的新用户app_open留存
    # "ten_minutes_new_user_app_open_retention": "ten_minutes_new_user_app_open_retention",   # 使用时长10分钟以上的新用户app_open留存
    "points_out_statistics": "points_out_statistics",   # 积分支出统计
    "points_out_purpose_statistics": "points_out_purpose_statistics",   # 积分支出统计(按gift下的活动统计)
    "points_in_statistics": "points_in_statistics",   # 积分收入统计
    "new_user_invite_master": "new_user_invite_master",     # new_user_inivte实验中每日进入实验的师傅的人数
    "new_user_invite_master_with_referrals": "new_user_invite_master_with_referrals",     # new_user_inivte实验中每日进入实验且有邀请行为的师傅的人数
    "new_user_invite_apprentice_retention": "new_user_invite_apprentice_retention",     # new_user_inivte实验中徒弟的留存
    "new_user_retention_by_brand": "new_user_retention_by_brand",     # 新用户留存(按手机品牌)
    "news_read_duration_avg": "news_read_duration_avg",     # 用户读新闻平均时长
    "news_read_duration_avg_by_count": "news_read_duration_avg_by_count",     # 用户读一个新闻平均时长
    "video_watch_duration_avg": "video_watch_duration_avg",     # 用户沉浸流看视频平均时长
    "video_watch_duration_avg_by_count": "video_watch_duration_avg_by_count",     # 用户沉浸流看一个视频平均时长
    "video_impression_duration_avg": "video_impression_duration_avg",     # 用户沉浸外看视频流外平均时长
    "job_query_log_cost": "job_query_log_cost",     # BigQuery费用明细
    "page_dau_and_penetration": "page_dau_and_penetration",     # 各页面DAU及渗透率
    "page_avg_time": "page_avg_time",     # 各页面平均时长
    "page_retention": "page_retention",     # 各页面留存
    "image_ctr_by_placement": "image_ctr_by_placement",     # 各位置照片ctr
    "video_ctr_by_placement": "video_ctr_by_placement",     # 各位置视频ctr
    "silient_user": "silient_user",     # 沉默用户分级
    "accounts_without_ad_click": "accounts_without_ad_click",       # 日活用户广告点击统计
    "user_cash_value": "user_cash_value",       # 用户广告价值
}

class AutoSyncMainDay:
    def __init__(self, logger):
        self.country_code = "'" + "','".join(constants.COUNTRY_CODE) + "'"
        self.placement = "'" + "','".join(constants.NEWS_PLACEMENT) + "'"
        self.video_placement = "'" + "','".join(constants.VIDEO_PLACEMENT) + "'"
        self.video_kind_placement = "'" + "','".join(constants.VIDEO_KIND_PLACEMENT) + "'"
        self.indicator_dimension = "'" + "','".join(constants.INDICATOR_DIMENSION) + "'"
        self.recall_experiment = "'" + "','".join(constants.RECALL_EXPERIMENT) + "'"
        self.rank_experiment = "'" + "','".join(constants.RANKING_EXPERIMENT) + "'"
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
                new_user_news_click_average.NewUserNewsClickAverageData(start_time, end_time, self.country_code, self.placement, self.indicator_dimension, value, logger).compute_data()

            elif key == "new_user_video_watch_average":
                new_user_video_watch_average.NewUserVideoWatchAverageData(start_time, end_time, self.country_code, self.video_placement, self.indicator_dimension, value, self.logger).compute_data()
                
            elif key == "old_user_video_watch_average":
                old_user_video_watch_average.OldUserVideoWatchAverageData(start_time, end_time, self.country_code, self.video_placement, self.indicator_dimension, value, self.logger).compute_data()
                
            elif key == "video_watch_average":
                video_watch_average.VideoWatchAverageData(start_time, end_time, self.country_code, self.video_placement, self.indicator_dimension, value, self.logger).compute_data()
                
            elif key == "new_users_retention_news_event":
                new_users_retention_news_event.NewUsersRetentionNewsEvent(start_time, end_time, self.country_code, self.indicator_dimension, value, self.logger).compute_data()
                
            elif key == "video_ctr":
                video_ctr.VideoCTRData(start_time, end_time, self.country_code, self.video_placement, self.indicator_dimension, value, self.logger).compute_data()
                
            elif key == "video_ctr_people":
                video_ctr_people.VideoCTRPeopleData(start_time, end_time, self.country_code, self.video_placement, self.indicator_dimension, value, self.logger).compute_data()
                
            elif key == "new_users_retention_tab_impression":
                new_users_retention_tab_impression.NewUsersRetentionTabImpression(start_time, end_time, value, self.logger).compute_data()
                
            elif key == "experiment_new_users_retention_tab_impression":
                experiment_new_users_retention_tab_impression.ExperimentNewUsersRetentionTabImpression(start_time, end_time, self.indicator_dimension, value, self.logger).compute_data()
                
            elif key == "new_users_events_retention":
                new_users_events_retention.NewUsersEventsRetention(start_time, end_time, value, self.logger).compute_data()
                
            elif key == "old_users_events_retention":
                old_users_events_retention.OldUsersEventsRetention(start_time, end_time, value, self.logger).compute_data()
                
            elif key == "partiko_memories_new_users_events_retention":
                partiko_memories_new_users_events_retention.PartikoMemoriesNewUsersEventsRetention(start_time, end_time, self.indicator_dimension, value, self.logger).compute_data()
                
            elif key == "partiko_memories_new_users_events_retention_with_impression":
                partiko_memories_new_users_events_retention_with_impression.PartikoMemoriesNewUsersEventsRetentionWithImpression(start_time, end_time, self.country_code, self.indicator_dimension, value, self.logger).compute_data("{}/SQL/{}.sql".format(DIR, value))
                
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
                partiko_memories_new_user_user_time_average_of_duration.PartikoMemoriesNewUserUserTimeAverageOfDuration(start_time, end_time, self.indicator_dimension, value, logger).compute_data("{}/SQL/{}.sql".format(DIR, value))

            elif key == "experiment_immersive_page_duration_avg":
                experiment_immersive_page_duration_avg.ExperimentImmersivePageDurationAvg(start_time, end_time, self.country_code, value, logger).compute_data("{}/SQL/{}.sql".format(DIR, value))

            elif key == "new_user_news_ctr_people":
                new_user_news_ctr_people.NewUserCTRPeopleData(start_time, end_time, self.country_code, self.placement, self.indicator_dimension, value, self.logger).compute_data()
                
            elif key == "new_user_video_ctr_people":
                new_user_video_ctr_people.NewUserVideoCTRPeopleData(start_time, end_time, self.country_code, self.video_placement, self.indicator_dimension, value, self.logger).compute_data()
                
            # elif key == "partiko_experiment_new_users_retention_tab_impression":
            #     partiko_experiment_new_users_retention_tab_impression.PartikoExperimentNewUsersRetentionTabImpression(start_time, end_time, self.indicator_dimension, value, self.logger).compute_data()

            elif key == "cash_out":
                cash_out.CashOut(start_time, end_time, value, logger).compute_data("{}/SQL/{}.sql".format(DIR, value))

            elif key == "cashout_by_method":
                cashout_by_method.CashOutByMethod(start_time, end_time, value, logger).compute_data("{}/SQL/{}.sql".format(DIR, value))

            elif key == "cashout_by_created_at":
                cashout_by_created_at.CashOutByCreatedAt(start_time, end_time, value, logger).compute_data("{}/SQL/{}.sql".format(DIR, value))

            elif key == "cashout_by_money":
                cashout_by_money.CashOutByMoney(start_time, end_time, value, logger).compute_data("{}/SQL/{}.sql".format(DIR, value))

            elif key == "video_ctr_recall":
                video_ctr_recall.VideoCtrRecall(start_time, end_time, self.country_code, self.recall_experiment, value, logger).compute_data("{}/SQL/{}.sql".format(DIR, value))

            elif key == "video_ctr_dimension_recall":
                video_ctr_dimension_recall.VideoCtrDimensionRecall(start_time, end_time, self.country_code, self.recall_experiment, value, logger).compute_data("{}/SQL/{}.sql".format(DIR, value))

            elif key == "video_ctr_by_dimension_recall":
                video_ctr_by_dimension_recall.VideoCtrByDimensionRecall(start_time, end_time, self.country_code, self.recall_experiment, value, logger).compute_data("{}/SQL/{}.sql".format(DIR, value))

            elif key == "video_ctr_recall_data_index0":
                video_ctr_recall_data_index0.VideoCtrRecallDataIndex0(start_time, end_time, self.country_code, self.recall_experiment, value, logger).compute_data("{}/SQL/{}.sql".format(DIR, value))
            
            elif key == "video_ctr_people_recall":
                video_ctr_people_recall.VideoCtrPeopleRecall(start_time, end_time, self.country_code, self.recall_experiment, value, logger).compute_data("{}/SQL/{}.sql".format(DIR, value))

            elif key == "video_ctr_people_dimension_recall":
                video_ctr_people_dimension_recall.VideoCtrPeopleDimensionRecall(start_time, end_time, self.country_code, self.recall_experiment, value, logger).compute_data("{}/SQL/{}.sql".format(DIR, value))

            elif key == "video_ctr_people_by_dimension_recall":
                video_ctr_people_by_dimension_recall.VideoCtrPeopleByDimensionRecall(start_time, end_time, self.country_code, self.recall_experiment, value, logger).compute_data("{}/SQL/{}.sql".format(DIR, value))

            elif key == "video_ctr_people_recall_data_index0":
                video_ctr_people_recall_data_index0.VideoCtrPeopleRecallDataIndex0(start_time, end_time, self.country_code, self.recall_experiment, value, logger).compute_data("{}/SQL/{}.sql".format(DIR, value))

            elif key == "video_ctr_rank":
                video_ctr_rank.VideoCtrRank(start_time, end_time, self.country_code, self.rank_experiment, value, logger).compute_data("{}/SQL/{}.sql".format(DIR, value))

            elif key == "video_ctr_people_rank":
                video_ctr_people_rank.VideoCtrPeopleRank(start_time, end_time, self.country_code, self.rank_experiment, value, logger).compute_data("{}/SQL/{}.sql".format(DIR, value))

            elif key == "video_watch_average_of_duration_recall":
                video_watch_average_of_duration_recall.VideoWatchAverageOfDurationRecall(start_time, end_time, self.country_code, self.recall_experiment, value, logger).compute_data("{}/SQL/{}.sql".format(DIR, value))

            elif key == "video_watch_average_of_duration_by_dimension_recall":
                video_watch_average_of_duration_by_dimension_recall.VideoWatchAverageOfDurationByDimensionRecall(start_time, end_time, self.country_code, self.recall_experiment, value, logger).compute_data("{}/SQL/{}.sql".format(DIR, value))

            elif key == "video_watch_average_of_duration_rank":
                video_watch_average_of_duration_rank.VideoWatchAverageOfDurationRank(start_time, end_time, self.country_code, self.rank_experiment, value, logger).compute_data("{}/SQL/{}.sql".format(DIR, value))

            elif key == "immersive_retention_recall":
                immersive_retention_recall.ImmersiveRetentionRecall(start_time, end_time, self.recall_experiment, value, logger).compute_data("{}/SQL/{}.sql".format(DIR, value))

            elif key == "immersive_retention_by_dimension_recall":
                immersive_retention_by_dimension_recall.ImmersiveRetentionByDimensionRecall(start_time, end_time, self.recall_experiment, value, logger).compute_data("{}/SQL/{}.sql".format(DIR, value))
            
            elif key == "immersive_retention_rank":
                immersive_retention_rank.ImmersiveRetentionRank(start_time, end_time, self.rank_experiment, value, logger).compute_data("{}/SQL/{}.sql".format(DIR, value))

            elif key == "video_ctr_with_device_model_recall":
                video_ctr_with_device_model_recall.VideoCtrWithDeviceModelRecall(start_time, end_time, self.country_code, self.recall_experiment, value, logger).compute_data("{}/SQL/{}.sql".format(DIR, value))

            elif key == "video_ctr_with_device_model_rank":
                video_ctr_with_device_model_rank.VideoCtrWithDeviceModelRank(start_time, end_time, self.country_code, self.rank_experiment, value, logger).compute_data("{}/SQL/{}.sql".format(DIR, value))

            elif key == "video_ctr_with_brand_recall":
                video_ctr_with_brand_recall.VideoCtrWithBrandRecall(start_time, end_time, self.country_code, self.recall_experiment, value, logger).compute_data("{}/SQL/{}.sql".format(DIR, value))

            elif key == "video_ctr_with_brand_rank":
                video_ctr_with_brand_rank.VideoCtrWithBrandRank(start_time, end_time, self.country_code, self.rank_experiment, value, logger).compute_data("{}/SQL/{}.sql".format(DIR, value))

            elif key == "video_watch_average_by_dimension_recall":
                video_watch_average_by_dimension_recall.VideoWatchAverageByDimensionRecall(start_time, end_time, self.country_code, self.recall_experiment, value, logger).compute_data("{}/SQL/{}.sql".format(DIR, value))

            elif key == "video_watch_average_rank":
                video_watch_average_rank.VideoWatchAverageRank(start_time, end_time, self.country_code, self.rank_experiment, value, logger).compute_data("{}/SQL/{}.sql".format(DIR, value))

            elif key == "posts":
                posts.Posts(start_time, end_time, self.country_code, value, logger).compute_data("{}/SQL/{}.sql".format(DIR, value))

            elif key == "posts_user_count":
                posts_user_count.PostsUserCount(start_time, end_time, self.country_code, value, logger).compute_data("{}/SQL/{}.sql".format(DIR, value))

            elif key == "video_retention_recall":
                video_retention_recall.VideoRetentionRecall(start_time, end_time, self.country_code, self.recall_experiment, value, logger).compute_data("{}/SQL/{}.sql".format(DIR, value))

            elif key == "video_retention_rank":
                video_retention_rank.VideoRetentionRank(start_time, end_time, self.country_code, self.rank_experiment, value, logger).compute_data("{}/SQL/{}.sql".format(DIR, value))

            elif key == "video_average_of_total_duration_recall":
                video_average_of_total_duration_recall.VideoAverageOfTotalDurationRecall(start_time, end_time, self.country_code, self.recall_experiment, value, logger).compute_data("{}/SQL/{}.sql".format(DIR, value))

            elif key == "video_average_of_total_duration_rank":
                video_average_of_total_duration_rank.VideoAverageOfTotalDurationRank(start_time, end_time, self.country_code, self.rank_experiment, value, logger).compute_data("{}/SQL/{}.sql".format(DIR, value))

            elif key == "video_average_of_total_duration_by_dimension_recall":
                video_average_of_total_duration_by_dimension_recall.VideoAverageOfTotalDurationByDimensionRecall(start_time, end_time, self.country_code, self.recall_experiment, value, logger).compute_data("{}/SQL/{}.sql".format(DIR, value))

            elif key == "video_ctr_without_experiments":
                video_ctr_without_experiments.VideoCtrWithoutExperiments(start_time, end_time, self.country_code, value, logger).compute_data("{}/SQL/{}.sql".format(DIR, value))
            
            elif key == "video_ctr_people_without_experiments":
                video_ctr_people_without_experiments.VideoCtrPeopleWithoutExperiments(start_time, end_time, self.country_code, value, logger).compute_data("{}/SQL/{}.sql".format(DIR, value))
            
            elif key == "video_watch_average_of_duration_without_experiments":
                video_watch_average_of_duration_without_experiments.VideoWatchAverageOfDurationWithoutExperiments(start_time, end_time, self.country_code, value, logger).compute_data("{}/SQL/{}.sql".format(DIR, value))

            elif key == "user_total_duration_average":
                user_total_duration_average.UserTotalDurationAverage(start_time, end_time, self.country_code, value, logger).compute_data("{}/SQL/{}.sql".format(DIR, value))

            elif key == "user_avg_cost":
                user_avg_cost.UserAvgCostOut(start_time, value, logger).compute_data("{}/SQL/{}.sql".format(DIR, value))

            elif key == "short_video_ctr":
                short_video_ctr.ShortVideoCtr(start_time, end_time, self.country_code, value, logger).compute_data("{}/SQL/{}.sql".format(DIR, value))

            elif key == "short_new_video_ctr":
                short_new_video_ctr.ShortNewVideoCtr(start_time, end_time, self.country_code, value, logger).compute_data("{}/SQL/{}.sql".format(DIR, value))

            elif key == "short_video_completion_rate":
                short_video_completion_rate.ShortVideoCompletionRate(start_time, end_time, self.country_code, value, logger).compute_data("{}/SQL/{}.sql".format(DIR, value))

            elif key == "short_new_video_completion_rate":
                short_new_video_completion_rate.ShortNewVideoCompletionRate(start_time, end_time, self.country_code, value, logger).compute_data("{}/SQL/{}.sql".format(DIR, value))

            elif key == "one_minute_video_ctr":
                one_minute_video_ctr.OneMinuteVideoCtr(start_time, end_time, self.country_code, value, logger).compute_data("{}/SQL/{}.sql".format(DIR, value))

            elif key == "one_minute_new_video_ctr":
                one_minute_new_video_ctr.OneMinuteNewVideoCtr(start_time, end_time, self.country_code, value, logger).compute_data("{}/SQL/{}.sql".format(DIR, value))

            elif key == "one_minute_video_completion_rate":
                one_minute_video_completion_rate.OneMinuteVideoCompletionRate(start_time, end_time, self.country_code, value, logger).compute_data("{}/SQL/{}.sql".format(DIR, value))

            elif key == "one_minute_new_video_completion_rate":
                one_minute_new_video_completion_rate.OneMinuteNewVideoCompletionRate(start_time, end_time, self.country_code, value, logger).compute_data("{}/SQL/{}.sql".format(DIR, value))

            elif key == "five_minutes_video_ctr":
                five_minutes_video_ctr.FiveMinutesVideoCtr(start_time, end_time, self.country_code, value, logger).compute_data("{}/SQL/{}.sql".format(DIR, value))

            elif key == "five_minutes_new_video_ctr":
                five_minutes_new_video_ctr.FiveMinutesNewVideoCtr(start_time, end_time, self.country_code, value, logger).compute_data("{}/SQL/{}.sql".format(DIR, value))

            elif key == "five_minutes_video_completion_rate":
                five_minutes_video_completion_rate.FiveMinutesVideoCompletionRate(start_time, end_time, self.country_code, value, logger).compute_data("{}/SQL/{}.sql".format(DIR, value))

            elif key == "five_minutes_new_video_completion_rate":
                five_minutes_new_video_completion_rate.FiveMinutesNewVideoCompletionRate(start_time, end_time, self.country_code, value, logger).compute_data("{}/SQL/{}.sql".format(DIR, value))

            elif key == "push_ctr_without_experiments":
                push_ctr_without_experiments.PushCtrWithoutExperiments(start_time, end_time, self.country_code, value, logger).compute_data("{}/SQL/{}.sql".format(DIR, value))

            elif key == "push_ctr_people_without_experiments":
                push_ctr_people_without_experiments.PushCtrPeopleWithoutExperiments(start_time, end_time, self.country_code, value, logger).compute_data("{}/SQL/{}.sql".format(DIR, value))

            elif key == "notification_video_ctr_without_experiments":
                notification_video_ctr_without_experiments.NotificationVideoCtrWithoutExperiments(start_time, end_time, self.country_code, value, logger).compute_data("{}/SQL/{}.sql".format(DIR, value))

            elif key == "notification_video_ctr_without_experiments_by_people":
                notification_video_ctr_without_experiments_by_people.NotificationVideoCtrWithoutExperimentsByPeople(start_time, end_time, self.country_code, value, logger).compute_data("{}/SQL/{}.sql".format(DIR, value))

            elif key == "notification_news_ctr_without_experiments":
                notification_news_ctr_without_experiments.NotificationNewsCtrWithoutExperiments(start_time, end_time, self.country_code, value, logger).compute_data("{}/SQL/{}.sql".format(DIR, value))

            elif key == "notification_news_ctr_without_experiments_by_people":
                notification_news_ctr_without_experiments_by_people.NotificationNewsCtrWithoutExperimentsByPeople(start_time, end_time, self.country_code, value, logger).compute_data("{}/SQL/{}.sql".format(DIR, value))

            elif key == "new_videos_ctr":
                new_videos_ctr.NewVideosCtr(start_time, end_time, self.country_code, value, logger).compute_data("{}/SQL/{}.sql".format(DIR, value))

            elif key == "immersive_video_watch_average_recall_by_model":    
                immersive_video_watch_average_recall_by_model.ImmersiveVideoWatchAverageRecallByModel(start_time, end_time, self.country_code, value, logger).compute_data("{}/SQL/{}.sql".format(DIR, value))

            elif key == "immersive_video_watch_average_of_duration_recall_by_model":    
                immersive_video_watch_average_of_duration_recall_by_model.ImmersiveVideoWatchAverageOfDurationRecallByModel(start_time, end_time, self.country_code, value, logger).compute_data("{}/SQL/{}.sql".format(DIR, value))

            elif key == "immersive_retention_recall_by_model":    
                immersive_retention_recall_by_model.ImmersiveRetentionRecallByModel(start_time, end_time, self.country_code, value, logger).compute_data("{}/SQL/{}.sql".format(DIR, value))

            elif key == "immersive_video_watch_average_recall_by_bucket":    
                immersive_video_watch_average_recall_by_bucket.ImmersiveVideoWatchAverageRecallByBucket(start_time, end_time, self.country_code, value, logger).compute_data("{}/SQL/{}.sql".format(DIR, value))

            elif key == "immersive_video_watch_average_of_duration_recall_by_bucket":    
                immersive_video_watch_average_of_duration_recall_by_bucket.ImmersiveVideoWatchAverageOfDurationRecallByBucket(start_time, end_time, self.country_code, value, logger).compute_data("{}/SQL/{}.sql".format(DIR, value))

            elif key == "immersive_retention_recall_by_bucket":    
                immersive_retention_recall_by_bucket.ImmersiveRetentionRecallByBucket(start_time, end_time, self.country_code, value, logger).compute_data("{}/SQL/{}.sql".format(DIR, value))

            elif key == "immersive_video_watch_average_rough_rank_by_model":    
                immersive_video_watch_average_rough_rank_by_model.ImmersiveVideoWatchAverageRoughRankByModel(start_time, end_time, self.country_code, value, logger).compute_data("{}/SQL/{}.sql".format(DIR, value))

            elif key == "immersive_video_watch_average_of_duration_rough_rank_by_model":    
                immersive_video_watch_average_of_duration_rough_rank_by_model.ImmersiveVideoWatchAverageOfDurationRoughRankByModel(start_time, end_time, self.country_code, value, logger).compute_data("{}/SQL/{}.sql".format(DIR, value))

            elif key == "immersive_retention_rough_rank_by_model":    
                immersive_retention_rough_rank_by_model.ImmersiveRetentionRoughRankByModel(start_time, end_time, self.country_code, value, logger).compute_data("{}/SQL/{}.sql".format(DIR, value))

            elif key == "video_ctr_recall_by_model": 
                video_ctr_recall_by_model.VideoCtrRecallByModel(start_time, end_time, self.country_code, value, logger).compute_data("{}/SQL/{}.sql".format(DIR, value))

            elif key == "video_ctr_people_recall_by_model": 
                video_ctr_people_recall_by_model.VideoCtrPeopleRecallByModel(start_time, end_time, self.country_code, value, logger).compute_data("{}/SQL/{}.sql".format(DIR, value))

            elif key == "video_ctr_recall_by_bucket": 
                video_ctr_recall_by_bucket.VideoCtrRecallByBucket(start_time, end_time, self.country_code, value, logger).compute_data("{}/SQL/{}.sql".format(DIR, value))

            elif key == "video_ctr_people_recall_by_bucket": 
                video_ctr_people_recall_by_bucket.VideoCtrPeopleRecallByBucket(start_time, end_time, self.country_code, value, logger).compute_data("{}/SQL/{}.sql".format(DIR, value))

            elif key == "video_ctr_rough_rank_by_model": 
                video_ctr_rough_rank_by_model.VideoCtrRoughRankByModel(start_time, end_time, self.country_code, value, logger).compute_data("{}/SQL/{}.sql".format(DIR, value))

            elif key == "video_ctr_people_rough_rank_by_model": 
                video_ctr_people_rough_rank_by_model.VideoCtrPeopleRoughRankByModel(start_time, end_time, self.country_code, value, logger).compute_data("{}/SQL/{}.sql".format(DIR, value))

            elif key == "video_click_average_recall_by_model":
                video_click_average_recall_by_model.VideoClickAverageRecallByModel(start_time, end_time, self.country_code, value, logger).compute_data("{}/SQL/{}.sql".format(DIR, value))

            elif key == "video_click_average_recall_by_bucket":
                video_click_average_recall_by_bucket.VideoClickAverageRecallByBucket(start_time, end_time, self.country_code, value, logger).compute_data("{}/SQL/{}.sql".format(DIR, value))

            elif key == "video_click_average_rough_rank_by_model":
                video_click_average_rough_rank_by_model.VideoClickAverageRoughRankByModel(start_time, end_time, self.country_code, value, logger).compute_data("{}/SQL/{}.sql".format(DIR, value))

            elif key == "video_retention_recall_by_model":
                video_retention_recall_by_model.VideoRetentionRecallByModel(start_time, end_time, self.country_code, value, logger).compute_data("{}/SQL/{}.sql".format(DIR, value))

            elif key == "video_retention_recall_by_bucket":
                video_retention_recall_by_bucket.VideoRetentionRecallByBucket(start_time, end_time, self.country_code, value, logger).compute_data("{}/SQL/{}.sql".format(DIR, value))

            elif key == "video_retention_rough_rank_by_model":
                video_retention_rough_rank_by_model.VideoRetentionRoughRankByModel(start_time, end_time, self.country_code, value, logger).compute_data("{}/SQL/{}.sql".format(DIR, value))

            elif key == "thiry_seconds_new_user_app_open_retention":
                thiry_seconds_new_user_app_open_rteention.ThirtySecondsNewUserAppOpenRetention(start_time, end_time, self.country_code, value, logger).compute_data("{}/SQL/{}.sql".format(DIR, value))

            elif key == "three_minutes_new_user_app_open_retention":
                three_minutes_new_user_app_open_retention.ThreeMinutesNewUserAppOpenRetention(start_time, end_time, self.country_code, value, logger).compute_data("{}/SQL/{}.sql".format(DIR, value))

            elif key == "five_minutes_new_user_app_open_retention":
                five_minutes_new_user_app_open_retention.FiveMinutesNewUserAppOpenRetention(start_time, end_time, self.country_code, value, logger).compute_data("{}/SQL/{}.sql".format(DIR, value))

            elif key == "ten_minutes_new_user_app_open_retention":
                ten_minutes_new_user_app_open_retention.TenMinutesNewUserAppOpenRetention(start_time, end_time, self.country_code, value, logger).compute_data("{}/SQL/{}.sql".format(DIR, value))

            elif key == "points_out_statistics":
                points_out_statistics.PointsOutStatistics(start_time, end_time, self.country_code, value, logger).compute_data("{}/SQL/{}.sql".format(DIR, value))

            elif key == "points_out_purpose_statistics":
                points_out_purpose_statistics.PointsOutPurposeStatistics(start_time, end_time, self.country_code, value, logger).compute_data("{}/SQL/{}.sql".format(DIR, value))

            elif key == "points_in_statistics":
                points_in_statistics.PointsInStatistics(start_time, end_time, self.country_code, value, logger).compute_data("{}/SQL/{}.sql".format(DIR, value))

            elif key == "new_user_invite_master":
                new_user_invite_master.NewUserInviteMaster(start_time, end_time, self.country_code, value, logger).compute_data("{}/SQL/{}.sql".format(DIR, value))

            elif key == "new_user_invite_master_with_referrals":
                new_user_invite_master_with_referrals.NewUserInviteMasterWithReferrals(start_time, end_time, self.country_code, value, logger).compute_data("{}/SQL/{}.sql".format(DIR, value))

            elif key == "new_user_invite_apprentice_retention":
                new_user_invite_apprentice_retention.NewUserInviteApprenticeRetention(start_time, end_time, self.country_code, value, logger).compute_data("{}/SQL/{}.sql".format(DIR, value))

            elif key == "new_user_retention_by_brand":
                new_user_retention_by_brand.NewUserRetentionByBrand(start_time, end_time, self.country_code, value, logger).compute_data("{}/SQL/{}.sql".format(DIR, value))

            elif key == "news_read_duration_avg":
                news_read_duration_avg.NewsReadDurationAvg(start_time, end_time, self.country_code, value, logger).compute_data("{}/SQL/{}.sql".format(DIR, value))

            elif key == "news_read_duration_avg_by_count":
                news_read_duration_avg_by_count.NewsReadDurationAvgByCount(start_time, end_time, self.country_code, value, logger).compute_data("{}/SQL/{}.sql".format(DIR, value))

            elif key == "video_watch_duration_avg":
                video_watch_duration_avg.VideoWatchDurationAvg(start_time, end_time, self.country_code, value, logger).compute_data("{}/SQL/{}.sql".format(DIR, value))

            elif key == "video_watch_duration_avg_by_count":
                video_watch_duration_avg_by_count.VideoWatchDurationAvgByCount(start_time, end_time, self.country_code, value, logger).compute_data("{}/SQL/{}.sql".format(DIR, value))

            elif key == "video_impression_duration_avg":
                video_impression_duration_avg.VideoImpressionDurationAvg(start_time, end_time, self.country_code, value, logger).compute_data("{}/SQL/{}.sql".format(DIR, value))

            elif key == "job_query_log_cost":
                job_query_log_cost.JobQueryLogCost(start_time, end_time, value, logger).compute_data("{}/SQL/{}.sql".format(DIR, value))

            elif key == "page_dau_and_penetration":
                page_dau_and_penetration.PageDauAndPenetration(start_time, end_time, self.country_code, value, logger).compute_data("{}/SQL/{}.sql".format(DIR, value))

            elif key == "page_avg_time":
                page_avg_time.PageAvgTime(start_time, end_time, self.country_code, value, logger).compute_data("{}/SQL/{}.sql".format(DIR, value))

            elif key == "page_retention":
                page_retention.PageRetention(start_time, end_time, self.country_code, value, logger).compute_data("{}/SQL/{}.sql".format(DIR, value))

            elif key == "image_ctr_by_placement":
                image_ctr_by_placement.ImageCtrByPlacement(start_time, end_time, self.country_code, value, logger).compute_data("{}/SQL/{}.sql".format(DIR, value))

            elif key == "video_ctr_by_placement":
                video_ctr_by_placement.VideoCtrByPlacement(start_time, end_time, self.country_code, value, logger).compute_data("{}/SQL/{}.sql".format(DIR, value))

            elif key == "silient_user":
                silient_user.SilentUser(start_time, end_time, self.country_code, value, logger).compute_data("{}/SQL/{}.sql".format(DIR, value))

            elif key == "accounts_without_ad_click":
                accounts_without_ad_click.AccountsWithoutAdClick(start_time, end_time, self.country_code, value, logger).compute_data("{}/SQL/{}.sql".format(DIR, value))

            elif key == "user_cash_value":
                user_cash_value.UserCashValue(start_time, end_time, value, logger).compute_data("{}/SQL/{}.sql".format(DIR, value))

            indicator_end_time = datetime.datetime.now()
            indicator_use_time = indicator_end_time - indicator_start_time
            self.logger.info("sync {} indicator end use {} seconds".format(key, indicator_use_time))

if __name__ == "__main__":
    logger = Logger("BuzzBreak Auto Sync Main Day", os.path.join(DIR, 'logs/auto_sync_main_day.log'), users=["teddy"])
    sync_tables = ["input.accounts", "partiko.memories", "partiko.account_profiles", "partiko.point_transactions", "partiko.withdraw_transactions", "partiko.referrals", "partiko.ads", "partiko.posts"]
    sync_tables_str = "'" + "', '".join(sync_tables) + "'"
    fields = ["table_name", "updated_at"]
    while True:
        buzzbreak_mysql_client = MySQL("MYSQL_BUZZBREAK")
        buzzbreak_mongo_client = Mongo("MONGO_ANALYTICS")
        sql_mysql = "select table_name, max(updated_at) FROM {} group by table_name".format("main_day_involed_bigquery_tables")
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
            buzzbreak_mysql_client.close_client()
            buzzbreak_mongo_client.close_client()
            if use_time.total_seconds() > 60*60:
                logger.alert("buzzbreak execute sync indicator scripts over one hour, please delay FinBI sync time")
            # sleep_time = getRestSeconds(datetime.datetime.utcnow()) + 60*60*1
            # time.sleep(sleep_time)
            sys.exit(0)
        elif condition == 2:
            buzzbreak_mysql_client.close_client()
            buzzbreak_mongo_client.close_client()
            logger.info("sync buzzbreak indicator scripts by day {} fail and stop due to date_diff = {}".format(start_time.strftime("%Y-%m-%d"), date_diff))
            logger.alert("sync buzzbreak indicator scripts by day {} fail and stop due to date_diff = {}".format(start_time.strftime("%Y-%m-%d"), date_diff))
            sys.exit(0)
        elif condition == 0:
            buzzbreak_mysql_client.close_client()
            buzzbreak_mongo_client.close_client()
            logger.info("mongo sync buzzbreak indicator scripts by day {} fail due to date_diff = {}, try again 30 minutes later".format(start_time.strftime("%Y-%m-%d"), date_diff))
            logger.alert("sync buzzbreak indicator scripts by day {} fail due to date_diff = {}, try again 30 minutes later".format(start_time.strftime("%Y-%m-%d"), date_diff))
            if now_time_utc.hour == 4:
                logger.info("mongo sync buzzbreak indicator scripts by day {} fail and stop due to date_diff = {}".format(start_time.strftime("%Y-%m-%d"), date_diff))
                logger.alert("sync buzzbreak indicator scripts by day {} fail and stop due to date_diff = {}".format(start_time.strftime("%Y-%m-%d"), date_diff))
                sys.exit(0)
            time.sleep(60*30)