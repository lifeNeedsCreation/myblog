import os
import sys
import getopt
import datetime

BASE_DIR = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.append(BASE_DIR)
sys.path.append(DIR)

from utils import constants
from utils.bigquery import buzzbreak_bigquery_client
from utils.mysql import buzzbreak_mysql_client
from utils.logger import Logger
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
from indicator_scripts import new_user_news_ctr_people
from indicator_scripts import new_user_video_ctr_people
from indicator_scripts import push_retention
# from indicator_scripts import partiko_experiment_new_users_retention_tab_impression
from indicator_scripts import new_video_click_ctr_by_type
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
from indicator_scripts import experiment_immersive_page_duration_avg
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


# 指标列表
KIND = {
    "all": 1,   # 所有指标
    "ctr": 1,   # 新闻ctr
    "ctr_people": 1,  # 新闻 click_user_ratio
    "news_ctr_notification_new_user": 1,  # 新用户新闻push的ctr
    "news_ctr_notification_new_user_people": 1,  # 新用户新闻push的ctr（人）
    "news_ctr_notification_old_user": 1,  # 老用户新闻push的ctr
    "news_ctr_notification_old_user_people": 1,  # 老用户新闻push的ctr（人）
    "video_ctr_notification_new_user": 1,   # 新用户视频push的ctr
    "video_ctr_notification_new_user_people": 1,   # 新用户视频push的ctr（人）
    "video_ctr_notification_old_user": 1,   # 老用户视频push的ctr
    "video_ctr_notification_old_user_people": 1,   # 老用户视频push的ctr（人）
    "new_user_news_click_average": 1,    # 新用户新闻平均点击率
    "new_user_video_watch_average": 1,      # 新用户视频平均观看次数
    "old_user_video_watch_average": 1,      # 老用户视频平均观看次数
    "video_watch_average": 1,      # 所有用户视频平均观看次数
    "new_users_retention_news_event": 1,   # 新闻用户留存率
    "video_ctr": 1,   # 视频ctr
    "video_ctr_people": 1,   # 视频 click_user_ratio
    "new_users_retention_tab_impression": 1,    # tab_impression 新用户留存
    "new_users_events_retention": 1,    # 新用户在app_open与tab_impression下的留存
    "old_users_events_retention": 1,    # app_open与tab_impression 老用户留存
    "experiment_new_users_retention_tab_impression": 1,     # tab_impression 实验中新用户留存
    "partiko_memories_new_users_events_retention": 1,       # app_open与tab_impression 实验中新用户留存
    "partiko_memories_new_users_events_retention_with_impression": 1,     # 实验中新用户留存(去掉没有新闻视频曝光的用户)
    "partiko_memories_old_users_events_retention": 1,       # app_open与tab_impression 实验中老用户留存
    "partiko_memories_average_of_invites": 1,     # partiko.memories 实验中的 平均邀请人数
    "new_users_partiko_memories_average_of_invites": 1,     # partiko.memories 实验中的 新用户平均邀请人数
    "partiko_experiment_average_of_invites": 1,     # partiko.experiment 实验中的 平均邀请人数
    "partiko_memories_user_time_average_of_duration": 1,     # partiko.memories 实验中 用户在各个页面的停留时间
    "partiko_memories_new_user_user_time_average_of_duration": 1, # partiko.memories 实验中新用户在各个页面的停留时间
    "experiment_immersive_page_duration_avg": 1,     # 沉浸流页面用户平局停留时长
    "new_user_news_ctr_people": 1,  # 新用户 新闻 click_user_ratio
    "new_user_video_ctr_people": 1,  # 新用户 视频 click_user_ratio
    "push_tention": 1,
    # "partiko_experiment_new_users_retention_tab_impression": 1,     # partiko.experiment 实验中 新用户在各个 tab 的留存
    "new_video_click_ctr_by_type": 1,   # 各类新视频点击率、点击数及曝光数
    "cash_out": 1, # 统计打钱，按国家和天
    "cashout_by_method": 1, # 各国家打钱统计(按打钱方式)
    "cashout_by_created_at": 1,   # 各国家打钱统计(按打钱创建时间)
    "cashout_by_money": 1,   # 各国家打钱统计(按打钱金额)
    "video_ctr_recall": 1,  # 召回实验的视频ctr
    "video_ctr_dimension_recall": 1,  # 召回实验的视频ctr(按维度分组)
    "video_ctr_by_dimension_recall": 1,  # 召回实验的视频ctr(按实验维度分组)
    "video_ctr_recall_data_index0": 1,  # 召回实验的视频ctr(data_index=0)
    "video_ctr_people_recall": 1,    # 召回实验的视频ctr(人)
    "video_ctr_people_dimension_recall": 1,    # 召回实验的视频ctr(人，按维度分组)
    "video_ctr_people_by_dimension_recall": 1,  # 召回实验的视频ctr(人，按实验维度分组)
    "video_ctr_people_recall_data_index0": 1,    # 召回实验的视频ctr(人)(data_index=0)
    "video_ctr_rank": 1,  # Rank实验的视频ctr
    "video_ctr_people_rank": 1,    # Rank实验的视频ctr(人)
    "video_watch_average_of_duration_recall": 1,    # 召回实验下所有用户的平均观看时长
    "video_watch_average_of_duration_by_dimension_recall": 1,   # 召回实验下所有用户的平均观看时长(按实验维度分组)
    "video_watch_average_of_duration_rank": 1,    # Rank实验下所有用户的平均观看时长
    "immersive_retention_recall": 1,    # 沉浸流召回实验留存
    "immersive_retention_by_dimension_recall": 1,   # 沉浸流召回实验留存(按实验维度分组)
    "immersive_retention_rank": 1,    # 沉浸流Rank实验留存
    "video_ctr_with_device_model_recall": 1,    # 召回实验视频ctr（按机型）
    "video_ctr_with_device_model_rank": 1,    # Rank实验视频ctr（按机型）
    "video_ctr_with_brand_recall": 1,    # 召回实验视频ctr（按品牌）
    "video_ctr_with_brand_rank": 1,    # Rank实验视频ctr（按品牌）
    "video_watch_average_by_dimension_recall": 1,   # 召回实验平均观看次数(按实验维度分组)
    "video_watch_average_rank": 1,   # Rank实验平均观看次数(按实验策略分组)
    "posts": 1,     # 发帖数量统计
    "posts_user_count": 1,     # 发帖人数统计
    "video_retention_recall": 1,    # 召回实验视频留存(按实验策略分组)
    "video_retention_rank": 1,      # Rank实验视频留存(按实验策略分组)
    "video_average_of_total_duration_recall": 1,    # 召回实验用户平均总时长
    "video_average_of_total_duration_rank": 1,    # Rank实验用户平均总时长
    "video_average_of_total_duration_by_dimension_recall": 1,   # 召回实验用户平均总时长(按实验维度分组)
    "video_ctr_without_experiments": 1,     # 视频ctr(按次数，不带实验)
    "video_ctr_people_without_experiments": 1,     # 视频ctr(按人数，不带实验)
    "video_watch_average_of_duration_without_experiments": 1,       # 用户平均观看时长(不带实验)
    "user_total_duration_average": 1,   # 各国家用户平均使用app时间
    "user_avg_cost": 1,     # 用户平均成本
    "short_video_ctr": 1,   # 3分钟以下短视频点击率
    "short_new_video_ctr": 1,   # 3分钟以下短视频点击率(7天内的视频)
    "short_video_completion_rate": 1,   # 3分钟以下短视频完播率
    "short_new_video_completion_rate": 1,   # 3分钟以下短视频完播率(7天内的视频)
    "one_minute_video_ctr": 1,   # 1分钟以下短视频点击率
    "one_minute_new_video_ctr": 1,   # 1分钟以下短视频点击率(7天内的视频)
    "one_minute_video_completion_rate": 1,   # 1分钟以下短视频完播率
    "one_minute_new_video_completion_rate": 1,   # 1分钟以下短视频完播率(7天内的视频)
    "five_minutes_video_ctr": 1,   # 5分钟以下短视频点击率
    "five_minutes_new_video_ctr": 1,   # 5分钟以下短视频点击率(7天内的视频)
    "five_minutes_video_completion_rate": 1,   # 5分钟以下短视频完播率
    "five_minutes_new_video_completion_rate": 1,   # 5分钟以下短视频完播率(7天内的视频)
    "push_ctr_without_experiments": 1,     # 推送的ctr(次数)
    "push_ctr_people_without_experiments": 1,     # 推送的ctr(人数)
    "notification_video_ctr_without_experiments": 1,    # 视频推送的ctr(次数)
    "notification_video_ctr_without_experiments_by_people": 1,    # 视频推送的ctr(人数)
    "notification_news_ctr_without_experiments": 1,    # 新闻推送的ctr(次数)
    "notification_news_ctr_without_experiments_by_people": 1,    # 新闻推送的ctr(人数)
    "new_videos_ctr": 1,    # 新视频ctr（2天内的视频）
    "immersive_video_watch_average_recall_by_model": 1,      # 召回实验平均观看次数(按模型统计)
    "immersive_video_watch_average_of_duration_recall_by_model": 1,      # 召回实验平均观看时长(按模型统计)
    "immersive_retention_recall_by_model": 1,   # 召回实验留存(按模型统计)
    "immersive_video_watch_average_recall_by_bucket": 1,      # 召回实验平均观看次数(按桶统计)
    "immersive_video_watch_average_of_duration_recall_by_bucket": 1,      # 召回实验平均观看时长(按桶统计)
    "immersive_retention_recall_by_bucket": 1,   # 召回实验留存(按桶统计)
    "immersive_video_watch_average_rough_rank_by_model": 1,      # 粗排实验平均观看次数(按模型统计)
    "immersive_video_watch_average_of_duration_rough_rank_by_model": 1,      # 粗排实验平均观看时长(按模型统计)
    "immersive_retention_rough_rank_by_model": 1,   # 粗排实验留存(按模型统计)
    "video_ctr_recall_by_model": 1,     # 召回实验视频次数ctr(按模型统计)
    "video_ctr_people_recall_by_model": 1,     # 召回实验视频人数ctr(按模型统计)
    "video_ctr_recall_by_bucket": 1,     # 召回实验视频次数ctr(按桶统计)
    "video_ctr_people_recall_by_bucket": 1,     # 召回实验视频人数ctr(按桶统计)
    "video_ctr_rough_rank_by_model": 1,     # 粗排实验视频次数ctr(按模型统计)
    "video_ctr_people_rough_rank_by_model": 1,     # 粗排实验视频人数ctr(按模型统计)
    "video_click_average_recall_by_model": 1,       # 召回实验视频平均点击次数(按模型统计)
    "video_click_average_recall_by_bucket": 1,      # 召回实验视频平均点击次数(按桶统计)
    "video_click_average_rough_rank_by_model": 1,       # 粗排实验视频平均点击次数(按模型统计)
    "video_retention_recall_by_model": 1,   # 召回实验视频留存(按模型统计)
    "video_retention_recall_by_bucket": 1,   # 召回实验视频留存(按桶统计)
    "video_retention_rough_rank_by_model": 1,   # 粗排实验视频留存(按模型统计)
    "thirty_seconds_new_user_app_open_retention": 1,   # 使用时长30秒到3分钟的新用户app_open留存
    "three_minutes_new_user_app_open_retention": 1,   # 使用时长3到5分钟的新用户app_open留存
    "five_minutes_new_user_app_open_retention": 1,   # 使用时长5到10分钟的新用户app_open留存
    "ten_minutes_new_user_app_open_retention": 1,   # 使用时长10分钟以上的新用户app_open留存
    "points_out_statistics": 1,   # 积分支出统计
    "points_out_purpose_statistics": 1,   # 积分支出统计(按gift下的活动统计)
    "points_in_statistics": 1,   # 积分收入统计
    "new_user_invite_master": 1,     # new_user_inivte实验中每日进入实验的师傅的人数
    "new_user_invite_master_with_referrals": 1,     # new_user_inivte实验中每日进入实验且有邀请行为的师傅的人数
    "new_user_invite_apprentice_retention": 1,     # new_user_inivte实验中徒弟的留存
    "new_user_retention_by_brand": 1,     # 新用户留存(按手机品牌)
    "news_read_duration_avg": 1,     # 用户读新闻平均时长
    "news_read_duration_avg_by_count": 1,     # 用户读一个新闻平均时长
    "video_watch_duration_avg": 1,     # 用户沉浸流看视频平均时长
    "video_watch_duration_avg_by_count": 1,     # 用户沉浸流看一个视频平均时长
    "video_impression_duration_avg": 1,     # 用户沉浸外看视频流外平均时长
    "job_query_log_cost": 1,    # BigQuery费用明细
    "page_dau_and_penetration": 1,     # 各页面DAU及渗透率
    "page_avg_time": 1,     # 各页面平均时长
    "page_retention": 1,     # 各页面留存
    "image_ctr_by_placement": 1,     # 各位置照片ctr
    "video_ctr_by_placement": 1,     # 各位置视频ctr
    "silient_user": 1,     # 沉默用户分级
    "accounts_without_ad_click": 1,     # 日活用户广告点击统计
}


# 周期：天

if __name__ == "__main__":
    logger = Logger("Main Day", os.path.join(DIR, 'logs/main_day.log'))
    logger.info("{} start!".format(datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")))
    argv = sys.argv[1:]
    params_msg = "params: [-h] [--help] [-s] [-e] [-k] [--start_time] [--end_time] [--kind]"
    if len(argv) <= 0:
        print(params_msg)
        print("option  参数不能为空，请输入相关参数！")
        sys.exit()
    try:
        """
            options, args = getopt.getopt(args, shortopts, longopts=[])

            参数args：一般是sys.argv[1:]。过滤掉sys.argv[0]，它是执行脚本的名字，不算做命令行参数。
            参数shortopts：短格式分析串。例如："hp:i:"，h后面没有冒号，表示后面不带参数；p和i后面带有冒号，表示后面带参数。
            参数longopts：长格式分析串列表。例如：["help", "ip=", "port="]，help后面没有等号，表示后面不带参数；ip和port后面带冒号，表示后面带参数。

            返回值options是以元组为元素的列表，每个元组的形式为：(选项串, 附加参数)，如：('-i', '192.168.0.1')
            返回值args是个列表，其中的元素是那些不含'-'或'--'的参数。
        """
        opts, args = getopt.getopt(argv, "hs:e:k:", ["help", "start_time=", "end_time=", "kind="])
    except getopt.GetoptError as e:
        print(params_msg)
        print(e.msg)
        sys.exit(2)
    # 处理 返回值options是以元组为元素的列表。
    start_time = None
    end_time = None
    kind = None
    for opt, arg in opts:
        if opt in ("-h", "--help"):
            print(params_msg)
            print("-h / --help:  帮助")
            print("-s / --start_time:  需要计算指标的开始时间，格式:'YYYY-MM-DD HH:MM:SS'   该字段必须是UTC时间")
            print("-e / --end_time:  需要计算指标的截止时间，格式:'YYYY-MM-DD HH:MM:SS'   该字段必须是UTC时间")
            print("-k / --kind:  需要计算的指标， 该字段可取值为：" + " ，".join(KIND.keys()))
            sys.exit()
        elif opt in ("-s", "--start_time"):
            try:
                start_time = datetime.datetime.strptime(arg, '%Y-%m-%d %H:%M:%S')
            except ValueError:
                print(params_msg)
                print("option -s / --start_time:该参数格式错误，请查看 -h / --help")
                sys.exit(2)
        elif opt in ("-e", "--end_time"):
            try:
                end_time = datetime.datetime.strptime(arg, '%Y-%m-%d %H:%M:%S')
            except ValueError:
                print(params_msg)
                print("option -e / --end_time:该参数格式错误，请查看 -h / --help")
                sys.exit(2)
        elif opt in ("-k", "--kind"):
            kind = arg
            if kind not in KIND:
                print(params_msg)
                print("option -k / --kind:该参数取值错误，请查看 -h / --help")
                sys.exit(2)
    """
    获取当前系统时间，并转成ISO格式
    now_time = datetime.datetime.now()
    now_time_iso = now_time.isoformat()

    获取当前utc时间，并转成ISO格式
    方法一：
    utc_tz = pytz.timezone('UTC')
    now_time_utc = datetime.datetime.now(tz=utc_tz)
    now_time_utc_iso = now_time_utc.isoformat()
    方法二：
    now_time_utc = datetime.datetime.utcnow()
    now_time_utc_iso = now_time_utc.isoformat()
    """
    # 设置缺省值
    now_time_utc = datetime.datetime.utcnow()
    if not start_time:
        start_time = now_time_utc - datetime.timedelta(days=1, hours=now_time_utc.hour, minutes=now_time_utc.minute, seconds=now_time_utc.second, microseconds=now_time_utc.microsecond)
    if not end_time:
        end_time = now_time_utc - datetime.timedelta(hours=now_time_utc.hour, minutes=now_time_utc.minute, seconds=now_time_utc.second, microseconds=now_time_utc.microsecond)
    if not kind:
        kind = "all"
    if start_time >= end_time:
        print(params_msg)
        print("option   -s / --start_time 和 -e / --end_time:这组参数赋值有问题：开始时间必须小于截止时间")
        sys.exit(2)
    # 打印 返回值args列表，即其中的元素是那些不含'-'或'--'的参数。
    # for i in range(0, len(args)):
    #     print('参数 %s 为：%s' % (i + 1, args[i]))

    # 开始数据指标统计
    country_code = "'" + "','".join(constants.COUNTRY_CODE) + "'"
    placement = "'" + "','".join(constants.NEWS_PLACEMENT) + "'"
    video_placement = "'" + "','".join(constants.VIDEO_PLACEMENT) + "'"
    video_kind_placement = "'" + "','".join(constants.VIDEO_KIND_PLACEMENT) + "'"
    indicator_dimension = "'" + "','".join(constants.INDICATOR_DIMENSION) + "'"
    recall_experiment = "'" + "','".join(constants.RECALL_EXPERIMENT) + "'"
    rank_experiment = "'" + "','".join(constants.RANKING_EXPERIMENT) + "'"
    if kind == "all":
        ctr.CTRData(start_time, end_time, country_code, placement, indicator_dimension, "day_news_ctr", logger).compute_data()

        ctr_people.CTRPeopleData(start_time, end_time, country_code, placement, indicator_dimension, "day_news_ctr_people", logger).compute_data()

        news_ctr_notification_new_user.NewsCtrNotificationNewUserData(start_time, end_time, country_code, indicator_dimension, "day_news_ctr_notification_new_user", logger).compute_data()

        news_ctr_notification_new_user_people.NewsCtrNotificationNewUserPeopleData(start_time, end_time, country_code, indicator_dimension, "day_news_ctr_notification_new_user_people", logger).compute_data()

        news_ctr_notification_old_user.NewsCtrNotificationOldUserData(start_time, end_time, country_code, indicator_dimension, "day_news_ctr_notification_old_user", logger).compute_data()

        news_ctr_notification_old_user_people.NewsCtrNotificationOldUserPeopleData(start_time, end_time, country_code, indicator_dimension, "day_news_ctr_notification_old_user_people", logger).compute_data()
        
        video_ctr_notification_new_user.VideoCtrNotificationNewUserData(start_time, end_time, country_code, indicator_dimension, "day_video_ctr_notification_new_user", logger).compute_data()

        video_ctr_notification_new_user_people.VideoCtrNotificationNewUserPeopleData(start_time, end_time, country_code, indicator_dimension, "day_video_ctr_notification_new_user_people", logger).compute_data()

        video_ctr_notification_old_user.VideoCtrNotificationOldUserData(start_time, end_time, country_code, indicator_dimension, "day_video_ctr_notification_old_user", logger).compute_data()

        video_ctr_notification_old_user_people.VideoCtrNotificationOldUserPeopleData(start_time, end_time, country_code, indicator_dimension, "day_video_ctr_notification_old_user_people", logger).compute_data()

        new_user_news_click_average.NewUserNewsClickAverageData(start_time, end_time, country_code, placement, indicator_dimension, "day_new_user_news_click_average", logger).compute_data()

        new_user_video_watch_average.NewUserVideoWatchAverageData(start_time, end_time, country_code, video_placement, indicator_dimension, "day_new_user_video_watch_average", logger).compute_data()

        old_user_video_watch_average.OldUserVideoWatchAverageData(start_time, end_time, country_code, video_placement, indicator_dimension, "day_old_user_video_watch_average", logger).compute_data()

        video_watch_average.VideoWatchAverageData(start_time, end_time, country_code, video_placement, indicator_dimension, "day_video_watch_average", logger).compute_data()


        new_users_retention_news_event.NewUsersRetentionNewsEvent(start_time, end_time, country_code, indicator_dimension, "new_users_retention_news_event", logger).compute_data()

        video_ctr.VideoCTRData(start_time, end_time, country_code, video_placement, indicator_dimension, "day_video_ctr", logger).compute_data()

        video_ctr_people.VideoCTRPeopleData(start_time, end_time, country_code, video_placement, indicator_dimension, "day_video_ctr_people", logger).compute_data()

        new_users_retention_tab_impression.NewUsersRetentionTabImpression(start_time, end_time, 'new_users_retention_tab_impression', logger).compute_data()

        experiment_new_users_retention_tab_impression.ExperimentNewUsersRetentionTabImpression(start_time, end_time, indicator_dimension, 'experiment_new_users_retention_tab_impression', logger).compute_data()

        new_users_events_retention.NewUsersEventsRetention(start_time, end_time, 'new_users_events_retention', logger).compute_data()
        
        partiko_memories_new_users_events_retention.PartikoMemoriesNewUsersEventsRetention(start_time, end_time, indicator_dimension, 'partiko_memories_new_users_events_retention', logger).compute_data()

        partiko_memories_new_users_events_retention_with_impression.PartikoMemoriesNewUsersEventsRetentionWithImpression(start_time, end_time, country_code, indicator_dimension, 'partiko_memories_new_users_events_retention_with_impression', logger).compute_data("{}/SQL/{}.sql".format(DIR, "partiko_memories_new_users_events_retention_with_impression"))
        

        partiko_experiment_average_of_invites.PartikoExperimentAverageOfInvites(start_time, end_time, indicator_dimension, 'partiko_experiment_average_of_invites', logger).compute_data()
        
        partiko_memories_user_time_average_of_duration.PartikoMemoriesUserTimeAverageOfDuration(start_time, end_time, indicator_dimension, 'partiko_memories_user_time_average_of_duration', logger).compute_data()

        partiko_memories_new_user_user_time_average_of_duration.PartikoMemoriesNewUserUserTimeAverageOfDuration(start_time, end_time, indicator_dimension, 'partiko_memories_new_user_user_time_average_of_duration', logger).compute_data("{}/SQL/{}.sql".format(DIR, "partiko_memories_new_user_user_time_average_of_duration"))

        experiment_immersive_page_duration_avg.ExperimentImmersivePageDurationAvg(start_time, end_time, country_code, 'experiment_immersive_page_duration_avg', logger).compute_data("{}/SQL/{}.sql".format(DIR, "experiment_immersive_page_duration_avg"))
        
        new_user_news_ctr_people.NewUserCTRPeopleData(start_time, end_time, country_code, placement, indicator_dimension, "day_new_user_news_ctr_people", logger).compute_data()

        new_user_video_ctr_people.NewUserVideoCTRPeopleData(start_time, end_time, country_code, video_placement, indicator_dimension, "day_new_user_video_ctr_people", logger).compute_data()

        partiko_memories_average_of_invites.PartikoMemoriesAverageOfInvites(start_time, end_time, indicator_dimension, 'partiko_memories_average_of_invites', logger).compute_data()

        new_users_partiko_memories_average_of_invites.NewUsersPartikoMemoriesAverageOfInvites(start_time, end_time, indicator_dimension, 'new_users_partiko_memories_average_of_invites', logger).compute_data()

        # partiko_experiment_new_users_retention_tab_impression.PartikoExperimentNewUsersRetentionTabImpression(start_time, end_time, indicator_dimension, 'partiko_experiment_new_users_retention_tab_impression', logger).compute_data()

        old_users_events_retention.OldUsersEventsRetention(start_time, end_time, 'old_users_events_retention', logger).compute_data()

        partiko_memories_old_users_events_retention.PartikoMemoriesOldUsersEventsRetention(start_time, end_time, indicator_dimension, 'partiko_memories_old_users_events_retention', logger).compute_data()

        cash_out.CashOut(start_time, end_time, "cash_out", logger).compute_data("{}/SQL/{}.sql".format(DIR, "cash_out"))

        cashout_by_method.CashOutByMethod(start_time, end_time, "cashout_by_method", logger).compute_data("{}/SQL/{}.sql".format(DIR, "cashout_by_method"))

        cashout_by_created_at.CashOutByCreatedAt(start_time, end_time, "cashout_by_created_at", logger).compute_data("{}/SQL/{}.sql".format(DIR, "cashout_by_created_at"))

        cashout_by_money.CashOutByMoney(start_time, end_time, "cashout_by_money", logger).compute_data("{}/SQL/{}.sql".format(DIR, "cashout_by_money"))

        video_ctr_recall.VideoCtrRecall(start_time, end_time, country_code, recall_experiment, "video_ctr_recall", logger).compute_data("{}/SQL/{}.sql".format(DIR, "video_ctr_recall"))

        video_ctr_dimension_recall.VideoCtrDimensionRecall(start_time, end_time, country_code, recall_experiment, "video_ctr_dimension_recall", logger).compute_data("{}/SQL/{}.sql".format(DIR, "video_ctr_dimension_recall"))

        video_ctr_by_dimension_recall.VideoCtrByDimensionRecall(start_time, end_time, country_code, recall_experiment, "video_ctr_by_dimension_recall", logger).compute_data("{}/SQL/{}.sql".format(DIR, "video_ctr_by_dimension_recall"))

        video_ctr_recall_data_index0.VideoCtrRecallDataIndex0(start_time, end_time, country_code, recall_experiment, "video_ctr_recall_data_index0", logger).compute_data("{}/SQL/{}.sql".format(DIR, "video_ctr_recall_data_index0"))

        video_ctr_people_recall.VideoCtrPeopleRecall(start_time, end_time, country_code, recall_experiment, "video_ctr_people_recall", logger).compute_data("{}/SQL/{}.sql".format(DIR, "video_ctr_people_recall"))

        video_ctr_people_dimension_recall.VideoCtrPeopleDimensionRecall(start_time, end_time, country_code, recall_experiment, "video_ctr_people_dimension_recall", logger).compute_data("{}/SQL/{}.sql".format(DIR, "video_ctr_people_dimension_recall"))

        video_ctr_people_by_dimension_recall.VideoCtrPeopleByDimensionRecall(start_time, end_time, country_code, recall_experiment, "video_ctr_people_by_dimension_recall", logger).compute_data("{}/SQL/{}.sql".format(DIR, "video_ctr_people_by_dimension_recall"))

        video_ctr_people_recall_data_index0.VideoCtrPeopleRecallDataIndex0(start_time, end_time, country_code, recall_experiment, "video_ctr_people_recall_data_index0", logger).compute_data("{}/SQL/{}.sql".format(DIR, "video_ctr_people_recall_data_index0"))

        video_ctr_rank.VideoCtrRank(start_time, end_time, country_code, rank_experiment, "video_ctr_rank", logger).compute_data("{}/SQL/{}.sql".format(DIR, "video_ctr_rank"))

        video_ctr_people_rank.VideoCtrPeopleRank(start_time, end_time, country_code, rank_experiment, "video_ctr_people_rank", logger).compute_data("{}/SQL/{}.sql".format(DIR, "video_ctr_people_rank"))

        video_watch_average_of_duration_recall.VideoWatchAverageOfDurationRecall(start_time, end_time, country_code, recall_experiment, "video_watch_average_of_duration_recall", logger).compute_data("{}/SQL/{}.sql".format(DIR, "video_watch_average_of_duration_recall"))

        video_watch_average_of_duration_by_dimension_recall.VideoWatchAverageOfDurationByDimensionRecall(start_time, end_time, country_code, recall_experiment, "video_watch_average_of_duration_by_dimension_recall", logger).compute_data("{}/SQL/{}.sql".format(DIR, "video_watch_average_of_duration_by_dimension_recall"))

        video_watch_average_of_duration_rank.VideoWatchAverageOfDurationRank(start_time, end_time, country_code, rank_experiment, "video_watch_average_of_duration_rank", logger).compute_data("{}/SQL/{}.sql".format(DIR, "video_watch_average_of_duration_rank"))

        immersive_retention_recall.ImmersiveRetentionRecall(start_time, end_time, recall_experiment, "immersive_retention_recall", logger).compute_data("{}/SQL/{}.sql".format(DIR, "immersive_retention_recall"))

        immersive_retention_by_dimension_recall.ImmersiveRetentionByDimensionRecall(start_time, end_time, recall_experiment, "immersive_retention_by_dimension_recall", logger).compute_data("{}/SQL/{}.sql".format(DIR, "immersive_retention_by_dimension_recall"))

        immersive_retention_rank.ImmersiveRetentionRank(start_time, end_time, rank_experiment, "immersive_retention_rank", logger).compute_data("{}/SQL/{}.sql".format(DIR, "immersive_retention_rank"))

        video_ctr_with_device_model_recall.VideoCtrWithDeviceModelRecall(start_time, end_time, country_code, recall_experiment, "video_ctr_with_device_model_recall", logger).compute_data("{}/SQL/{}.sql".format(DIR, "video_ctr_with_device_model_recall"))

        video_ctr_with_device_model_rank.VideoCtrWithDeviceModelRank(start_time, end_time, country_code, rank_experiment, "video_ctr_with_device_model_rank", logger).compute_data("{}/SQL/{}.sql".format(DIR, "video_ctr_with_device_model_rank"))

        video_ctr_with_brand_recall.VideoCtrWithBrandRecall(start_time, end_time, country_code, recall_experiment, "video_ctr_with_brand_recall", logger).compute_data("{}/SQL/{}.sql".format(DIR, "video_ctr_with_brand_recall"))

        video_ctr_with_brand_rank.VideoCtrWithBrandRank(start_time, end_time, country_code, rank_experiment, "video_ctr_with_brand_rank", logger).compute_data("{}/SQL/{}.sql".format(DIR, "video_ctr_with_brand_rank"))

        video_watch_average_by_dimension_recall.VideoWatchAverageByDimensionRecall(start_time, end_time, country_code, recall_experiment, "video_watch_average_by_dimension_recall", logger).compute_data("{}/SQL/{}.sql".format(DIR, "video_watch_average_by_dimension_recall"))

        video_watch_average_rank.VideoWatchAverageRank(start_time, end_time, country_code, rank_experiment, "video_watch_average_rank", logger).compute_data("{}/SQL/{}.sql".format(DIR, "video_watch_average_rank"))

        posts.Posts(start_time, end_time, country_code, "posts", logger).compute_data("{}/SQL/{}.sql".format(DIR, "posts"))

        posts_user_count.PostsUserCount(start_time, end_time, country_code, "posts_user_count", logger).compute_data("{}/SQL/{}.sql".format(DIR, "posts_user_count"))

        video_retention_recall.VideoRetentionRecall(start_time, end_time, country_code, recall_experiment, "video_retention_recall", logger).compute_data("{}/SQL/{}.sql".format(DIR, "video_retention_recall"))

        video_retention_rank.VideoRetentionRank(start_time, end_time, country_code, rank_experiment, "video_retention_rank", logger).compute_data("{}/SQL/{}.sql".format(DIR, "video_retention_rank"))

        video_average_of_total_duration_recall.VideoAverageOfTotalDurationRecall(start_time, end_time, country_code, recall_experiment, "video_average_of_total_duration_recall", logger).compute_data("{}/SQL/{}.sql".format(DIR, "video_average_of_total_duration_recall"))

        video_average_of_total_duration_rank.VideoAverageOfTotalDurationRank(start_time, end_time, country_code, rank_experiment, "video_average_of_total_duration_rank", logger).compute_data("{}/SQL/{}.sql".format(DIR, "video_average_of_total_duration_rank"))

        video_average_of_total_duration_by_dimension_recall.VideoAverageOfTotalDurationByDimensionRecall(start_time, end_time, country_code, recall_experiment, "video_average_of_total_duration_by_dimension_recall", logger).compute_data("{}/SQL/{}.sql".format(DIR, "video_average_of_total_duration_by_dimension_recall"))

        video_ctr_without_experiments.VideoCtrWithoutExperiments(start_time, end_time, country_code, "video_ctr_without_experiments", logger).compute_data("{}/SQL/{}.sql".format(DIR, "video_ctr_without_experiments"))

        video_ctr_people_without_experiments.VideoCtrPeopleWithoutExperiments(start_time, end_time, country_code, "video_ctr_people_without_experiments", logger).compute_data("{}/SQL/{}.sql".format(DIR, "video_ctr_people_without_experiments"))

        video_watch_average_of_duration_without_experiments.VideoWatchAverageOfDurationWithoutExperiments(start_time, end_time, country_code, "video_watch_average_of_duration_without_experiments", logger).compute_data("{}/SQL/{}.sql".format(DIR, "video_watch_average_of_duration_without_experiments"))

        user_total_duration_average.UserTotalDurationAverage(start_time, end_time, country_code, "user_total_duration_average", logger).compute_data("{}/SQL/{}.sql".format(DIR, "user_total_duration_average"))

        user_avg_cost.UserAvgCostOut(start_time, "user_avg_cost", logger).compute_data("{}/SQL/{}.sql".format(DIR, "user_avg_cost"))

        short_video_ctr.ShortVideoCtr(start_time, end_time, country_code, "short_video_ctr", logger).compute_data("{}/SQL/{}.sql".format(DIR, "short_video_ctr"))

        short_new_video_ctr.ShortNewVideoCtr(start_time, end_time, country_code, "short_new_video_ctr", logger).compute_data("{}/SQL/{}.sql".format(DIR, "short_new_video_ctr"))

        short_video_completion_rate.ShortVideoCompletionRate(start_time, end_time, country_code, "short_video_completion_rate", logger).compute_data("{}/SQL/{}.sql".format(DIR, "short_video_completion_rate"))

        short_new_video_completion_rate.ShortNewVideoCompletionRate(start_time, end_time, country_code, "short_new_video_completion_rate", logger).compute_data("{}/SQL/{}.sql".format(DIR, "short_new_video_completion_rate"))

        one_minute_video_ctr.OneMinuteVideoCtr(start_time, end_time, country_code, "one_minute_video_ctr", logger).compute_data("{}/SQL/{}.sql".format(DIR, "one_minute_video_ctr"))

        one_minute_new_video_ctr.OneMinuteNewVideoCtr(start_time, end_time, country_code, "one_minute_new_video_ctr", logger).compute_data("{}/SQL/{}.sql".format(DIR, "one_minute_new_video_ctr"))

        one_minute_video_completion_rate.OneMinuteVideoCompletionRate(start_time, end_time, country_code, "one_minute_video_completion_rate", logger).compute_data("{}/SQL/{}.sql".format(DIR, "one_minute_video_completion_rate"))

        one_minute_new_video_completion_rate.OneMinuteNewVideoCompletionRate(start_time, end_time, country_code, "one_minute_new_video_completion_rate", logger).compute_data("{}/SQL/{}.sql".format(DIR, "one_minute_new_video_completion_rate"))

        five_minutes_video_ctr.FiveMinutesVideoCtr(start_time, end_time, country_code, "five_minutes_video_ctr", logger).compute_data("{}/SQL/{}.sql".format(DIR, "five_minutes_video_ctr"))

        five_minutes_new_video_ctr.FiveMinutesNewVideoCtr(start_time, end_time, country_code, "five_minutes_new_video_ctr", logger).compute_data("{}/SQL/{}.sql".format(DIR, "five_minutes_new_video_ctr"))

        five_minutes_video_completion_rate.FiveMinutesVideoCompletionRate(start_time, end_time, country_code, "five_minutes_video_completion_rate", logger).compute_data("{}/SQL/{}.sql".format(DIR, "five_minutes_video_completion_rate"))

        five_minutes_new_video_completion_rate.FiveMinutesNewVideoCompletionRate(start_time, end_time, country_code, "five_minutes_new_video_completion_rate", logger).compute_data("{}/SQL/{}.sql".format(DIR, "five_minutes_new_video_completion_rate"))

        push_ctr_without_experiments.PushCtrWithoutExperiments(start_time, end_time, country_code, "push_ctr_without_experiments", logger).compute_data("{}/SQL/{}.sql".format(DIR, "push_ctr_without_experiments"))

        push_ctr_people_without_experiments.PushCtrPeopleWithoutExperiments(start_time, end_time, country_code, "push_ctr_people_without_experiments", logger).compute_data("{}/SQL/{}.sql".format(DIR, "push_ctr_people_without_experiments"))

        notification_video_ctr_without_experiments.NotificationVideoCtrWithoutExperiments(start_time, end_time, country_code, "notification_video_ctr_without_experiments", logger).compute_data("{}/SQL/{}.sql".format(DIR, "notification_video_ctr_without_experiments"))

        notification_video_ctr_without_experiments_by_people.NotificationVideoCtrWithoutExperimentsByPeople(start_time, end_time, country_code, "notification_video_ctr_without_experiments_by_people", logger).compute_data("{}/SQL/{}.sql".format(DIR, "notification_video_ctr_without_experiments_by_people"))

        notification_news_ctr_without_experiments.NotificationNewsCtrWithoutExperiments(start_time, end_time, country_code, "notification_news_ctr_without_experiments", logger).compute_data("{}/SQL/{}.sql".format(DIR, "notification_news_ctr_without_experiments"))

        notification_news_ctr_without_experiments_by_people.NotificationNewsCtrWithoutExperimentsByPeople(start_time, end_time, country_code, "notification_news_ctr_without_experiments_by_people", logger).compute_data("{}/SQL/{}.sql".format(DIR, "notification_news_ctr_without_experiments_by_people"))

        new_videos_ctr.NewVideosCtr(start_time, end_time, country_code, "new_videos_ctr", logger).compute_data("{}/SQL/{}.sql".format(DIR, "new_videos_ctr"))

        immersive_video_watch_average_recall_by_model.ImmersiveVideoWatchAverageRecallByModel(start_time, end_time, country_code, "immersive_video_watch_average_recall_by_model", logger).compute_data("{}/SQL/{}.sql".format(DIR, "immersive_video_watch_average_recall_by_model"))

        immersive_video_watch_average_of_duration_recall_by_model.ImmersiveVideoWatchAverageOfDurationRecallByModel(start_time, end_time, country_code, "immersive_video_watch_average_of_duration_recall_by_model", logger).compute_data("{}/SQL/{}.sql".format(DIR, "immersive_video_watch_average_of_duration_recall_by_model"))

        immersive_retention_recall_by_model.ImmersiveRetentionRecallByModel(start_time, end_time, country_code, "immersive_retention_recall_by_model", logger).compute_data("{}/SQL/{}.sql".format(DIR, "immersive_retention_recall_by_model"))

        immersive_video_watch_average_recall_by_bucket.ImmersiveVideoWatchAverageRecallByBucket(start_time, end_time, country_code, "immersive_video_watch_average_recall_by_bucket", logger).compute_data("{}/SQL/{}.sql".format(DIR, "immersive_video_watch_average_recall_by_bucket"))

        immersive_video_watch_average_of_duration_recall_by_bucket.ImmersiveVideoWatchAverageOfDurationRecallByBucket(start_time, end_time, country_code, "immersive_video_watch_average_of_duration_recall_by_bucket", logger).compute_data("{}/SQL/{}.sql".format(DIR, "immersive_video_watch_average_of_duration_recall_by_bucket"))

        immersive_retention_recall_by_bucket.ImmersiveRetentionRecallByBucket(start_time, end_time, country_code, "immersive_retention_recall_by_bucket", logger).compute_data("{}/SQL/{}.sql".format(DIR, "immersive_retention_recall_by_bucket"))

        immersive_video_watch_average_rough_rank_by_model.ImmersiveVideoWatchAverageRoughRankByModel(start_time, end_time, country_code, "immersive_video_watch_average_rough_rank_by_model", logger).compute_data("{}/SQL/{}.sql".format(DIR, "immersive_video_watch_average_rough_rank_by_model"))

        immersive_video_watch_average_of_duration_rough_rank_by_model.ImmersiveVideoWatchAverageOfDurationRoughRankByModel(start_time, end_time, country_code, "immersive_video_watch_average_of_duration_rough_rank_by_model", logger).compute_data("{}/SQL/{}.sql".format(DIR, "immersive_video_watch_average_of_duration_rough_rank_by_model"))

        immersive_retention_rough_rank_by_model.ImmersiveRetentionRoughRankByModel(start_time, end_time, country_code, "immersive_retention_rough_rank_by_model", logger).compute_data("{}/SQL/{}.sql".format(DIR, "immersive_retention_rough_rank_by_model"))

        video_ctr_recall_by_model.VideoCtrRecallByModel(start_time, end_time, country_code, "video_ctr_recall_by_model", logger).compute_data("{}/SQL/{}.sql".format(DIR, "video_ctr_recall_by_model"))

        video_ctr_people_recall_by_model.VideoCtrPeopleRecallByModel(start_time, end_time, country_code, "video_ctr_people_recall_by_model", logger).compute_data("{}/SQL/{}.sql".format(DIR, "video_ctr_people_recall_by_model"))

        video_ctr_recall_by_bucket.VideoCtrRecallByBucket(start_time, end_time, country_code, "video_ctr_recall_by_bucket", logger).compute_data("{}/SQL/{}.sql".format(DIR, "video_ctr_recall_by_bucket"))

        video_ctr_people_recall_by_bucket.VideoCtrPeopleRecallByBucket(start_time, end_time, country_code, "video_ctr_people_recall_by_bucket", logger).compute_data("{}/SQL/{}.sql".format(DIR, "video_ctr_people_recall_by_bucket"))

        video_ctr_rough_rank_by_model.VideoCtrRoughRankByModel(start_time, end_time, country_code, "video_ctr_rough_rank_by_model", logger).compute_data("{}/SQL/{}.sql".format(DIR, "video_ctr_rough_rank_by_model"))

        video_ctr_people_rough_rank_by_model.VideoCtrPeopleRoughRankByModel(start_time, end_time, country_code, "video_ctr_people_rough_rank_by_model", logger).compute_data("{}/SQL/{}.sql".format(DIR, "video_ctr_people_rough_rank_by_model"))

        video_click_average_recall_by_model.VideoClickAverageRecallByModel(start_time, end_time, country_code, "video_click_average_recall_by_model", logger).compute_data("{}/SQL/{}.sql".format(DIR, "video_click_average_recall_by_model"))

        video_click_average_recall_by_bucket.VideoClickAverageRecallByBucket(start_time, end_time, country_code, "video_click_average_recall_by_bucket", logger).compute_data("{}/SQL/{}.sql".format(DIR, "video_click_average_recall_by_bucket"))

        video_click_average_rough_rank_by_model.VideoClickAverageRoughRankByModel(start_time, end_time, country_code, "video_click_average_rough_rank_by_model", logger).compute_data("{}/SQL/{}.sql".format(DIR, "video_click_average_rough_rank_by_model"))

        video_retention_recall_by_model.VideoRetentionRecallByModel(start_time, end_time, country_code, "video_retention_recall_by_model", logger).compute_data("{}/SQL/{}.sql".format(DIR, "video_retention_recall_by_model"))

        video_retention_recall_by_bucket.VideoRetentionRecallByBucket(start_time, end_time, country_code, "video_retention_recall_by_bucket", logger).compute_data("{}/SQL/{}.sql".format(DIR, "video_retention_recall_by_bucket"))

        video_retention_rough_rank_by_model.VideoRetentionRoughRankByModel(start_time, end_time, country_code, "video_retention_rough_rank_by_model", logger).compute_data("{}/SQL/{}.sql".format(DIR, "video_retention_rough_rank_by_model"))

        thirty_seconds_new_user_app_open_retention.ThirtySecondsNewUserAppOpenRetention(start_time, end_time, country_code, "thirty_seconds_new_user_app_open_retention", logger).compute_data("{}/SQL/{}.sql".format(DIR, "thirty_seconds_new_user_app_open_retention"))

        three_minutes_new_user_app_open_retention.ThreeMinutesNewUserAppOpenRetention(start_time, end_time, country_code, "three_minutes_new_user_app_open_retention", logger).compute_data("{}/SQL/{}.sql".format(DIR, "three_minutes_new_user_app_open_retention"))

        five_minutes_new_user_app_open_retention.FiveMinutesNewUserAppOpenRetention(start_time, end_time, country_code, "five_minutes_new_user_app_open_retention", logger).compute_data("{}/SQL/{}.sql".format(DIR, "five_minutes_new_user_app_open_retention"))

        ten_minutes_new_user_app_open_retention.TenMinutesNewUserAppOpenRetention(start_time, end_time, country_code, "ten_minutes_new_user_app_open_retention", logger).compute_data("{}/SQL/{}.sql".format(DIR, "ten_minutes_new_user_app_open_retention"))

        points_out_statistics.PointsOutStatistics(start_time, end_time, country_code, "points_out_statistics", logger).compute_data("{}/SQL/{}.sql".format(DIR, "points_out_statistics"))

        points_out_purpose_statistics.PointsOutPurposeStatistics(start_time, end_time, country_code, "points_out_purpose_statistics", logger).compute_data("{}/SQL/{}.sql".format(DIR, "points_out_purpose_statistics"))

        points_in_statistics.PointsInStatistics(start_time, end_time, country_code, "points_in_statistics", logger).compute_data("{}/SQL/{}.sql".format(DIR, "points_in_statistics"))

        new_user_invite_master.NewUserInviteMaster(start_time, end_time, country_code, "new_user_invite_master", logger).compute_data("{}/SQL/{}.sql".format(DIR, "new_user_invite_master"))

        new_user_invite_master_with_referrals.NewUserInviteMasterWithReferrals(start_time, end_time, country_code, "new_user_invite_master_with_referrals", logger).compute_data("{}/SQL/{}.sql".format(DIR, "new_user_invite_master_with_referrals"))

        new_user_invite_apprentice_retention.NewUserInviteApprenticeRetention(start_time, end_time, country_code, "new_user_invite_apprentice_retention", logger).compute_data("{}/SQL/{}.sql".format(DIR, "new_user_invite_apprentice_retention"))

        new_user_retention_by_brand.NewUserRetentionByBrand(start_time, end_time, country_code, "new_user_retention_by_brand", logger).compute_data("{}/SQL/{}.sql".format(DIR, "new_user_retention_by_brand"))

        news_read_duration_avg.NewsReadDurationAvg(start_time, end_time, country_code, "news_read_duration_avg", logger).compute_data("{}/SQL/{}.sql".format(DIR, "news_read_duration_avg"))

        news_read_duration_avg_by_count.NewsReadDurationAvgByCount(start_time, end_time, country_code, "news_read_duration_avg_by_count", logger).compute_data("{}/SQL/{}.sql".format(DIR, "news_read_duration_avg_by_count"))

        video_watch_duration_avg.VideoWatchDurationAvg(start_time, end_time, country_code, "video_watch_duration_avg", logger).compute_data("{}/SQL/{}.sql".format(DIR, "video_watch_duration_avg"))

        video_watch_duration_avg_by_count.VideoWatchDurationAvgByCount(start_time, end_time, country_code, "video_watch_duration_avg_by_count", logger).compute_data("{}/SQL/{}.sql".format(DIR, "video_watch_duration_avg_by_count"))

        video_impression_duration_avg.VideoImpressionDurationAvg(start_time, end_time, country_code, "video_impression_duration_avg", logger).compute_data("{}/SQL/{}.sql".format(DIR, "video_impression_duration_avg"))

        job_query_log_cost.JobQueryLogCost(start_time, end_time, "job_query_log_cost", logger).compute_data("{}/SQL/{}.sql".format(DIR, "job_query_log_cost"))

        page_dau_and_penetration.PageDauAndPenetration(start_time, end_time, country_code, "page_dau_and_penetration", logger).compute_data("{}/SQL/{}.sql".format(DIR, "page_dau_and_penetration"))

        page_avg_time.PageAvgTime(start_time, end_time, country_code, "page_avg_time", logger).compute_data("{}/SQL/{}.sql".format(DIR, "page_avg_time"))

        page_retention.PageRetention(start_time, end_time, country_code, "page_retention", logger).compute_data("{}/SQL/{}.sql".format(DIR, "page_retention"))

        image_ctr_by_placement.ImageCtrByPlacement(start_time, end_time, country_code,  "image_ctr_by_placement", logger).compute_data("{}/SQL/{}.sql".format(DIR, "image_ctr_by_placement"))

        video_ctr_by_placement.VideoCtrByPlacement(start_time, end_time, country_code,  "video_ctr_by_placement", logger).compute_data("{}/SQL/{}.sql".format(DIR, "video_ctr_by_placement"))

        silient_user.SilentUser(start_time, end_time, country_code,  "silient_user", logger).compute_data("{}/SQL/{}.sql".format(DIR, "silient_user"))

        accounts_without_ad_click.AccountsWithoutAdClick(start_time, end_time, country_code,  "accounts_without_ad_click", logger).compute_data("{}/SQL/{}.sql".format(DIR, "accounts_without_ad_click"))


    elif kind == "ctr":
        ctr.CTRData(start_time, end_time, country_code, placement, indicator_dimension, "day_news_ctr", logger).compute_data()
    elif kind == "ctr_people":
        ctr_people.CTRPeopleData(start_time, end_time, country_code, placement, indicator_dimension, "day_news_ctr_people", logger).compute_data()
    elif kind == "news_ctr_notification_new_user":
        news_ctr_notification_new_user.NewsCtrNotificationNewUserData(start_time, end_time, country_code, indicator_dimension, "day_news_ctr_notification_new_user", logger).compute_data()
    elif kind == "news_ctr_notification_new_user_people":
        news_ctr_notification_new_user_people.NewsCtrNotificationNewUserPeopleData(start_time, end_time, country_code, indicator_dimension, "day_news_ctr_notification_new_user_people", logger).compute_data()
    elif kind == "news_ctr_notification_old_user":
        news_ctr_notification_old_user.NewsCtrNotificationOldUserData(start_time, end_time, country_code, indicator_dimension, "day_news_ctr_notification_old_user", logger).compute_data()
    elif kind == "news_ctr_notification_old_user_people":
        news_ctr_notification_old_user_people.NewsCtrNotificationOldUserPeopleData(start_time, end_time, country_code, indicator_dimension, "day_news_ctr_notification_old_user_people", logger).compute_data()
    elif kind == "video_ctr_notification_new_user":
        video_ctr_notification_new_user.VideoCtrNotificationNewUserData(start_time, end_time, country_code, indicator_dimension, "day_video_ctr_notification_new_user", logger).compute_data()
    elif kind == "video_ctr_notification_new_user_people":
        video_ctr_notification_new_user_people.VideoCtrNotificationNewUserPeopleData(start_time, end_time, country_code, indicator_dimension, "day_video_ctr_notification_new_user_people", logger).compute_data()
    elif kind == "video_ctr_notification_old_user":
        video_ctr_notification_old_user.VideoCtrNotificationOldUserData(start_time, end_time, country_code, indicator_dimension, "day_video_ctr_notification_old_user", logger).compute_data()
    elif kind == "video_ctr_notification_old_user_people":
        video_ctr_notification_old_user_people.VideoCtrNotificationOldUserPeopleData(start_time, end_time, country_code, indicator_dimension, "day_video_ctr_notification_old_user_people", logger).compute_data()
    elif kind == "new_user_news_click_average":
        new_user_news_click_average.NewUserNewsClickAverageData(start_time, end_time, country_code, placement, indicator_dimension, "day_new_user_news_click_average", logger).compute_data()
    elif kind == "new_user_video_watch_average":
        new_user_video_watch_average.NewUserVideoWatchAverageData(start_time, end_time, country_code, video_placement, indicator_dimension, "day_new_user_video_watch_average", logger).compute_data()
    elif kind == "old_user_video_watch_average":
        old_user_video_watch_average.OldUserVideoWatchAverageData(start_time, end_time, country_code, video_placement, indicator_dimension, "day_old_user_video_watch_average", logger).compute_data()
    elif kind == "video_watch_average":
        video_watch_average.VideoWatchAverageData(start_time, end_time, country_code, video_placement, indicator_dimension, "day_video_watch_average", logger).compute_data()
    elif kind == "new_users_retention_news_event":
        new_users_retention_news_event.NewUsersRetentionNewsEvent(start_time, end_time, country_code, indicator_dimension, "new_users_retention_news_event", logger).compute_data()
    elif kind == "video_ctr":
        video_ctr.VideoCTRData(start_time, end_time, country_code, video_placement, indicator_dimension, "day_video_ctr", logger).compute_data()
    elif kind == "video_ctr_people":
        video_ctr_people.VideoCTRPeopleData(start_time, end_time, country_code, video_placement, indicator_dimension, "day_video_ctr_people", logger).compute_data()
    elif kind == 'new_users_retention_tab_impression':
        new_users_retention_tab_impression.NewUsersRetentionTabImpression(start_time, end_time, 'new_users_retention_tab_impression', logger).compute_data()
    elif kind == 'new_users_events_retention':
        new_users_events_retention.NewUsersEventsRetention(start_time, end_time, 'new_users_events_retention', logger).compute_data()
    elif kind == 'old_users_events_retention':
        old_users_events_retention.OldUsersEventsRetention(start_time, end_time, 'old_users_events_retention', logger).compute_data()
    elif kind == 'experiment_new_users_retention_tab_impression':
        experiment_new_users_retention_tab_impression.ExperimentNewUsersRetentionTabImpression(start_time, end_time, indicator_dimension, 'experiment_new_users_retention_tab_impression', logger).compute_data()
    elif kind == 'partiko_memories_new_users_events_retention':
        partiko_memories_new_users_events_retention.PartikoMemoriesNewUsersEventsRetention(start_time, end_time, indicator_dimension, 'partiko_memories_new_users_events_retention', logger).compute_data()
    elif kind == 'partiko_memories_new_users_events_retention_with_impression':
        partiko_memories_new_users_events_retention_with_impression.PartikoMemoriesNewUsersEventsRetentionWithImpression(start_time, end_time, country_code, indicator_dimension, 'partiko_memories_new_users_events_retention_with_impression', logger).compute_data("{}/SQL/{}.sql".format(DIR, "partiko_memories_new_users_events_retention_with_impression"))
    elif kind == 'partiko_memories_old_users_events_retention':
        partiko_memories_old_users_events_retention.PartikoMemoriesOldUsersEventsRetention(start_time, end_time, indicator_dimension, 'partiko_memories_old_users_events_retention', logger).compute_data()
    elif kind == 'partiko_memories_average_of_invites':
        partiko_memories_average_of_invites.PartikoMemoriesAverageOfInvites(start_time, end_time, indicator_dimension, 'partiko_memories_average_of_invites', logger).compute_data()
    elif kind == 'new_users_partiko_memories_average_of_invites':
        new_users_partiko_memories_average_of_invites.NewUsersPartikoMemoriesAverageOfInvites(start_time, end_time, indicator_dimension, 'new_users_partiko_memories_average_of_invites', logger).compute_data()
    elif kind == 'partiko_experiment_average_of_invites':
        partiko_experiment_average_of_invites.PartikoExperimentAverageOfInvites(start_time, end_time, indicator_dimension, 'partiko_experiment_average_of_invites', logger).compute_data()
    elif kind == 'partiko_memories_user_time_average_of_duration':
        partiko_memories_user_time_average_of_duration.PartikoMemoriesUserTimeAverageOfDuration(start_time, end_time, indicator_dimension, 'partiko_memories_user_time_average_of_duration', logger).compute_data()
    elif kind == 'partiko_memories_new_user_user_time_average_of_duration':
        partiko_memories_new_user_user_time_average_of_duration.PartikoMemoriesNewUserUserTimeAverageOfDuration(start_time, end_time, indicator_dimension, 'partiko_memories_new_user_user_time_average_of_duration', logger).compute_data("{}/SQL/{}.sql".format(DIR, "partiko_memories_new_user_user_time_average_of_duration"))
    elif kind == "experiment_immersive_page_duration_avg":
        experiment_immersive_page_duration_avg.ExperimentImmersivePageDurationAvg(start_time, end_time, country_code, 'experiment_immersive_page_duration_avg', logger).compute_data("{}/SQL/{}.sql".format(DIR, "experiment_immersive_page_duration_avg"))
    elif kind == "new_user_news_ctr_people":
        new_user_news_ctr_people.NewUserCTRPeopleData(start_time, end_time, country_code, placement, indicator_dimension, "day_new_user_news_ctr_people", logger).compute_data()
    elif kind == "new_user_video_ctr_people":
        new_user_video_ctr_people.NewUserVideoCTRPeopleData(start_time, end_time, country_code, video_placement, indicator_dimension, "day_new_user_video_ctr_people", logger).compute_data()
    elif kind == "push_tention":
        push_retention.PushRetentionData(start_time, end_time, country_code, indicator_dimension, "push_tention", logger).compute_data()
    # elif kind == "partiko_experiment_new_users_retention_tab_impression":
    #     partiko_experiment_new_users_retention_tab_impression.PartikoExperimentNewUsersRetentionTabImpression(start_time, end_time, indicator_dimension, "partiko_experiment_new_users_retention_tab_impression", logger).compute_data()
    elif kind == "new_video_click_ctr_by_type":
        new_video_click_ctr_by_type.NewVideoClickCtrByType(start_time, end_time, video_kind_placement, "new_video_click_ctr_by_type", logger).compute_data()
    elif kind == "cash_out":
        cash_out.CashOut(start_time, end_time, "cash_out", logger).compute_data("{}/SQL/{}.sql".format(DIR, "cash_out"))
    elif kind == "cashout_by_method":
        cashout_by_method.CashOutByMethod(start_time, end_time, "cashout_by_method", logger).compute_data("{}/SQL/{}.sql".format(DIR, "cashout_by_method"))
    elif kind == "cashout_by_created_at":
        cashout_by_created_at.CashOutByCreatedAt(start_time, end_time, "cashout_by_created_at", logger).compute_data("{}/SQL/{}.sql".format(DIR, "cashout_by_created_at"))
    elif kind == "cashout_by_money":
        cashout_by_money.CashOutByMoney(start_time, end_time, "cashout_by_money", logger).compute_data("{}/SQL/{}.sql".format(DIR, "cashout_by_money"))
    elif kind == "video_ctr_recall":
        video_ctr_recall.VideoCtrRecall(start_time, end_time, country_code, recall_experiment, "video_ctr_recall", logger).compute_data("{}/SQL/{}.sql".format(DIR, "video_ctr_recall"))
    elif kind == "video_ctr_dimension_recall":
        video_ctr_dimension_recall.VideoCtrDimensionRecall(start_time, end_time, country_code, recall_experiment, "video_ctr_dimension_recall", logger).compute_data("{}/SQL/{}.sql".format(DIR, "video_ctr_dimension_recall"))
    elif kind == "video_ctr_by_dimension_recall":
        video_ctr_by_dimension_recall.VideoCtrByDimensionRecall(start_time, end_time, country_code, recall_experiment, "video_ctr_by_dimension_recall", logger).compute_data("{}/SQL/{}.sql".format(DIR, "video_ctr_by_dimension_recall"))
    elif kind == "video_ctr_recall_data_index0":
        video_ctr_recall_data_index0.VideoCtrRecallDataIndex0(start_time, end_time, country_code, recall_experiment, "video_ctr_recall_data_index0", logger).compute_data("{}/SQL/{}.sql".format(DIR, "video_ctr_recall_data_index0"))
    elif kind == "video_ctr_people_recall":
        video_ctr_people_recall.VideoCtrPeopleRecall(start_time, end_time, country_code, recall_experiment, "video_ctr_people_recall", logger).compute_data("{}/SQL/{}.sql".format(DIR, "video_ctr_people_recall"))
    elif kind == "video_ctr_people_dimension_recall":
        video_ctr_people_dimension_recall.VideoCtrPeopleDimensionRecall(start_time, end_time, country_code, recall_experiment, "video_ctr_people_dimension_recall", logger).compute_data("{}/SQL/{}.sql".format(DIR, "video_ctr_people_dimension_recall"))
    elif kind == "video_ctr_people_by_dimension_recall":
        video_ctr_people_by_dimension_recall.VideoCtrPeopleByDimensionRecall(start_time, end_time, country_code, recall_experiment, "video_ctr_people_by_dimension_recall", logger).compute_data("{}/SQL/{}.sql".format(DIR, "video_ctr_people_by_dimension_recall"))
    elif kind == "video_ctr_people_recall_data_index0":
        video_ctr_people_recall_data_index0.VideoCtrPeopleRecallDataIndex0(start_time, end_time, country_code, recall_experiment, "video_ctr_people_recall_data_index0", logger).compute_data("{}/SQL/{}.sql".format(DIR, "video_ctr_people_recall_data_index0"))
    elif kind == "video_ctr_rank":
        video_ctr_rank.VideoCtrRank(start_time, end_time, country_code, rank_experiment, "video_ctr_rank", logger).compute_data("{}/SQL/{}.sql".format(DIR, "video_ctr_rank"))
    elif kind == "video_ctr_people_rank":
        video_ctr_people_rank.VideoCtrPeopleRank(start_time, end_time, country_code, rank_experiment, "video_ctr_people_rank", logger).compute_data("{}/SQL/{}.sql".format(DIR, "video_ctr_people_rank"))
    elif kind == "video_watch_average_of_duration_recall":
        video_watch_average_of_duration_recall.VideoWatchAverageOfDurationRecall(start_time, end_time, country_code, recall_experiment, "video_watch_average_of_duration_recall", logger).compute_data("{}/SQL/{}.sql".format(DIR, "video_watch_average_of_duration_recall"))
    elif kind == "video_watch_average_of_duration_by_dimension_recall":
        video_watch_average_of_duration_by_dimension_recall.VideoWatchAverageOfDurationByDimensionRecall(start_time, end_time, country_code, recall_experiment, "video_watch_average_of_duration_by_dimension_recall", logger).compute_data("{}/SQL/{}.sql".format(DIR, "video_watch_average_of_duration_by_dimension_recall"))
    elif kind == "video_watch_average_of_duration_rank":
        video_watch_average_of_duration_rank.VideoWatchAverageOfDurationRank(start_time, end_time, country_code, rank_experiment, "video_watch_average_of_duration_rank", logger).compute_data("{}/SQL/{}.sql".format(DIR, "video_watch_average_of_duration_rank"))
    elif kind == "immersive_retention_recall":
        immersive_retention_recall.ImmersiveRetentionRecall(start_time, end_time, recall_experiment, "immersive_retention_recall", logger).compute_data("{}/SQL/{}.sql".format(DIR, "immersive_retention_recall"))
    elif kind == "immersive_retention_by_dimension_recall":
        immersive_retention_by_dimension_recall.ImmersiveRetentionByDimensionRecall(start_time, end_time, recall_experiment, "immersive_retention_by_dimension_recall", logger).compute_data("{}/SQL/{}.sql".format(DIR, "immersive_retention_by_dimension_recall"))
    elif kind == "immersive_retention_rank":
        immersive_retention_rank.ImmersiveRetentionRank(start_time, end_time, rank_experiment, "immersive_retention_rank", logger).compute_data("{}/SQL/{}.sql".format(DIR, "immersive_retention_rank"))
    elif kind == "video_ctr_with_device_model_recall":
        video_ctr_with_device_model_recall.VideoCtrWithDeviceModelRecall(start_time, end_time, country_code, recall_experiment, "video_ctr_with_device_model_recall", logger).compute_data("{}/SQL/{}.sql".format(DIR, "video_ctr_with_device_model_recall"))
    elif kind == "video_ctr_with_device_model_rank":
        video_ctr_with_device_model_rank.VideoCtrWithDeviceModelRank(start_time, end_time, country_code, rank_experiment, "video_ctr_with_device_model_rank", logger).compute_data("{}/SQL/{}.sql".format(DIR, "video_ctr_with_device_model_rank"))
    elif kind == "video_ctr_with_brand_recall":
        video_ctr_with_brand_recall.VideoCtrWithBrandRecall(start_time, end_time, country_code, recall_experiment, "video_ctr_with_brand_recall", logger).compute_data("{}/SQL/{}.sql".format(DIR, "video_ctr_with_brand_recall"))
    elif kind == "video_ctr_with_brand_rank":
        video_ctr_with_brand_rank.VideoCtrWithBrandRank(start_time, end_time, country_code, rank_experiment, "video_ctr_with_brand_rank", logger).compute_data("{}/SQL/{}.sql".format(DIR, "video_ctr_with_brand_rank"))
    elif kind == "video_watch_average_by_dimension_recall":
        video_watch_average_by_dimension_recall.VideoWatchAverageByDimensionRecall(start_time, end_time, country_code, recall_experiment, "video_watch_average_by_dimension_recall", logger).compute_data("{}/SQL/{}.sql".format(DIR, "video_watch_average_by_dimension_recall"))
    elif kind == "video_watch_average_rank":
        video_watch_average_rank.VideoWatchAverageRank(start_time, end_time, country_code, rank_experiment, "video_watch_average_rank", logger).compute_data("{}/SQL/{}.sql".format(DIR, "video_watch_average_rank"))
    elif kind == "posts":
        posts.Posts(start_time, end_time, country_code, "posts", logger).compute_data("{}/SQL/{}.sql".format(DIR, "posts"))
    elif kind == "posts_user_count":
        posts_user_count.PostsUserCount(start_time, end_time, country_code, "posts_user_count", logger).compute_data("{}/SQL/{}.sql".format(DIR, "posts_user_count"))
    elif kind == "video_retention_recall":
        video_retention_recall.VideoRetentionRecall(start_time, end_time, country_code, recall_experiment, "video_retention_recall", logger).compute_data("{}/SQL/{}.sql".format(DIR, "video_retention_recall"))
    elif kind == "video_retention_rank":
        video_retention_rank.VideoRetentionRank(start_time, end_time, country_code, rank_experiment, "video_retention_rank", logger).compute_data("{}/SQL/{}.sql".format(DIR, "video_retention_rank"))
    elif kind == "video_average_of_total_duration_recall":
        video_average_of_total_duration_recall.VideoAverageOfTotalDurationRecall(start_time, end_time, country_code, recall_experiment, "video_average_of_total_duration_recall", logger).compute_data("{}/SQL/{}.sql".format(DIR, "video_average_of_total_duration_recall"))
    elif kind == "video_average_of_total_duration_rank":
        video_average_of_total_duration_rank.VideoAverageOfTotalDurationRank(start_time, end_time, country_code, rank_experiment, "video_average_of_total_duration_rank", logger).compute_data("{}/SQL/{}.sql".format(DIR, "video_average_of_total_duration_rank"))
    elif kind == "video_average_of_total_duration_by_dimension_recall":
        video_average_of_total_duration_by_dimension_recall.VideoAverageOfTotalDurationByDimensionRecall(start_time, end_time, country_code, recall_experiment, "video_average_of_total_duration_by_dimension_recall", logger).compute_data("{}/SQL/{}.sql".format(DIR, "video_average_of_total_duration_by_dimension_recall"))
    elif kind == "video_ctr_without_experiments":
        video_ctr_without_experiments.VideoCtrWithoutExperiments(start_time, end_time, country_code, "video_ctr_without_experiments", logger).compute_data("{}/SQL/{}.sql".format(DIR, "video_ctr_without_experiments"))
    elif kind == "video_ctr_people_without_experiments":
        video_ctr_people_without_experiments.VideoCtrPeopleWithoutExperiments(start_time, end_time, country_code, "video_ctr_people_without_experiments", logger).compute_data("{}/SQL/{}.sql".format(DIR, "video_ctr_people_without_experiments"))
    elif kind == "video_watch_average_of_duration_without_experiments":
        video_watch_average_of_duration_without_experiments.VideoWatchAverageOfDurationWithoutExperiments(start_time, end_time, country_code, "video_watch_average_of_duration_without_experiments", logger).compute_data("{}/SQL/{}.sql".format(DIR, "video_watch_average_of_duration_without_experiments"))
    elif kind == "user_total_duration_average":
        user_total_duration_average.UserTotalDurationAverage(start_time, end_time, country_code, "user_total_duration_average", logger).compute_data("{}/SQL/{}.sql".format(DIR, "user_total_duration_average"))
    elif kind == "user_avg_cost":
        user_avg_cost.UserAvgCostOut(start_time, "user_avg_cost", logger).compute_data("{}/SQL/{}.sql".format(DIR, "user_avg_cost"))
    elif kind == "short_video_ctr":
        short_video_ctr.ShortVideoCtr(start_time, end_time, country_code, "short_video_ctr", logger).compute_data("{}/SQL/{}.sql".format(DIR, "short_video_ctr"))
    elif kind == "short_new_video_ctr":
        short_new_video_ctr.ShortNewVideoCtr(start_time, end_time, country_code, "short_new_video_ctr", logger).compute_data("{}/SQL/{}.sql".format(DIR, "short_new_video_ctr"))
    elif kind == "short_video_completion_rate":
        short_video_completion_rate.ShortVideoCompletionRate(start_time, end_time, country_code, "short_video_completion_rate", logger).compute_data("{}/SQL/{}.sql".format(DIR, "short_video_completion_rate"))
    elif kind == "short_new_video_completion_rate":
        short_new_video_completion_rate.ShortNewVideoCompletionRate(start_time, end_time, country_code, "short_new_video_completion_rate", logger).compute_data("{}/SQL/{}.sql".format(DIR, "short_new_video_completion_rate"))
    elif kind == "one_minute_video_ctr":    
        one_minute_video_ctr.OneMinuteVideoCtr(start_time, end_time, country_code, "one_minute_video_ctr", logger).compute_data("{}/SQL/{}.sql".format(DIR, "one_minute_video_ctr"))
    elif kind == "one_minute_new_video_ctr":
        one_minute_new_video_ctr.OneMinuteNewVideoCtr(start_time, end_time, country_code, "one_minute_new_video_ctr", logger).compute_data("{}/SQL/{}.sql".format(DIR, "one_minute_new_video_ctr"))
    elif kind == "one_minute_video_completion_rate":
        one_minute_video_completion_rate.OneMinuteVideoCompletionRate(start_time, end_time, country_code, "one_minute_video_completion_rate", logger).compute_data("{}/SQL/{}.sql".format(DIR, "one_minute_video_completion_rate"))
    elif kind == "one_minute_new_video_completion_rate":
        one_minute_new_video_completion_rate.OneMinuteNewVideoCompletionRate(start_time, end_time, country_code, "one_minute_new_video_completion_rate", logger).compute_data("{}/SQL/{}.sql".format(DIR, "one_minute_new_video_completion_rate"))
    elif kind == "five_minutes_video_ctr":
        five_minutes_video_ctr.FiveMinutesVideoCtr(start_time, end_time, country_code, "five_minutes_video_ctr", logger).compute_data("{}/SQL/{}.sql".format(DIR, "five_minutes_video_ctr"))
    elif kind == "five_minutes_new_video_ctr":
        five_minutes_new_video_ctr.FiveMinutesNewVideoCtr(start_time, end_time, country_code, "five_minutes_new_video_ctr", logger).compute_data("{}/SQL/{}.sql".format(DIR, "five_minutes_new_video_ctr"))
    elif kind == "five_minutes_video_completion_rate":
        five_minutes_video_completion_rate.FiveMinutesVideoCompletionRate(start_time, end_time, country_code, "five_minutes_video_completion_rate", logger).compute_data("{}/SQL/{}.sql".format(DIR, "five_minutes_video_completion_rate"))
    elif kind == "five_minutes_new_video_completion_rate":
        five_minutes_new_video_completion_rate.FiveMinutesNewVideoCompletionRate(start_time, end_time, country_code, "five_minutes_new_video_completion_rate", logger).compute_data("{}/SQL/{}.sql".format(DIR, "five_minutes_new_video_completion_rate"))
    elif kind == "push_ctr_without_experiments":
        push_ctr_without_experiments.PushCtrWithoutExperiments(start_time, end_time, country_code, "push_ctr_without_experiments", logger).compute_data("{}/SQL/{}.sql".format(DIR, "push_ctr_without_experiments"))
    elif kind == "push_ctr_people_without_experiments":
        push_ctr_people_without_experiments.PushCtrPeopleWithoutExperiments(start_time, end_time, country_code, "push_ctr_people_without_experiments", logger).compute_data("{}/SQL/{}.sql".format(DIR, "push_ctr_people_without_experiments"))
    elif kind == "notification_video_ctr_without_experiments":
        notification_video_ctr_without_experiments.NotificationVideoCtrWithoutExperiments(start_time, end_time, country_code, "notification_video_ctr_without_experiments", logger).compute_data("{}/SQL/{}.sql".format(DIR, "notification_video_ctr_without_experiments"))
    elif kind == "notification_video_ctr_without_experiments_by_people":
        notification_video_ctr_without_experiments_by_people.NotificationVideoCtrWithoutExperimentsByPeople(start_time, end_time, country_code, "notification_video_ctr_without_experiments_by_people", logger).compute_data("{}/SQL/{}.sql".format(DIR, "notification_video_ctr_without_experiments_by_people"))
    elif kind == "notification_news_ctr_without_experiments":
        notification_news_ctr_without_experiments.NotificationNewsCtrWithoutExperiments(start_time, end_time, country_code, "notification_news_ctr_without_experiments", logger).compute_data("{}/SQL/{}.sql".format(DIR, "notification_news_ctr_without_experiments"))
    elif kind == "notification_news_ctr_without_experiments_by_people":
        notification_news_ctr_without_experiments_by_people.NotificationNewsCtrWithoutExperimentsByPeople(start_time, end_time, country_code, "notification_news_ctr_without_experiments_by_people", logger).compute_data("{}/SQL/{}.sql".format(DIR, "notification_news_ctr_without_experiments_by_people"))
    elif kind == "new_videos_ctr":    
        new_videos_ctr.NewVideosCtr(start_time, end_time, country_code, "new_videos_ctr", logger).compute_data("{}/SQL/{}.sql".format(DIR, "new_videos_ctr"))
    elif kind == "immersive_video_watch_average_recall_by_model":    
        immersive_video_watch_average_recall_by_model.ImmersiveVideoWatchAverageRecallByModel(start_time, end_time, country_code, "immersive_video_watch_average_recall_by_model", logger).compute_data("{}/SQL/{}.sql".format(DIR, "immersive_video_watch_average_recall_by_model"))
    elif kind == "immersive_video_watch_average_of_duration_recall_by_model":    
        immersive_video_watch_average_of_duration_recall_by_model.ImmersiveVideoWatchAverageOfDurationRecallByModel(start_time, end_time, country_code, "immersive_video_watch_average_of_duration_recall_by_model", logger).compute_data("{}/SQL/{}.sql".format(DIR, "immersive_video_watch_average_of_duration_recall_by_model"))
    elif kind == "immersive_retention_recall_by_model":    
        immersive_retention_recall_by_model.ImmersiveRetentionRecallByModel(start_time, end_time, country_code, "immersive_retention_recall_by_model", logger).compute_data("{}/SQL/{}.sql".format(DIR, "immersive_retention_recall_by_model"))
    elif kind == "immersive_video_watch_average_recall_by_bucket":    
        immersive_video_watch_average_recall_by_bucket.ImmersiveVideoWatchAverageRecallByBucket(start_time, end_time, country_code, "immersive_video_watch_average_recall_by_bucket", logger).compute_data("{}/SQL/{}.sql".format(DIR, "immersive_video_watch_average_recall_by_bucket"))
    elif kind == "immersive_video_watch_average_of_duration_recall_by_bucket":    
        immersive_video_watch_average_of_duration_recall_by_bucket.ImmersiveVideoWatchAverageOfDurationRecallByBucket(start_time, end_time, country_code, "immersive_video_watch_average_of_duration_recall_by_bucket", logger).compute_data("{}/SQL/{}.sql".format(DIR, "immersive_video_watch_average_of_duration_recall_by_bucket"))
    elif kind == "immersive_retention_recall_by_bucket":    
        immersive_retention_recall_by_bucket.ImmersiveRetentionRecallByBucket(start_time, end_time, country_code, "immersive_retention_recall_by_bucket", logger).compute_data("{}/SQL/{}.sql".format(DIR, "immersive_retention_recall_by_bucket"))
    elif kind == "immersive_video_watch_average_rough_rank_by_model":    
        immersive_video_watch_average_rough_rank_by_model.ImmersiveVideoWatchAverageRoughRankByModel(start_time, end_time, country_code, "immersive_video_watch_average_rough_rank_by_model", logger).compute_data("{}/SQL/{}.sql".format(DIR, "immersive_video_watch_average_rough_rank_by_model"))
    elif kind == "immersive_video_watch_average_of_duration_rough_rank_by_model":    
        immersive_video_watch_average_of_duration_rough_rank_by_model.ImmersiveVideoWatchAverageOfDurationRoughRankByModel(start_time, end_time, country_code, "immersive_video_watch_average_of_duration_rough_rank_by_model", logger).compute_data("{}/SQL/{}.sql".format(DIR, "immersive_video_watch_average_of_duration_rough_rank_by_model"))
    elif kind == "immersive_retention_rough_rank_by_model":    
        immersive_retention_rough_rank_by_model.ImmersiveRetentionRoughRankByModel(start_time, end_time, country_code, "immersive_retention_rough_rank_by_model", logger).compute_data("{}/SQL/{}.sql".format(DIR, "immersive_retention_rough_rank_by_model"))
    elif kind == "video_ctr_recall_by_model": 
        video_ctr_recall_by_model.VideoCtrRecallByModel(start_time, end_time, country_code, "video_ctr_recall_by_model", logger).compute_data("{}/SQL/{}.sql".format(DIR, "video_ctr_recall_by_model"))
    elif kind == "video_ctr_people_recall_by_model": 
        video_ctr_people_recall_by_model.VideoCtrPeopleRecallByModel(start_time, end_time, country_code, "video_ctr_people_recall_by_model", logger).compute_data("{}/SQL/{}.sql".format(DIR, "video_ctr_people_recall_by_model"))
    elif kind == "video_ctr_recall_by_bucket": 
        video_ctr_recall_by_bucket.VideoCtrRecallByBucket(start_time, end_time, country_code, "video_ctr_recall_by_bucket", logger).compute_data("{}/SQL/{}.sql".format(DIR, "video_ctr_recall_by_bucket"))
    elif kind == "video_ctr_people_recall_by_bucket": 
        video_ctr_people_recall_by_bucket.VideoCtrPeopleRecallByBucket(start_time, end_time, country_code, "video_ctr_people_recall_by_bucket", logger).compute_data("{}/SQL/{}.sql".format(DIR, "video_ctr_people_recall_by_bucket"))
    elif kind == "video_ctr_rough_rank_by_model": 
        video_ctr_rough_rank_by_model.VideoCtrRoughRankByModel(start_time, end_time, country_code, "video_ctr_rough_rank_by_model", logger).compute_data("{}/SQL/{}.sql".format(DIR, "video_ctr_rough_rank_by_model"))
    elif kind == "video_ctr_people_rough_rank_by_model": 
        video_ctr_people_rough_rank_by_model.VideoCtrPeopleRoughRankByModel(start_time, end_time, country_code, "video_ctr_people_rough_rank_by_model", logger).compute_data("{}/SQL/{}.sql".format(DIR, "video_ctr_people_rough_rank_by_model"))
    elif kind == "video_click_average_recall_by_model":
        video_click_average_recall_by_model.VideoClickAverageRecallByModel(start_time, end_time, country_code, "video_click_average_recall_by_model", logger).compute_data("{}/SQL/{}.sql".format(DIR, "video_click_average_recall_by_model"))
    elif kind == "video_click_average_recall_by_bucket":
        video_click_average_recall_by_bucket.VideoClickAverageRecallByBucket(start_time, end_time, country_code, "video_click_average_recall_by_bucket", logger).compute_data("{}/SQL/{}.sql".format(DIR, "video_click_average_recall_by_bucket"))
    elif kind == "video_click_average_rough_rank_by_model":
        video_click_average_rough_rank_by_model.VideoClickAverageRoughRankByModel(start_time, end_time, country_code, "video_click_average_rough_rank_by_model", logger).compute_data("{}/SQL/{}.sql".format(DIR, "video_click_average_rough_rank_by_model"))
    elif kind == "video_retention_recall_by_model":
        video_retention_recall_by_model.VideoRetentionRecallByModel(start_time, end_time, country_code, "video_retention_recall_by_model", logger).compute_data("{}/SQL/{}.sql".format(DIR, "video_retention_recall_by_model"))
    elif kind == "video_retention_recall_by_bucket":
        video_retention_recall_by_bucket.VideoRetentionRecallByBucket(start_time, end_time, country_code, "video_retention_recall_by_bucket", logger).compute_data("{}/SQL/{}.sql".format(DIR, "video_retention_recall_by_bucket"))
    elif kind == "video_retention_rough_rank_by_model":
        video_retention_rough_rank_by_model.VideoRetentionRoughRankByModel(start_time, end_time, country_code, "video_retention_rough_rank_by_model", logger).compute_data("{}/SQL/{}.sql".format(DIR, "video_retention_rough_rank_by_model"))
    elif kind == "thirty_seconds_new_user_app_open_retention":
        thirty_seconds_new_user_app_open_retention.ThirtySecondsNewUserAppOpenRetention(start_time, end_time, country_code, "thirty_seconds_new_user_app_open_retention", logger).compute_data("{}/SQL/{}.sql".format(DIR, "thirty_seconds_new_user_app_open_retention"))
    elif kind == "three_minutes_new_user_app_open_retention":
        three_minutes_new_user_app_open_retention.ThreeMinutesNewUserAppOpenRetention(start_time, end_time, country_code, "three_minutes_new_user_app_open_retention", logger).compute_data("{}/SQL/{}.sql".format(DIR, "three_minutes_new_user_app_open_retention"))
    elif kind == "five_minutes_new_user_app_open_retention":
        five_minutes_new_user_app_open_retention.FiveMinutesNewUserAppOpenRetention(start_time, end_time, country_code, "five_minutes_new_user_app_open_retention", logger).compute_data("{}/SQL/{}.sql".format(DIR, "five_minutes_new_user_app_open_retention"))
    elif kind == "ten_minutes_new_user_app_open_retention":
        ten_minutes_new_user_app_open_retention.TenMinutesNewUserAppOpenRetention(start_time, end_time, country_code, "ten_minutes_new_user_app_open_retention", logger).compute_data("{}/SQL/{}.sql".format(DIR, "ten_minutes_new_user_app_open_retention"))
    elif kind == "points_out_statistics":
        points_out_statistics.PointsOutStatistics(start_time, end_time, country_code, "points_out_statistics", logger).compute_data("{}/SQL/{}.sql".format(DIR, "points_out_statistics"))
    elif kind == "points_out_purpose_statistics":
        points_out_purpose_statistics.PointsOutPurposeStatistics(start_time, end_time, country_code, "points_out_purpose_statistics", logger).compute_data("{}/SQL/{}.sql".format(DIR, "points_out_purpose_statistics"))
    elif kind == "points_in_statistics":
        points_in_statistics.PointsInStatistics(start_time, end_time, country_code, "points_in_statistics", logger).compute_data("{}/SQL/{}.sql".format(DIR, "points_in_statistics"))
    elif kind == "new_user_invite_master":
         new_user_invite_master.NewUserInviteMaster(start_time, end_time, country_code, "new_user_invite_master", logger).compute_data("{}/SQL/{}.sql".format(DIR, "new_user_invite_master"))
    elif kind == "new_user_invite_master_with_referrals":
        new_user_invite_master_with_referrals.NewUserInviteMasterWithReferrals(start_time, end_time, country_code, "new_user_invite_master_with_referrals", logger).compute_data("{}/SQL/{}.sql".format(DIR, "new_user_invite_master_with_referrals"))
    elif kind == "new_user_invite_apprentice_retention":
        new_user_invite_apprentice_retention.NewUserInviteApprenticeRetention(start_time, end_time, country_code, "new_user_invite_apprentice_retention", logger).compute_data("{}/SQL/{}.sql".format(DIR, "new_user_invite_apprentice_retention"))
    elif kind == "new_user_retention_by_brand":
        new_user_retention_by_brand.NewUserRetentionByBrand(start_time, end_time, country_code, "new_user_retention_by_brand", logger).compute_data("{}/SQL/{}.sql".format(DIR, "new_user_retention_by_brand"))
    elif kind == "news_read_duration_avg":
        news_read_duration_avg.NewsReadDurationAvg(start_time, end_time, country_code, "news_read_duration_avg", logger).compute_data("{}/SQL/{}.sql".format(DIR, "news_read_duration_avg"))
    elif kind == "news_read_duration_avg_by_count":
        news_read_duration_avg_by_count.NewsReadDurationAvgByCount(start_time, end_time, country_code, "news_read_duration_avg_by_count", logger).compute_data("{}/SQL/{}.sql".format(DIR, "news_read_duration_avg_by_count"))
    elif kind == "video_watch_duration_avg":
        video_watch_duration_avg.VideoWatchDurationAvg(start_time, end_time, country_code, "video_watch_duration_avg", logger).compute_data("{}/SQL/{}.sql".format(DIR, "video_watch_duration_avg"))
    elif kind == "video_watch_duration_avg_by_count":
        video_watch_duration_avg_by_count.VideoWatchDurationAvgByCount(start_time, end_time, country_code, "video_watch_duration_avg_by_count", logger).compute_data("{}/SQL/{}.sql".format(DIR, "video_watch_duration_avg_by_count"))
    elif kind == "video_impression_duration_avg":
        video_impression_duration_avg.VideoImpressionDurationAvg(start_time, end_time, country_code, "video_impression_duration_avg", logger).compute_data("{}/SQL/{}.sql".format(DIR, "video_impression_duration_avg"))
    elif kind == "job_query_log_cost":
        job_query_log_cost.JobQueryLogCost(start_time, end_time, "job_query_log_cost", logger).compute_data("{}/SQL/{}.sql".format(DIR, "job_query_log_cost"))
    elif kind == "page_dau_and_penetration":
        page_dau_and_penetration.PageDauAndPenetration(start_time, end_time, country_code, "page_dau_and_penetration", logger).compute_data("{}/SQL/{}.sql".format(DIR, "page_dau_and_penetration"))
    elif kind == "page_avg_time":
        page_avg_time.PageAvgTime(start_time, end_time, country_code, "page_avg_time", logger).compute_data("{}/SQL/{}.sql".format(DIR, "page_avg_time"))
    elif kind == "page_retention":
        page_retention.PageRetention(start_time, end_time, country_code, "page_retention", logger).compute_data("{}/SQL/{}.sql".format(DIR, "page_retention"))
    elif kind == "image_ctr_by_placement":
        image_ctr_by_placement.ImageCtrByPlacement(start_time, end_time, country_code,  "image_ctr_by_placement", logger).compute_data("{}/SQL/{}.sql".format(DIR, "image_ctr_by_placement"))
    elif kind == "video_ctr_by_placement":
        video_ctr_by_placement.VideoCtrByPlacement(start_time, end_time, country_code,  "video_ctr_by_placement", logger).compute_data("{}/SQL/{}.sql".format(DIR, "video_ctr_by_placement"))
    elif kind == "silient_user":
        silient_user.SilentUser(start_time, end_time, country_code,  "silient_user", logger).compute_data("{}/SQL/{}.sql".format(DIR, "silient_user"))
    elif kind == "accounts_without_ad_click":
        accounts_without_ad_click.AccountsWithoutAdClick(start_time, end_time, country_code,  "accounts_without_ad_click", logger).compute_data("{}/SQL/{}.sql".format(DIR, "accounts_without_ad_click"))
    else:
        pass

    
    if buzzbreak_bigquery_client:
        buzzbreak_bigquery_client.close()
    buzzbreak_mysql_client.close_client()
    logger.info("{} complete!".format(datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")))


