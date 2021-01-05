import os
import sys
import getopt
import datetime

BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.append(BASE_DIR)

from utils import constants
from utils.bigquery import bigquery_client
from utils.mysql import mysql_client
from utils.logger import Logger
from upload_scripts import ctr
from upload_scripts import ctr_people
from upload_scripts import news_ctr_notification_new_user
from upload_scripts import news_ctr_notification_old_user
from upload_scripts import video_ctr_notification_new_user
from upload_scripts import video_ctr_notification_old_user
from upload_scripts import new_user_news_click_average
from upload_scripts import new_user_video_watch_average
from upload_scripts import video_ctr
from upload_scripts import video_ctr_people
from upload_scripts import new_users_retention_news_event
from upload_scripts import new_users_retention_tab_impression
from upload_scripts import new_users_events_retention
from upload_scripts import old_users_events_retention
from upload_scripts import experiment_new_users_retention_tab_impression
from upload_scripts import partiko_memories_new_users_events_retention
from upload_scripts import partiko_memories_old_users_events_retention
from upload_scripts import partiko_experiment_average_of_invites
from upload_scripts import partiko_memories_average_of_invites
from upload_scripts import new_users_partiko_memories_average_of_invites
from upload_scripts import partiko_memories_user_time_average_of_duration
from upload_scripts import new_user_news_ctr_people
from upload_scripts import new_user_video_ctr_people
from upload_scripts import push_retention
from upload_scripts import partiko_experiment_new_users_retention_tab_impression

# 指标列表
KIND = {
    "all": 1,   # 所有指标
    "ctr": 1,   # 新闻ctr
    "ctr_people": 1,  # 新闻 click_user_ratio
    "news_ctr_notification_new_user": 1,  # 新用户新闻推送点击率
    "news_ctr_notification_old_user": 1,  # 老用户新闻推送点击率
    "video_ctr_notification_new_user": 1,   # 新用户视频推送点击率
    "video_ctr_notification_old_user": 1,   # 新用户视频推送点击率
    "new_user_news_click_average": 1,    # 新用户新闻平均点击率
    "new_user_video_watch_average": 1,      # 新用户视频平均观看率
    "new_users_retention_news_event": 1,   # 新闻用户留存率
    "video_ctr": 1,   # 视频ctr
    "video_ctr_people": 1,   # 视频 click_user_ratio
    "new_users_retention_tab_impression": 1,    # tab_impression 新用户留存
    "new_users_events_retention": 1,    # 新用户在app_open与tab_impression下的留存
    "old_users_events_retention": 1,    # app_open与tab_impression 老用户留存
    "experiment_new_users_retention_tab_impression": 1,     # tab_impression 实验中新用户留存
    "partiko_memories_new_users_events_retention": 1,       # app_open与tab_impression 实验中新用户留存
    "partiko_memories_old_users_events_retention": 1,       # app_open与tab_impression 实验中老用户留存
    "partiko_memories_average_of_invites": 1,     # partiko.memories 实验中的 平均邀请人数
    "new_users_partiko_memories_average_of_invites": 1,     # partiko.memories 实验中的 新用户平均邀请人数
    "partiko_experiment_average_of_invites": 1,     # partiko.experiment 实验中的 平均邀请人数
    "partiko_memories_user_time_average_of_duration": 1,     # partiko.memories 实验中 用户在各个页面的停留时间
    "new_user_news_ctr_people": 1,  # 新用户 新闻 click_user_ratio
    "new_user_video_ctr_people": 1,  # 新用户 视频 click_user_ratio
    "push_tention": 1,
    "partiko_experiment_new_users_retention_tab_impression": 1,     # partiko.experiment 实验中 新用户在各个 tab 的留存
}


# 周期：天

if __name__ == "__main__":
    logger = Logger("Main Day", os.path.join(BASE_DIR, 'logs/main_day.log'))
    print(datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S") + "  start!")
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
    placement = "'" + "','".join(constants.PLACEMENT) + "'"
    video_placement = "'" + "','".join(constants.VIDEO_PLACEMENT) + "'"
    indicator_dimension = "'" + "','".join(constants.INDICATOR_DIMENSION) + "'"
    if kind == "all":
        ctr.CTRData(start_time, end_time, country_code, placement, indicator_dimension, "day_news_ctr", logger).compute_data()

        ctr_people.CTRPeopleData(start_time, end_time, country_code, placement, indicator_dimension, "day_news_ctr_people", logger).compute_data()

        news_ctr_notification_new_user.NewsCtrNotificationNewUserData(start_time, end_time, country_code, indicator_dimension, "day_news_ctr_notification_new_user", logger).compute_data()

        news_ctr_notification_old_user.NewsCtrNotificationOldUserData(start_time, end_time, country_code, indicator_dimension, "day_news_ctr_notification_old_user", logger).compute_data()
        
        video_ctr_notification_new_user.VideoCtrNotificationNewUserData(start_time, end_time, country_code, indicator_dimension, "day_video_ctr_notification_new_user", logger).compute_data()

        video_ctr_notification_old_user.VideoCtrNotificationOldUserData(start_time, end_time, country_code, indicator_dimension, "day_video_ctr_notification_old_user", logger).compute_data()

        new_user_news_click_average.NewUserNewsClickAverageData(start_time, end_time, country_code, placement, indicator_dimension, "day_new_user_news_click_average", logger).compute_data()

        new_user_video_watch_average.NewUserVideoWatchAverageData(start_time, end_time, country_code, video_placement, indicator_dimension, "day_new_user_video_watch_average", logger).compute_data()

        new_users_retention_news_event.NewUsersRetentionNewsEvent(start_time, end_time, country_code, indicator_dimension, "new_users_retention_news_event", logger).compute_data()

        video_ctr.VideoCTRData(start_time, end_time, country_code, video_placement, indicator_dimension, "day_video_ctr", logger).compute_data()

        video_ctr_people.VideoCTRPeopleData(start_time, end_time, country_code, video_placement, indicator_dimension, "day_video_ctr_people", logger).compute_data()

        new_users_retention_tab_impression.NewUsersRetentionTabImpression(start_time, end_time, 'new_users_retention_tab_impression', logger).compute_data()

        experiment_new_users_retention_tab_impression.ExperimentNewUsersRetentionTabImpression(start_time, end_time, indicator_dimension, 'experiment_new_users_retention_tab_impression', logger).compute_data()

        new_users_events_retention.NewUsersEventsRetention(start_time, end_time, 'new_users_events_retention', logger).compute_data()
        
        partiko_memories_new_users_events_retention.PartikoMemoriesNewUsersEventsRetention(start_time, end_time, indicator_dimension, 'partiko_memories_new_users_events_retention', logger).compute_data()

        partiko_experiment_average_of_invites.PartikoExperimentAverageOfInvites(start_time, end_time, indicator_dimension, 'partiko_experiment_average_of_invites', logger).compute_data()
        
        partiko_memories_user_time_average_of_duration.PartikoMemoriesUserTimeAverageOfDuration(start_time, end_time, indicator_dimension, 'partiko_memories_user_time_average_of_duration', logger).compute_data()

        new_user_news_ctr_people.NewUserCTRPeopleData(start_time, end_time, country_code, placement, indicator_dimension, "day_new_user_news_ctr_people", logger).compute_data()

        new_user_video_ctr_people.NewUserVideoCTRPeopleData(start_time, end_time, country_code, video_placement, indicator_dimension, "day_new_user_video_ctr_people", logger).compute_data()

        partiko_memories_average_of_invites.PartikoMemoriesAverageOfInvites(start_time, end_time, indicator_dimension, 'partiko_memories_average_of_invites', logger).compute_data()

        new_users_partiko_memories_average_of_invites.NewUsersPartikoMemoriesAverageOfInvites(start_time, end_time, indicator_dimension, 'new_users_partiko_memories_average_of_invites', logger).compute_data()

        partiko_experiment_new_users_retention_tab_impression.PartikoExperimentNewUsersRetentionTabImpression(start_time, end_time, indicator_dimension, 'partiko_experiment_new_users_retention_tab_impression', logger).compute_data()

        old_users_events_retention.OldUsersEventsRetention(start_time, end_time, 'old_users_events_retention', logger).compute_data()

        partiko_memories_old_users_events_retention.PartikoMemoriesOldUsersRetentionTabImpression(start_time, end_time, indicator_dimension, 'partiko_memories_old_users_events_retention', logger).compute_data()

    elif kind == "ctr":
        ctr.CTRData(start_time, end_time, country_code, placement, indicator_dimension, "day_news_ctr", logger).compute_data()
    elif kind == "ctr_people":
        ctr_people.CTRPeopleData(start_time, end_time, country_code, placement, indicator_dimension, "day_news_ctr_people", logger).compute_data()
    elif kind == "news_ctr_notification_new_user":
        news_ctr_notification_new_user.NewsCtrNotificationNewUserData(start_time, end_time, country_code, indicator_dimension, "day_news_ctr_notification_new_user", logger).compute_data()
    elif kind == "news_ctr_notification_old_user":
        news_ctr_notification_old_user.NewsCtrNotificationOldUserData(start_time, end_time, country_code, indicator_dimension, "day_news_ctr_notification_old_user", logger).compute_data()
    elif kind == "video_ctr_notification_new_user":
        video_ctr_notification_new_user.VideoCtrNotificationNewUserData(start_time, end_time, country_code, indicator_dimension, "day_video_ctr_notification_new_user", logger).compute_data()
    elif kind == "video_ctr_notification_old_user":
        video_ctr_notification_old_user.VideoCtrNotificationOldUserData(start_time, end_time, country_code, indicator_dimension, "day_video_ctr_notification_old_user", logger).compute_data()
    elif kind == "new_user_news_click_average":
        new_user_news_click_average.NewUserNewsClickAverageData(start_time, end_time, country_code, placement, indicator_dimension, "day_new_user_news_click_average", logger).compute_data()
    elif kind == "new_user_video_watch_average":
        new_user_video_watch_average.NewUserVideoWatchAverageData(start_time, end_time, country_code, video_placement, indicator_dimension, "day_new_user_video_watch_average", logger).compute_data()
    elif kind == "new_users_retention_news_event":
        new_users_retention_news_event.NewUsersRetentionNewsEvent(start_time, end_time, country_code, indicator_dimension, "new_users_retention_news_event", logger).compute_data()
    elif kind == "video_ctr":
        video_ctr.VideoCTRData(start_time, end_time, country_code, video_placement, indicator_dimension, "day_video_ctr", loggger).compute_data()
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
    elif kind == 'partiko_memories_old_users_events_retention':
        partiko_memories_old_users_events_retention.PartikoMemoriesOldUsersRetentionTabImpression(start_time, end_time, indicator_dimension, 'partiko_memories_old_users_events_retention', logger).compute_data()
    elif kind == 'partiko_memories_average_of_invites':
        partiko_memories_average_of_invites.PartikoMemoriesAverageOfInvites(start_time, end_time, indicator_dimension, 'partiko_memories_average_of_invites', logger).compute_data()
    elif kind == 'new_users_partiko_memories_average_of_invites':
        new_users_partiko_memories_average_of_invites.NewUsersPartikoMemoriesAverageOfInvites(start_time, end_time, indicator_dimension, 'new_users_partiko_memories_average_of_invites', logger).compute_data()
    elif kind == 'partiko_experiment_average_of_invites':
        partiko_experiment_average_of_invites.PartikoExperimentAverageOfInvites(start_time, end_time, indicator_dimension, 'partiko_experiment_average_of_invites', logger).compute_data()
    elif kind == 'partiko_memories_user_time_average_of_duration':
        partiko_memories_user_time_average_of_duration.PartikoMemoriesUserTimeAverageOfDuration(start_time, end_time, indicator_dimension, 'partiko_memories_user_time_average_of_duration', logger).compute_data()
    elif kind == "new_user_news_ctr_people":
        new_user_news_ctr_people.NewUserCTRPeopleData(start_time, end_time, country_code, placement, indicator_dimension, "day_new_user_news_ctr_people", logger).compute_data()
    elif kind == "new_user_video_ctr_people":
        new_user_video_ctr_people.NewUserVideoCTRPeopleData(start_time, end_time, country_code, video_placement, indicator_dimension, "day_new_user_video_ctr_people", logger).compute_data()
    elif kind == "push_tention":
        push_retention.PushRetentionData(start_time, end_time, country_code, indicator_dimension, "push_tention", logger).compute_data()
    elif kind == 'partiko_experiment_new_users_retention_tab_impression':
        partiko_experiment_new_users_retention_tab_impression.PartikoExperimentNewUsersRetentionTabImpression(start_time, end_time, indicator_dimension, 'partiko_experiment_new_users_retention_tab_impression', logger).compute_data()
    else:
        pass

    if bigquery_client:
        bigquery_client.close()
    if mysql_client:
        mysql_client.close()
    print(datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S") + "  complete!")


