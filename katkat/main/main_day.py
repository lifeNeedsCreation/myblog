import os
import sys
import getopt
import datetime

BASE_DIR = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.append(BASE_DIR)
sys.path.append(DIR)

from utils import constants
from utils.bigquery import katkat_bigquery_client
from utils.mysql import katkat_mysql_client
from utils.logger import Logger
from indicator_scripts import different_channels_pr
from indicator_scripts import new_users_channels_average_of_duration
from indicator_scripts import new_users_video_watch_different_placement_average_of_duration
from indicator_scripts import new_users_video_watch_average
from indicator_scripts import new_users_channels_retention
from indicator_scripts import new_users_same_channels_retention
from indicator_scripts import cash_out
from indicator_scripts import cashout_by_method
from indicator_scripts import all_users_ad_impression_avg
from indicator_scripts import all_users_video_watch_average
from indicator_scripts import all_users_video_watch_average_of_duration
from indicator_scripts import ad_video_impression_ratio
from indicator_scripts import total_ad_video_impression_ratio
from indicator_scripts import posts
from indicator_scripts import user_total_duration_average
from indicator_scripts import user_avg_cost


# 指标列表
KIND = {
    "all": 1,   # 所有指标
    "different_channels_pr": 1,   # 不同channel渗透率
    "new_users_channels_average_of_duration": 1,    # 新用户不同channel的平均时长
    "new_users_video_watch_different_placement_average_of_duration": 1, # 新用户不同placement的平均观看时长
    "new_users_video_watch_average": 1, # 新用户视频不同位置平均观看次数
    "new_users_channels_retention": 1,  # 新用户不同channel的留存
    "new_users_same_channels_retention": 1,  # 新用户相同channel的留存
    "cash_out": 1,  # 统计打钱，按国家和天
    "cashout_by_method": 1,   # 各国家打钱统计(按打钱方式)
    "all_users_ad_impression_avg": 1,     # 所有用户不同位置广告的平均曝光次数
    "all_users_video_watch_average": 1, # 所有用户视频不同位置平均观看次数
    "all_users_video_watch_average_of_duration": 1, # 所有用户不同位置的平均观看时长
    "ad_video_impression_ratio": 1, # 沉浸流广告与视频曝光比率
    "total_ad_video_impression_ratio": 1, # 广告与视频曝光总比率
    "posts": 1,     # 发帖数量统计
    "user_total_duration_average": 1,   # 各国家用户平均使用app时间
    "user_avg_cost": 1,     # 用户平均成本
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
    channel = "'" + "','".join(constants.KATKAT_VIDEO_CHANNEL) + "'"
    video_placement = "'" + "','".join(constants.KATKAT_VIDEO_PLACEMENT) + "'"
    ad_placement = "'" + "','".join(constants.KATKAT_AD_PLACEMENT) + "'"
    if kind == "all":
        different_channels_pr.DifferentChannelsPRData(start_time, end_time, channel, "different_channels_pr", logger).compute_data()

        new_users_channels_average_of_duration.NewUsersChannelsAverageOfDuration(start_time, end_time, channel, "new_users_channels_average_of_duration", logger).compute_data("{}/SQL/{}.sql".format(DIR, "new_users_channels_average_of_duration"))

        new_users_video_watch_different_placement_average_of_duration.NewUsersVideoWatchDifferentPlacementsAverageOfDuration(start_time, end_time, video_placement, "new_users_video_watch_different_placement_average_of_duration", logger).compute_data("{}/SQL/{}.sql".format(DIR, "new_users_video_watch_different_placement_average_of_duration"))

        new_users_video_watch_average.NewUsersVideoWatchAverage(start_time, end_time, video_placement, "new_users_video_watch_average", logger).compute_data("{}/SQL/{}.sql".format(DIR, "new_users_video_watch_average"))

        new_users_channels_retention.NewUsersChannelsRetention(start_time, end_time, channel, "new_users_channels_retention", logger).compute_data("{}/SQL/{}.sql".format(DIR, "new_users_channels_retention"))

        new_users_same_channels_retention.NewUsersSameChannelsRetention(start_time, end_time, channel, "new_users_same_channels_retention", logger).compute_data("{}/SQL/{}.sql".format(DIR, "new_users_same_channels_retention"))

        cash_out.CashOut(start_time, end_time, "cash_out", logger).compute_data("{}/SQL/{}.sql".format(DIR, "cash_out"))

        cashout_by_method.CashOutByMethod(start_time, end_time, "cashout_by_method", logger).compute_data("{}/SQL/{}.sql".format(DIR, "cashout_by_method"))

        all_users_ad_impression_avg.AllUsersAdImpressionAvg(start_time, end_time, ad_placement, "all_users_ad_impression_avg", logger).compute_data("{}/SQL/{}.sql".format(DIR, "all_users_ad_impression_avg"))

        all_users_video_watch_average.AllUsersVideoWatchAverage(start_time, end_time, video_placement, "all_users_video_watch_average", logger).compute_data("{}/SQL/{}.sql".format(DIR, "all_users_video_watch_average"))

        all_users_video_watch_average_of_duration.AllUsersVideoWatchAverageOfDuration(start_time, end_time, video_placement, "all_users_video_watch_average_of_duration", logger).compute_data("{}/SQL/{}.sql".format(DIR, "all_users_video_watch_average_of_duration"))

        ad_video_impression_ratio.AdVideoImpressionRatio(start_time, end_time, country_code, "ad_video_impression_ratio", logger).compute_data("{}/SQL/{}.sql".format(DIR, "ad_video_impression_ratio"))

        total_ad_video_impression_ratio.TotalAdVideoImpressionRatio(start_time, end_time, country_code, "total_ad_video_impression_ratio", logger).compute_data("{}/SQL/{}.sql".format(DIR, "total_ad_video_impression_ratio"))
        
        posts.Posts(start_time, end_time, country_code, "posts", logger).compute_data("{}/SQL/{}.sql".format(DIR, "posts"))

        user_total_duration_average.UserTotalDurationAverage(start_time, end_time, country_code, "user_total_duration_average", logger).compute_data("{}/SQL/{}.sql".format(DIR, "user_total_duration_average"))

        user_avg_cost.UserAvgCostOut(start_time, end_time, "user_avg_cost", logger).compute_data("{}/SQL/{}.sql".format(DIR, "user_avg_cost"))


    elif kind == "different_channels_pr":
        different_channels_pr.DifferentChannelsPRData(start_time, end_time, channel, "different_channels_pr", logger).compute_data()
    elif kind == "new_users_channels_average_of_duration":
        new_users_channels_average_of_duration.NewUsersChannelsAverageOfDuration(start_time, end_time, channel, "new_users_channels_average_of_duration", logger).compute_data("{}/SQL/{}.sql".format(DIR, "new_users_channels_average_of_duration"))
    elif kind == "new_users_video_watch_different_placement_average_of_duration":
        new_users_video_watch_different_placement_average_of_duration.NewUsersVideoWatchDifferentPlacementsAverageOfDuration(start_time, end_time, video_placement, "new_users_video_watch_different_placement_average_of_duration", logger).compute_data("{}/SQL/{}.sql".format(DIR, "new_users_video_watch_different_placement_average_of_duration"))
    elif kind == "new_users_video_watch_average":
        new_users_video_watch_average.NewUsersVideoWatchAverage(start_time, end_time, video_placement, "new_users_video_watch_average", logger).compute_data("{}/SQL/{}.sql".format(DIR, "new_users_video_watch_average"))
    elif kind == "new_users_channels_retention":
        new_users_channels_retention.NewUsersChannelsRetention(start_time, end_time, channel, "new_users_channels_retention", logger).compute_data("{}/SQL/{}.sql".format(DIR, "new_users_channels_retention"))
    elif kind == "new_users_same_channels_retention":
        new_users_same_channels_retention.NewUsersSameChannelsRetention(start_time, end_time, channel, "new_users_same_channels_retention", logger).compute_data("{}/SQL/{}.sql".format(DIR, "new_users_same_channels_retention"))
    elif kind == "cash_out":
        cash_out.CashOut(start_time, end_time, "cash_out", logger).compute_data("{}/SQL/{}.sql".format(DIR, "cash_out"))
    elif kind == "cashout_by_method":
        cashout_by_method.CashOutByMethod(start_time, end_time, "cashout_by_method", logger).compute_data("{}/SQL/{}.sql".format(DIR, "cashout_by_method"))
    elif kind == "all_users_ad_impression_avg":
        all_users_ad_impression_avg.AllUsersAdImpressionAvg(start_time, end_time, ad_placement, "all_users_ad_impression_avg", logger).compute_data("{}/SQL/{}.sql".format(DIR, "all_users_ad_impression_avg"))
    elif kind == "all_users_video_watch_average":
        all_users_video_watch_average.AllUsersVideoWatchAverage(start_time, end_time, video_placement, "all_users_video_watch_average", logger).compute_data("{}/SQL/{}.sql".format(DIR, "all_users_video_watch_average"))
    elif kind == "all_users_video_watch_average_of_duration":
        all_users_video_watch_average_of_duration.AllUsersVideoWatchAverageOfDuration(start_time, end_time, video_placement, "all_users_video_watch_average_of_duration", logger).compute_data("{}/SQL/{}.sql".format(DIR, "all_users_video_watch_average_of_duration"))
    elif kind == "ad_video_impression_ratio":
        ad_video_impression_ratio.AdVideoImpressionRatio(start_time, end_time, country_code, "ad_video_impression_ratio", logger).compute_data("{}/SQL/{}.sql".format(DIR, "ad_video_impression_ratio"))
    elif kind == "total_ad_video_impression_ratio":
        total_ad_video_impression_ratio.TotalAdVideoImpressionRatio(start_time, end_time, country_code, "total_ad_video_impression_ratio", logger).compute_data("{}/SQL/{}.sql".format(DIR, "total_ad_video_impression_ratio"))
    elif kind == "posts":
        posts.Posts(start_time, end_time, country_code, "posts", logger).compute_data("{}/SQL/{}.sql".format(DIR, "posts"))
    elif kind == "user_total_duration_average":
        user_total_duration_average.UserTotalDurationAverage(start_time, end_time, country_code, "user_total_duration_average", logger).compute_data("{}/SQL/{}.sql".format(DIR, "user_total_duration_average"))
    elif kind == "user_avg_cost":
        user_avg_cost.UserAvgCostOut(start_time, end_time, "user_avg_cost", logger).compute_data("{}/SQL/{}.sql".format(DIR, "user_avg_cost"))
    else:
        pass

    if katkat_bigquery_client:
        katkat_bigquery_client.close()
    katkat_mysql_client.close_client()
    logger.info("{} complete!".format(datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")))


