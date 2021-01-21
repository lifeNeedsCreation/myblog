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
from indicator_scripts import new_users_channels_retention

# 指标列表
KIND = {
    "all": 1,   # 所有指标
    "different_channels_pr": 1,   # 不同channel渗透率
    "new_users_channels_average_of_duration": 1,    # 新用户不同channel的平均时长
    "new_users_channels_retention": 1,  # 新用户不同channel的留存
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
    channel = "'" + "','".join(constants.KATKAT_VIDEO_CHANNEL) + "'"
    if kind == "all":
        different_channels_pr.DifferentChannelsPRData(start_time, end_time, channel, "different_channels_pr", logger).compute_data()

        new_users_channels_average_of_duration.NewUsersChannelsAverageOfDuration(start_time, end_time, channel, "new_users_channels_average_of_duration", logger).compute_data()

        new_users_channels_retention.NewUsersChannelsRetention(start_time, end_time, channel, "new_users_channels_retention", logger).compute_data()

    elif kind == "different_channels_pr":
        different_channels_pr.DifferentChannelsPRData(start_time, end_time, channel, "different_channels_pr", logger).compute_data()
    elif kind == "new_users_channels_average_of_duration":
        new_users_channels_average_of_duration.NewUsersChannelsAverageOfDuration(start_time, end_time, channel, "new_users_channels_average_of_duration", logger).compute_data()
    elif kind == "new_users_channels_retention":
        new_users_channels_retention.NewUsersChannelsRetention(start_time, end_time, channel, "new_users_channels_retention", logger).compute_data()
    else:
        pass

    if katkat_bigquery_client:
        katkat_bigquery_client.close()
    if katkat_mysql_client:
        katkat_mysql_client.close()
    logger.info("{} complete!".format(datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")))


