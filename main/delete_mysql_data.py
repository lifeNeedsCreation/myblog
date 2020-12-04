
import sys
import getopt
import datetime
import os
BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.append(BASE_DIR)
from utils.mysql import mysql_client


# 定时删除mysql库中数据
if __name__ == "__main__":
    print(datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S") + "  delete data start!")
    argv = sys.argv[1:]
    params_msg = "params: [-h] [--help] [-d] [--day]"
    if len(argv) <= 0:
        print(params_msg)
        print("option  参数不能为空，请输入相关参数！")
        sys.exit()
    try:
        opts, args = getopt.getopt(argv, "hd:", ["help", "day="])
    except getopt.GetoptError as e:
        print(params_msg)
        print(e.msg)
        sys.exit(2)
    # 处理 返回值options是以元组为元素的列表。
    days = None
    for opt, arg in opts:
        if opt in ("-h", "--help"):
            print(params_msg)
            print("-h / --help:  帮助")
            print("-d / --day: 需要保留的数据的天数")
            sys.exit()
        elif opt in ("-d", "--day"):
            try:
                days = int(arg)
            except ValueError:
                print(params_msg)
                print("option -d / --day:该参数错误，请查看 -h / --help")
                sys.exit(2)
        else:
            pass
    # 设置缺省值
    if not days:
        days = 60

    if mysql_client:
        mysql_client.close()
    print(datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S") + " delete data complete!")


