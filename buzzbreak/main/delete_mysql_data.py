
import sys
import getopt
import datetime
import os

BASE_DIR = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.append(BASE_DIR)
sys.path.append(DIR)

from utils.mysql import buzzbreak_mysql_client
from utils.logger import Logger


# 定时删除mysql库中数据
if __name__ == "__main__":
    logger = Logger("Delete BuzzBreak Tables Data", os.path.join(DIR, 'logs/delete_buzzbreak_tables_data.log'))
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
    query_sql = "show tables from indicator_data"
    cursor = buzzbreak_mysql_client.cursor()
    table_list = list()
    try:
        # 执行sql语句
        tables_num = cursor.execute(query_sql)
        for i in cursor.fetchall():
            table_list.append(i[0])
    except Exception as e:
        print("mysql operate except")
        print(e)
    now_time_utc = datetime.datetime.utcnow()
    limit_time = now_time_utc - datetime.timedelta(days=days, hours=now_time_utc.hour, minutes=now_time_utc.minute, seconds=now_time_utc.second, microseconds=now_time_utc.microsecond)
    for table_name in table_list:
        sql_str = "DELETE FROM " + table_name + " where create_time<'" + limit_time.strftime("%Y-%m-%d %H:%M:%S") + "'"
        try:
            cursor.execute(sql_str)
            buzzbreak_mysql_client.commit()
            logger.info("limit_time={} delete buzzbreak tabel {} success".format(limit_time, table_name))
        except Exception as e:
            logger.exception("limit_time={} delete buzzbreak tabel {} err msg".format(limit_time, table_name))
            print(sql_str)
            print("delete table:" + table_name + " data,operate except")
            print(e)
    if cursor:
        cursor.close()
    if buzzbreak_mysql_client:
        buzzbreak_mysql_client.close()
    print(datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S") + " delete data complete!")


