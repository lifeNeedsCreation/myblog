import os
import sys
import getopt
import datetime

DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
BASE_DIR = os.path.dirname(DIR)
sys.path.append(DIR)
sys.path.append(BASE_DIR)

from utils.logger import Logger
from utils.mysql import katkat_mysql_client
from utils.constants import MYSQL_SUCCESS, MYSQL_EXCEPTION, ACTION_SUCCESS, ACTION_FAIL

class IndicatorDeadLine():
    """
    指标的结束时间的查询、插入及更新操作
    """
    def __init__(self, logger=None):
        self.logger = logger
        self.cursor = katkat_mysql_client.cursor()

    # 执行SQL语句
    def execute_sql(self, action, sql):
        try:
            self.cursor.execute(sql)
            katkat_mysql_client.commit()
            self.logger.info("Action: {} execute SQL: {} \n succuss".format(action, sql))
            result = {"msg": "{} success".format(action), "code": MYSQL_SUCCESS}
        except:
            self.logger.exception("Action: {} execute SQL: {} \n fail with err msg".format(action, sql))
            katkat_mysql_client.rollback()
            result = {"msg": "{} failed".format(action), "code": MYSQL_EXCEPTION}
        return result       

    # 关闭连接
    def close_cursor(self):
        if self.cursor:
            self.cursor.close()

    # 查询
    def query(self, indicator_name):
            query_sql = "SELECT indicator_name, dead_line FROM indicator_dead_line WHERE indicator_name = '{}'".format(indicator_name)
            res = self.execute_sql("query", query_sql)
            if res["code"] == MYSQL_SUCCESS:
                result = self.cursor.fetchone()
                self.logger.info(res["msg"])
                result = {"query_result": result, "code": ACTION_SUCCESS}
            else:
                self.logger.info(res["msg"])
                result = {"query_result": None, "code": ACTION_FAIL}
            return result

    # 插入
    def insert(self, indicator_name, dead_line):
        now_time_utc = datetime.datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S")
        res = self.query(indicator_name)
        if res["code"] == ACTION_SUCCESS:
            self.logger.info("The {} has been set, please checkout!".format(indicator_name))
            result = {"msg": "insert failed: {} has been set".format(indicator_name), "code": ACTION_FAIL}
        insert_sql = "INSERT INTO {} VALUES ({}, {}, {})".format("indicator_dead_line", indicator_name, dead_line, now_time_utc)
        res = self.execute_sql("insert", insert_sql)
        if res["code"] == MYSQL_SUCCESS:
            result = {"msg": res["msg"], "code": ACTION_SUCCESS}
        else:
            result = {"msg": res["msg"], "code": ACTION_FAIL}
        return result

    # 更新
    def update(self, indicator_name, new_dead_line):
        res = self.query(indicator_name)
        if res["code"] == ACTION_FAIL:
            self.logger.info("The {} has not been set, please checkout!".format(indicator_name))
            result = {"msg": "update failed: {} has not been set".format(indicator_name), "code": ACTION_FAIL}
        else:
            update_sql = "UPDATE indicator_dead_line SET dead_line = {} WHERE indicator_name = {}".format(new_dead_line, indicator_name)
            res_exe_sql = self.execute_sql("update", update_sql)
            if res["code"] == MYSQL_SUCCESS:
                result = {"msg": res["msg"], "code": ACTION_SUCCESS}
            else:
                result = {"msg": res["msg"], "code": ACTION_FAIL}
        return result


if __name__ == "__main__":
    argv = sys.argv[1:]
    params_msg = "params: [-h] [--help] [-i] [--indicator_name] [-d] [--dead_line]"
    if len(argv) <= 0:
        print(params_msg)
        print("option  参数不能为空，请输入相关参数!")
        sys.exit(2)
    try:
        opts, args = getopt.getopt(argv, "hi:d", ["help", "indicator_name=", "dead_line="])
    except getopt.GetoptError as e:
        print(params_msg)
        print(e.msg)
        sys.exit(2)
    for opt, arg in opts:
        if opt in ("-h", "--help"):
            print(params_msg)
            print("-h / --help: 帮助")
            print("-i / --indicator_name: 需要插入或更新的指标名称")
            print("-d / --dead_line: 指标计算的结束时间")
        elif opt in ("-i", "--indicator_name"):
            indicator_name = arg
        elif opt in ("-d", "--dead_line"):
            try:
                dead_line = datetime.datetime.strptime(arg, "%Y-%m-%d %H:%M:%S")
            except ValueError:
                print(params_msg)
                print("args -d / --dead_line: 该参数格式错误，请查看 -h / --help")
                sys.exit(2)
    now_time_utc = datetime.datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S")
    choice = int(input("please choose 1 to update dead_line, choose 2 to insert a new indicator with dead_line and choose 3 to query a indicator"))
    logger = Logger("Indicator Dead Line", os.path.join(DIR, "logs/indicator_dead_line.log"))
    indicator = IndicatorDeadLine(logger)
    if choice == 1:
        res = indicator.update(indicator_name, dead_line)
        print(res["msg"])
        sys.exit(2)
    elif choice == 2:
        res = indicator.insert(indicator_name, dead_line)
        print(res["msg"])
    elif choice == 3:
        res = indicator.query(indicator_name)
        if res:
            print("{} has not been set, please checkout!".format(indicator_name))
        else:
            print(res)
    sys.exit(2)



        