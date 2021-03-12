import os, sys
import pymysql
import datetime

BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
DIR = os.path.dirname(os.path.abspath(__file__))
sys.path.append(DIR)
sys.path.append(BASE_DIR + "/utils")

from read_config import CONFIG
from logger import Logger
from constants import *


class MySQL(object):
    def __init__(self, section):
        # 初始化
        """
        param : section，指连接哪个APP的数据库
        """
        self.host = CONFIG[section].get("URI")
        self.port = int(CONFIG[section].get("PORT"))
        self.user = CONFIG[section].get("USER")
        self.password = CONFIG[section].get("PASSWORD")
        self.db = CONFIG[section].get("DATABASE")
        self.logger = Logger(section, os.path.join(DIR, "logs/{}_mysql.log".format(section)))
        self.client = self.get_client()
        self.cursor = self.client.cursor()

    def get_client(self):
        # 连接数据库
        return pymysql.connect(host=self.host, user=self.user, password=self.password, db=self.db, port=self.port)

    def execute_sql(self, sql):
        # 执行SQL语句
        """
        param : sql, 指要执行的sq语句
        """
        try:
            self.cursor.execute(sql)
            self.client.commit()
            #self.logger.info("execute sql {} success".format(sql))
        except:
            self.logger.exception("execute sql {} fail".format(sql))
            self.client.rollback()

    def insert_dict(self, table_name, dict):
        # 插入新的记录
        """
        param : table_name，指要插入具体的表
        param : dict，指要插入的数据，每个key为一个big query表名，value 为对应的表的更新时间
        """
        fields = "(table_name, updated_at, create_time)"
        insert_sql = "INSERT INTO {} {} VALUES ".format(table_name, fields)
        values_sql = ""
        now_time = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        for table_name, updated_at in dict.items():
            values_sql += "('" + table_name + "', '" + updated_at.strftime("%Y-%m-%d %H:%M:%S") + "', '" + now_time + "'),"
        insert_sql += values_sql[:-1]
        self.execute_sql(insert_sql)
            

    def close_cursor(self):
        # 关闭游标
        if self.cursor:
            self.cursor.close()

    def close_client(self):
        # 关闭客户端
        if self.client:
            self.client.close()


buzzbreak_mysql_client = MySQL("MYSQL_BUZZBREAK")
katkat_mysql_client = MySQL("MYSQL_KATKAT")