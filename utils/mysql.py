import os, sys
import pymysql

BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.append(BASE_DIR + "/utils")

from read_config import CONFIG


class MySQL(object):

    def __init__(self, section):
        self.host = CONFIG[section].get("URI")
        self.port = int(CONFIG[section].get("PORT"))
        self.user = CONFIG[section].get("USER")
        self.password = CONFIG[section].get("PASSWORD")
        self.db = CONFIG[section].get("DATABASE")

    def get_client(self):
        return pymysql.connect(host=self.host, user=self.user, password=self.password, db=self.db, port=self.port)

buzzbreak_mysql_client = MySQL("MYSQL_BUZZBREAK").get_client()
katkat_mysql_client = MySQL("MYSQL_KATKAT").get_client()


