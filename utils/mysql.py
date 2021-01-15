import os, sys
import pymysql

BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.append(BASE_DIR + "/utils")

from read_config import CONFIG


class MySQL(object):

    def __init__(self, config):
        self.host = config.get("URI")
        self.port = config.get("PORT")
        self.user = config.get("USER")
        self.password = config.get("PASSWORD")
        self.db = config.get("DATABASE")

    def get_client(self):
        return pymysql.connect(host=self.host, user=self.user, password=self.password, db=self.db, port=self.port)

buzzbreak_mysql_client = MySQL(CONFIG["MYSQL_BUZZBREAK"]).get_client()
katkat_mysql_client = MySQL(CONFIG["MYSQL_KATKAT"]).get_client()


