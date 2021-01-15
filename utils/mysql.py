import os, sys
import pymysql

BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.append(BASE_DIR + "/utils")

from read_config import CONFIG


class MySQL(object):

    def __init__(self, host, port, user, password, db):
        self.host = host
        self.port = port
        self.user = user
        self.password = password
        self.db = db

    def get_client(self):
        return pymysql.connect(host=self.host, user=self.user, password=self.password, db=self.db, port=self.port)


buzzbreak_mysql_client = MySQL(CONFIG["MYSQL_BUZZBREAK"].get("URI"), CONFIG["MYSQL_BUZZBREAK"].get("PORT"), CONFIG["MYSQL_BUZZBREAK"].get("USER"), CONFIG["MYSQL_BUZZBREAK"].get("PASSWORD"), CONFIG["MYSQL_BUZZBREAK"].get("DATABASE")).get_client()
katkat_mysql_client = MySQL(CONFIG["MYSQL_KATKAT"].get("URI"), CONFIG["MYSQL_KATKAT"].get("PORT"), CONFIG["MYSQL_KATKAT"].get("USER"), CONFIG["MYSQL_KATKAT"].get("PASSWORD"), CONFIG["MYSQL_KATKAT"].get("DATABASE")).get_client()


