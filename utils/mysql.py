import pymysql


class MySQL(object):

    def __init__(self, host, port, user, password, db):
        self.host = host
        self.port = port
        self.user = user
        self.password = password
        self.db = db

    def get_client(self):
        return pymysql.connect(host=self.host, user=self.user, password=self.password, db=self.db, port=self.port)


mysql_client = MySQL("10.45.66.36", 3306, "root", "buzz2020", "indicator_data").get_client()

