import datetime
from utils.bigquery import buzzbreak_bigquery_client
from utils.mysql import buzzbreak_mysql_client
from utils.utils import read_sql


class VideosCtrCategoryHomepage:
    # 构造函数， 初始化数据
    """
        country_code: 指视频的上传国家
        placement：指视频不同的位置
        table_name：计算结果存的表
    """

    def __init__(self, country_code, placement, table_name, logger=None):
        self.country_code = country_code
        self.placement = placement
        self.table_name = table_name
        self.logger = logger
        self.fields = ["country_code", "category", "placement", "homepage", "ctr", "num"]

    # 查询bigquery，并解析组装数据
    def get_data(self, sql):
        df_result = buzzbreak_bigquery_client.query(sql).to_dataframe()
        dict_info = {field: [] for field in self.fields}
        for index, row in df_result.iterrows():
            for field in self.fields:
                dict_info[field].append(row[field])
        return dict_info

    # 组装查询sql，并将统计计算结果存入mysql
    def compute_data(self, path):
        sql = read_sql(path)
        params = {"country_code": self.country_code, "placement": self.placement}
        query = sql.format(**params)
        ctr = self.get_data(query)
        if ctr[self.fields[0]]:
            # 结果数据存入数据库
            values = ""
            for field in self.fields:
                values += field + ", "
            values += "create_time"
            insert_sql = f"INSERT INTO {self.table_name} ({values}) VALUES "
            now_time_utc = datetime.datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S")
            for i in range(len(ctr[self.fields[0]])):
                insert_sql += "("
                for field in self.fields:
                    insert_sql += f"'{ctr[field][i]}', "
                insert_sql += f"'{now_time_utc}'),"
            insert_sql = insert_sql[:-1]
            buzzbreak_mysql_client.execute_sql(insert_sql)
        else:
            self.logger.info("insert tabel {} fail due to query result is empty".format(self.table_name))