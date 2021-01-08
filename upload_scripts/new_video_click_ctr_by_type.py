import datetime
from utils.bigquery import bigquery_client
from utils.mysql import mysql_client


class NewUsersRetentionNewsEvent:
    # 构造函数， 初始化数据
    """
        start_time:指标计算的开始时间
        end_time：指标计算的结束时间
        country_code：需要计算的国家
        indicator_dimension：需要计算的实验组的维度
        table_name：计算结果存的表
    """

    def __init__(self, start_time, end_time, video_kind_placement, table_name, logger=None):
        self.start_time = start_time
        self.end_time = end_time
        self.video_kind_placement = video_kind_placement
        self.table_name = table_name
        self.logger = logger

    # 查询bigquery，并解析组装数据
    def get_data(self, sql):
        result = bigquery_client.query(sql).to_dataframe()
        fields = [
            'category',
            'placement',
            'num'
        ]
        dict_info = {field: [] for field in fields}
        for index, row in result.iterrows():
            for field in fields:
                dict_info[field].append(row[field])
        return dict_info

    # 组装查询sql，并将统计计算结果存入mysql
    def compute_data(self):
        start_time = self.start_time.strftime("%Y-%m-%d %H:%M:%S")
        end_time = self.end_time.strftime("%Y-%m-%d %H:%M:%S")
        click_sql = \
            f'''
            with
            videos as (select * from partiko.videos where created_at between timestamp_sub(timestamp'{start_time}', interval 2 day) and timestamp_sub(timestamp'{end_time}', interval 2 day)),

            video_click as (select json_extract_scalar(data, '$.id') as video_id, json_extract_scalar(data, '$.placement') as placement from `stream_events.video_click` where created_at between '{start_time}' and '{end_time}' and json_extract_scalar(data, '$.placement') in ({self.video_kind_placement})),

            select vc.placement, videos.category, count(*) as num from video_click as vc inner join videos on safe_cast(vc.video_id as numeric) = videos.id group by videos.category 
            '''
        click_data = self.get_data(click_sql)

        impression_sql = \
            f'''
            with
            videos as (select * from partiko.videos where created_at between timestamp_sub(timestamp'{start_time}', interval 2 day) and timestamp_sub(timestamp'{end_time}', interval 2 day)),

            video_impression as (select json_extract_scalar(data, '$.id') as video_id, json_extract_scalar(data, '$.placement') as placement from `stream_events.video_impression` where created_at between '{start_time}' and '{end_time}' and json_extract_scalar(data, '$.placement') in ({self.video_kind_placement})),

            select vc.placement, videos.category, count(*) as num from video_impression as vi inner join videos on safe_cast(vi.video_id as numeric) = videos.id group by vi.placement, videos.category 
            '''
        impression_data = self.get_data(impression_sql)

        # 结果数据存入数据库
        cursor = mysql_client.cursor()
        values = "category, placement, click_num, impression_num, ctr, create_time"
        insert_sql = f"INSERT INTO {self.table_name} ({values}) VALUES"
        now_time_utc = datetime.datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S")
        # sql 执行标识
        flag = False
        # 构造 sql
        for key in impression_data.keys():
            click_num = click_data.get(key, 0)
            impression_num = impression_data.get(key, 0)
            if impression_num < 0:
                continue
            temp_data = key.split("&&")
            if len(temp_data) < 3:
                continue
            # 拼接 sql values
            values_sql = "('" + temp_data[0] + "','" + temp_data[1] + "','" + temp_data[2] + "'," + str(click_num) + "," + str(impression_num) + "," + str(round(click_num/impression_num, 5)) + ",'" + start_time + "','" + end_time + "','" + now_time_utc.strftime("%Y-%m-%d %H:%M:%S") + "'),"
            insert_sql += values_sql
            flag = True

        if flag:
            insert_sql = insert_sql[:len(insert_sql)-1]
            try:
                # 执行sql语句
                cursor.execute(insert_sql)
                # 提交到数据库执行
                mysql_client.commit()
                self.logger.info("start_time={}, end_time={} insert tabel {} success".format(self.start_time, self.end_time, self.table_name))
            except:
                self.logger.exception("start_time={}, end_time={} insert tabel {} err msg".format(self.start_time, self.end_time, self.table_name))
                # 如果发生错误则回滚
                mysql_client.rollback()

        if cursor:
            cursor.close()
