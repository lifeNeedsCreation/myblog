from utils.bigquery import bigquery_client
from utils.mysql import mysql_client
import datetime


class NewsClickCtrNewUserData(object):
    """
    :param start_time: 指标计算的开始时间
    :param end_time: 指标计算的结束时间
    :param country_code: 需要计算的国家
    :param placement: app上需要计算的位置
    :param indicator_dimension: 需要计算的实验组的维度
    :param table_name: 计算结果存的表
    return 
    """
    # 构造函数，初始化数据
    def __init__(self, start_time, end_time, country_code, placement, indicator_dimension, table_name):
        self.start_time = start_time
        self.end_time = end_time
        self.country_code = country_code
        self.placement = placement
        self.indicator_dimension = indicator_dimension
        self.table_name = table_name

    # 查询 BigQuery，并解析组装数据
    def get_data(self, sql):
        """
        : param sql : sql语句
        : return result_num : 字典，{'country_code&&placement&&key&&value': num}
        """
        result = bigquery_client.query(sql).to_dataframe()
        print(result)
        result_num = {}
        for index, row in result.iterrows():
            country_code = row['country_code']
            placement = row['placement']
            key = row['key']
            value = row['value']
            num = row['num']
            if placement and country_code and key and value:
                result_num[country_code + "&&" + placement + "&&" + key + "&&" + value] = num
        return result_num

    # 组装查询 sql，并将统计计算结果存入 mysql
    def compute_data(self):
        """
        : param :
        : return :
        """
        start_time = self.start_time.strftime("%Y-%m-%d %H:%M:%S")
        end_time = self.end_time.strftime("%Y-%m-%d %H:%M:%S")
        # 新用户新闻点击数据
        news_click_sql = \
            f"""
            select result.country_code as country_code, result.placement as placement, result.key as key, result.value as value, count(result.news_id) as num from
            (select distinct account_news_click.account_id as account_id, account_news_click.news_id as news_id, account_news_click.country_code as country_code, account_news_click.placement as placement, memories.key as key, memories.value as value
            from 
            (select account_id, country_code, news_click.created_at as created_at, news_click.placement as placement, news_click.news_id as news_id from
            (select account_id, created_at, json_extract_scalar(data, '$.placement') as placement, json_extract_scalar(data, '$.news_post.id') as news_id from `stream_events.news_click` as news_click
            where news_click.created_at > '{start_time}' and news_click.created_at < '{end_time}' and json_extract_scalar(data, '$.placement') in ("home_tab_for_you", 'news_detail_activity')) as news_click
            left join input.accounts as accounts on accounts.id = news_click.account_id where accounts.created_at > '{start_time}' and accounts.created_at < '{end_time}' and accounts.name is not null and accounts.country_code in ({self.country_code})
            ) as account_news_click
            left join 
            (select account_id, key, value, updated_at from partiko.memories where key like 'experiment%' and value in ({self.indicator_dimension})) as memories on memories.account_id = account_news_click.account_id where key is not null and memories.updated_at <= account_news_click.created_at) as result
            group by country_code, placement, key, value
            """
        news_click_data = self.get_data(news_click_sql)

        # 新用户新闻曝光数据
        impression_sql = \
            f"""
            select result.country_code as country_code, result.placement as placement, result.key as key, result.value as value, count(result.account_id) as num from
            (select distinct account_news_impression.account_id as account_id, account_news_impression.country_code as country_code, account_news_impression.placement as placement, memories.key as key, memories.value as value from 
            (select account_id, country_code, news_impression.created_at as created_at, news_impression.placement as placement from
            (select account_id, created_at, json_extract_scalar(data, '$.placement') as placement from `stream_events.news_impression` as news_impression where news_impression.created_at > '{start_time}' and news_impression.created_at < '{end_time}' and json_extract_scalar(data, '$.placement') in ("home_tab_for_you", 'news_detail_activity')) as news_impression
            left join input.accounts as accounts on accounts.id = news_impression.account_id where accounts.created_at > '{start_time}' and accounts.created_at < '{end_time}' and accounts.name is not null and accounts.country_code in ({self.country_code})) as account_news_impression
            left join 
            (select account_id, key, value, updated_at from partiko.memories where key like 'experiment%' and value in ({self.indicator_dimension})) as memories on memories.account_id = account_news_impression.account_id where key is not null and memories.updated_at <= account_news_impression.created_at) as result
            group by country_code, placement, key, value
            """
        impression_data = self.get_data(impression_sql)

        # 非曝光情况下用户点击数据
        impression_union_sql = \
            f"""
            select country_code as country_code, placement as placement, key as key, value as value, count(account_id) as num from
            (select distinct account_news_click.account_id as account_id, account_news_click.country_code as country_code, account_news_click.placement as placement, memories.key as key, memories.value as value from 
            (select account_id, country_code, news_click.created_at as created_at, news_click.placement as placement from
            (select account_id, created_at, json_extract_scalar(data, '$.placement') as placement from `stream_events.news_click` as news_click where news_click.created_at > '{start_time}' and news_click.created_at < '{end_time}' and json_extract_scalar(data, '$.placement') in ("home_tab_for_you", 'news_detail_activity')) as news_click
            left join input.accounts as accounts on accounts.id = news_click.account_id where accounts.created_at > '{start_time}' and accounts.created_at < '{end_time}' and accounts.name is not null and accounts.country_code in ({self.country_code})) as account_news_click
            left join 
            (select account_id, key, value, updated_at from partiko.memories where key like 'experiment%' and value in ({self.indicator_dimension})) as memories on memories.account_id = account_news_click.account_id where key is not null and memories.updated_at <= account_news_click.created_at 

            union distinct

            select distinct account_news_impression.account_id as account_id, account_news_impression.country_code as country_code, account_news_impression.placement as placement, memories.key as key, memories.value as value from 
            (select account_id, country_code, news_impression.created_at as created_at, news_impression.placement as placement from
            (select account_id, created_at, json_extract_scalar(data, '$.placement') as placement from `stream_events.news_impression` as news_impression where news_impression.created_at > '{start_time}' and news_impression.created_at < '{end_time}' and json_extract_scalar(data, '$.placement') in ("home_tab_for_you", 'news_detail_activity')) as news_impression
            left join input.accounts as accounts on accounts.id = news_impression.account_id where accounts.created_at > '{start_time}' and accounts.created_at < '{end_time}' and accounts.name is not null and accounts.country_code in ({self.country_code})) as account_news_impression
            left join 
            (select account_id, key, value, updated_at from partiko.memories where key like 'experiment%' and value in ({self.indicator_dimension})) as memories on memories.account_id = account_news_impression.account_id where key is not null and memories.updated_at <= account_news_impression.created_at)
            group by country_code, placement, key, value
            """
        impression_union_data = self.get_data(impression_union_sql)

        # 结果存入数据库
        cursor = mysql_client.cursor()
        values = "country_code, placement, treatment_name, dimension, click_num, impression_num, impression_union_num, ctr, ctr_union, start_time, end_time, create_time"
        insert_sql = f"INSERT INTO {self.table_name} ({values}) VALUES "
        now_time_utc = datetime.datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S")
        flag = False
        for key in impression_union_data.keys():
            click_num = news_click_data.get(key, 0)
            impression_num = impression_data.get(key, 0)
            if impression_num <= 0:
                continue
            impression_union_num = impression_union_data.get(key, 0)
            if impression_union_num <= 0:
                continue
            temp_data = key.split("&&")
            if len(temp_data) < 4:
                continue
            values_sql = "('" + temp_data[0] + "','" + temp_data[1] + "','" + temp_data[2] + "','" + temp_data[3] + "','" + str(click_num) + "','" + str(impression_num) + "','" + str(impression_union_num) + "','" + str(round(click_num/impression_num, 5)) + "','" + str(round(click_num/impression_union_num, 5)) + "','" + start_time + "','" + end_time + "','" + now_time_utc + "'),"
            insert_sql += values_sql
            flag = True
        if flag:
            insert_sql = insert_sql[:-1]
            try:
                # 执行 sql 语句
                cursor.execute(insert_sql)
                # 提交到数据库执行
                mysql_client.commit()
            except Exception as e:
                # 如果发生错误则回滚
                print("写入Mysql失败，错误信息：", e)
                mysql_client.rollback()
        if cursor:
            cursor.close()




                
