#!/usr/bin/env python
# -*- coding: utf-8 -*-
# @module ctr_notification
# @author: Teddy
# @description: 计算新用户新闻推送点击率
# @since: 2020-12-23 16:58:13
# @version: Python3.7.4

from utils.bigquery import bigquery_client
from utils.mysql import mysql_client
import datetime


class NewsCtrNotificationNewUserData(object):
    """
    :param start_time: 指标计算的开始时间
    :param end_time: 指标计算的结束时间
    :param country_code: 需要计算的国家
    :param indicator_dimension: 需要计算的实验组的维度
    :param table_name: 计算结果存的表
    return 
    """
    # 构造函数，初始化数据
    def __init__(self, start_time, end_time, country_code, indicator_dimension, table_name):
        self.start_time = start_time
        self.end_time = end_time
        self.country_code = country_code 
        self.indicator_dimension = indicator_dimension
        self.table_name = table_name

    # 查询 BigQuery，并解析组装数据
    def get_data(self, sql):
        """
        : param sql: 拼接好的sql语句
        : return: 字典，筛选条件为key，统计的用户数量为value
        """
        res_num = {}
        bq_job = bigquery_client.query(sql).to_dataframe()
        for index, row in bq_job.iterrows():
            treatment_name = row["treatment_name"]
            country_code = row["country_code"]
            dimension = row["dimension"]
            num = row["num"]
            if treatment_name and country_code and dimension:
                res_num[treatment_name + "&&" + country_code + "&&" + dimension] =num
        print("==========================")
        print("res_num", res_num)
        return res_num

    # 组装查询 sql，并统计计算结果存入 mysql
    def compute_data(self):
        """
        """
        start_time = self.start_time.strftime("%Y-%m-%d %H:%M:%S")
        end_time = self.end_time.strftime("%Y-%m-%d %H:%M:%S")

        # 点击推送的用户统计
        click_sql = f"""
                    select result.key as treatment_name, result.country_code as country_code, result.value as dimension, count(result.account_id) as num from
                    (select a.account_id as account_id, a.created_at as created_at, memories.key as key, memories.value as value, a.country_code as country_code from 
                    (select notification_click.account_id as account_id, notification_click.created_at as created_at, accounts.country_code as country_code from 
                    (select * from buzzbreak-model-240306.stream_events.notification_click as click where click.created_at >= '{start_time}' and click.created_at < '{end_time}'  and JSON_VAlUE(data, '$.type') = 'news' and JSON_VALUE(data, '$.push_id') like 'push%') as notification_click  
                    LEFT JOIN buzzbreak-model-240306.input.accounts as accounts on accounts.id = notification_click.account_id where accounts.name is not null and accounts.country_code in ({self.country_code})
                    and accounts.created_at >= '{start_time}' and accounts.created_at < '{end_time}' and JSON_VALUE(data, '$.type') = 'news') as a    
                    LEFT JOIN (select account_id, key, value, updated_at from buzzbreak-model-240306.partiko.memories where key like 'experiment%' and value in ({self.indicator_dimension})) as memories
                    on memories.account_id = a.account_id
                    where key is not null) as result 
                    group by result.key, result.country_code, result.value
                    """
        click_data = self.get_data(click_sql)

        # 收到推送的用户统计
        received_sql = f"""
                      select result.key as treatment_name, result.country_code as country_code, result.value as dimension, count(result.account_id) as num from 
                      (select a.account_id as account_id, a.created_at as created_at, memories.key as key, memories.value as value, a.country_code as country_code from 
                      (select notification_received.account_id as account_id, notification_received.created_at as created_at, accounts.country_code as country_code from 
                      (select * from buzzbreak-model-240306.stream_events.notification_received as received where received.created_at >= '{start_time}' and received.created_at < '{end_time}' and JSON_VAlUE(data, '$.type') = 'news' and JSON_VALUE(data, '$.push_id') like 'push%') as notification_received 
                      LEFT JOIN buzzbreak-model-240306.input.accounts as accounts on accounts.id = notification_received.account_id where accounts.name is not null and accounts.country_code in ({self.country_code}) and accounts.created_at >= '{start_time}' and accounts.created_at < '{end_time}') as a 
                      LEFT JOIN (select account_id, key, value, updated_at from buzzbreak-model-240306.partiko.memories where key like 'experiment%' and value in ({self.indicator_dimension})) as memories on memories.account_id = a.account_id where key is not null) as result 
                      group by result.key, result.country_code, result.value  
                      """
        received_data = self.get_data(received_sql)

        ## 结果存入数据库
        cursor = mysql_client.cursor()
        insert_sql = "INSERT INTO " + self.table_name + "(treatment_name, country_code, dimension, ctr, start_time, end_time, create_time) VALUES"
        now_time_utc = datetime.datetime.utcnow()
        # sql 执行标识
        flag = False
        # 构造 sql
        for key in received_data.keys():
            click_num = click_data.get(key, 0)
            print("==========================")
            print("click_num", click_num)
            received_num = received_data.get(key, 0)
            print("==========================")
            print("received_num", received_num)
            
            if received_num < 0:
                continue
            temp_data = key.split("&&")
            print("==========================")
            print("temp_data", temp_data)
            if len(temp_data) < 4:
                continue
            # 拼接 sql values
            values_sql = "('" + temp_data[0] + "','" + temp_data[1] + "','" + temp_data[2] + "','" + str(round(click_num/received_num, 5)) + "','" + start_time + "','" + end_time + "','" + now_time_utc.strftime("%Y-%m-%d %H:%M:%S") + "'),"
            print("==========================")
            print("values_sql", values_sql)
            insert_sql += values_sql
            flag = True

        print("==========================")
        print("insert_sql", insert_sql)
        if flag:
            insert_sql = insert_sql[:len(insert_sql)-1]
            try:
                # 执行sql语句
                cursor.execute(insert_sql)
                # 提交到数据库执行
                mysql_client.commit()
                print("正确执行sql")
            except:
                # 如果发生错误则回滚
                mysql_client.rollback()
                print("错误回滚sql")


        if cursor:
            cursor.close()
