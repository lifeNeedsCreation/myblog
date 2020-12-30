#!/usr/bin/env python
# -*- coding: utf-8 -*-
# @module partiko_experiment_old_users_retention_tab_impression
# @author: Mr.In
# @description: 
# @since: 2020-12-26 15:15:30
# @version: Python3.7.4


import datetime
from utils.bigquery import bigquery_client
from utils.mysql import mysql_client


class PartikoMemoriesOldUsersRetentionTabImpression(object):
    """
    : param start_time: 指标计算的开始时间
    : param end_time：指标计算的结束时间
    : param indicator_dimension: 需要计算实验组的维度
    : param table_name：计算结果存的表
    """
    # 构造函数，初始化数据
    def __init__(self, start_time, end_time, indicator_dimension, table_name):
        self.start_time = start_time
        self.end_time = end_time
        self.indicator_dimension = indicator_dimension
        self.table_name = table_name

    # 查询 BigQuery，并解析组装数据
    def get_data(self, sql):
        result = bigquery_client.query(sql).to_dataframe()
        print(result)
        fields = [
            'key',
            'value',
            'country_code',
            'initial_date',
            'retention_date',
            'initial_event',
            'retention_event',
            'date_diff',
            'initial_users',
            'retention_users',
            'retention_rate',
        ]
        dict_info = {field: [] for field in fields}
        for index, row in result.iterrows():
            for field in fields:
                dict_info[field].append(row[field])
        return dict_info

    # 组装查询 sql，并将统计计算结果存入 mysql
    def compute_data(self):
        start_time = self.start_time.strftime("%Y-%m-%d %H:%M:%S")
        end_time = self.end_time.strftime("%Y-%m-%d %H:%M:%S")
        fields = [
            'key',
            'value',
            'country_code',
            'initial_date',
            'retention_date',
            'initial_event',
            'retention_event',
            'date_diff',
            'initial_users',
            'retention_users',
            'retention_rate',
        ]
        query = \
            f'''
            with
            accounts as (select * from input.accounts where name is not null),
            account_profiles as (select * from partiko.account_profiles where mac_address is not null),
            memories as (select * from partiko.memories where key like 'experiment_%' and value in ({self.indicator_dimension}) and updated_at > timestamp_sub(timestamp'{start_time}', interval 30 day) and updated_at < '{end_time}'),
            app_open as (select * from stream_events.app_open where created_at > timestamp_sub(timestamp'{start_time}', interval 30 day) and created_at < '{end_time}'),
            tab_impression as (select * from stream_events.tab_impression where created_at > timestamp_sub(timestamp'{start_time}', interval 30 day) and created_at < '{end_time}'),
            account as (select distinct id,country_code,key,value,extract(date from updated_at) as created_date from (select id,country_code, created_at from (select distinct id, country_code, created_at from accounts) inner join (select distinct account_id from (select mac_address, min(created_at) as created_at from account_profiles group by mac_address) as a inner join (select account_id, mac_address, created_at from account_profiles) as b on a.mac_address = b.mac_address and a.created_at = b.created_at) on id = account_id) inner join (select distinct account_id, key, value, updated_at from memories) on id = account_id and extract(date from created_at) = extract(date from updated_at)),
            events_target_time as (select * from ((select distinct account_id,extract(date from created_at) as date,json_extract_scalar(data,'$.tab') as event from tab_impression where created_at > '{start_time}' and created_at < '{end_time}') union all (select distinct account_id,extract(date from created_at) as date,'app_open' as event from app_open where created_at > '{start_time}' and created_at < '{end_time}'))),
            events_one_month as (select * from ((select distinct account_id,extract(date from created_at) as date,json_extract_scalar(data,'$.tab') as event from tab_impression) union all (select distinct account_id,extract(date from created_at) as date,'app_open' as event from app_open))),
            initial_events as (select distinct id, country_code, key, value, date as initial_date, event from account inner join events_one_month on id = account_id where created_date < date),
            retention_events as (select distinct id,country_code,key,value,initial_date,date as retention_date,initial_events.event as initial_event,events_target_time.event as retention_event,date_diff(date,initial_date,day) as date_diff from initial_events inner join events_target_time on id = account_id),
            initial_event_count as (select count(distinct id) as initial_users,country_code,key,value,initial_date,event as initial_event from initial_events group by country_code, initial_date, initial_event, key, value),
            retention_event_count as (select count(distinct id) as retention_users,country_code, key, value,initial_date,retention_date,initial_event,retention_event, date_diff from retention_events group by country_code,initial_date, retention_date, initial_event, retention_event, date_diff, key, value)
            select i.country_code, i.initial_date, i.key, i.value, retention_date, date_diff,i.initial_event, retention_event, initial_users, ifnull(retention_users, 0) as retention_users, round(ifnull(retention_users, 0)/initial_users, 4) as retention_rate from initial_event_count as i left join retention_event_count as r on i.country_code = r.country_code and i.initial_date = r.initial_date and i.initial_event = r.initial_event and i.key = r.key and i.value = r.value where date_diff is not null
            '''
        retention_data = self.get_data(query)
        # 结果数据存入数据库
        cursor = mysql_client.cursor()
        values = "treatment_name, dimension, country_code, initial_date, retention_date, initial_event, retention_event, date_diff, initial_users, retention_users, retention_rate, create_time"
        insert_sql = f"INSERT INTO {self.table_name} ({values}) VALUES "
        now_time_utc = datetime.datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S")
        for i in range(len(retention_data['country_code'])):
            insert_sql += "("
            for filed in fields:
                insert_sql += f"""'{retention_data[filed][i]}', """
            insert_sql += f"""'{now_time_utc}'),"""
        insert_sql = insert_sql[:-1]
        try:
            # 执行 sql 语句
            cursor.execute(insert_sql)
            # 提交到数据库
            mysql_client.commit()
        except Exception as e:
            # 如果发生错误则回滚
            print("错误信息：", e)
            mysql_client.rollback()
        if cursor:
            cursor.close()