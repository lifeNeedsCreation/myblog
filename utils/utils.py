import datetime

# 读取SQL文件
def read_sql(path):
    with open(path, "r") as f:
        sql = f.read()
    f.close()
    return sql

def get_previous_start_time(time):
    # 获取time昨天起始时间
    previous_start_time = time - datetime.timedelta(days=1, hours=time.hour, minutes=time.minute, seconds=time.second, microseconds=time.microsecond)
    return previous_start_time

def get_today_start_time(time):
    # 获取time起始时间
    today_start_time = time - datetime.timedelta(hours=time.hour, minutes=time.minute, seconds=time.second, microseconds=time.microsecond)
    return today_start_time

def getRestSeconds(time):
    # 获取time离第二天凌晨的时间间隔
    today_begin = datetime.datetime(time.year, time.month, time.day, 0, 0, 0)
    tomorrow_begin = today_begin + datetime.timedelta(days=1)
    rest_seconds = (tomorrow_begin - time).seconds
    return rest_seconds
    
