# 读取SQL文件
def read_sql(path):
    with open(path, "r") as f:
        sql = f.read()
    f.close()
    return sql