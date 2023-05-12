from psycopg2 import pool

# 创建连接池
dbpool = pool.SimpleConnectionPool(
    minconn=50,
    maxconn=100,
    host="43.136.114.133",
    database="stock_data",
    user="postgres",
    password="guilin@2022",
    options="-c search_path=cn_stock",
)


def execute(sql: str):
    # 从连接池中获取连接
    conn = dbpool.getconn()
    # 创建游标对象
    cursor = conn.cursor()
    # 插入
    cursor.execute(sql)
    conn.commit()
    # 关闭游标对象和数据库连接
    cursor.close()
    dbpool.putconn(conn)


def insert(sql: str, params: tuple):
    # 从连接池中获取连接
    conn = dbpool.getconn()
    # 创建游标对象
    cursor = conn.cursor()
    # 插入
    cursor.execute(sql, params)
    conn.commit()
    # 关闭游标对象和数据库连接
    cursor.close()
    dbpool.putconn(conn)


def batchInsert(sql: str, params: list):
    # 从连接池中获取连接
    conn = dbpool.getconn()
    # 创建游标对象
    cursor = conn.cursor()
    # 插入
    cursor.executemany(sql, params)
    conn.commit()
    # 关闭游标对象和数据库连接
    cursor.close()
    dbpool.putconn(conn)


def query(sql):
    # 从连接池中获取连接
    conn = dbpool.getconn()
    # 创建游标对象
    cursor = conn.cursor()
    # 查询
    cursor.execute(sql)
    result = cursor.fetchall()
    # 关闭游标对象和数据库连接
    cursor.close()
    dbpool.putconn(conn)
    return result


def query_single_col(sql):
    # 从连接池中获取连接
    conn = dbpool.getconn()
    # 创建游标对象
    cursor = conn.cursor()
    # 查询
    cursor.execute(sql)
    result = cursor.fetchone()
    # 关闭游标对象和数据库连接
    cursor.close()
    dbpool.putconn(conn)
    return result
