import pymysql
from dbutils.pooled_db import PooledDB

# 创建数据库连接池
pool = PooledDB(
    creator=pymysql,  # 使用 PyMySQL 模块
    maxconnections=5,  # 连接池允许的最大连接数
    mincached=2,  # 初始化时连接池中至少存在的空闲连接数
    maxcached=5,  # 连接池中最多允许的空闲连接数
    maxshared=3,  # 连接池中最多允许的共享连接数
    blocking=True,  # 连接池中如果没有可用连接是否阻塞等待，默认为 True
    maxusage=None,  # 一个连接最多被重复使用的次数，默认为 None 表示无限制
    setsession=[],  # 用于传递到数据库的额外参数，通常是一些 SQL 语句
    ping=0,  # 自动检查连接是否有效的间隔时间，0 表示不检查
    host="43.139.119.217",  # 主机地址
    port=3306,  # 端口号，默认为 3306
    user="root",  # 用户名
    password="guilin@2022",  # 密码
    database="stock_data",  # 数据库名称
    charset="utf8mb4",  # 数据库编码方式
)


def insert(sql: str, params: tuple):
    # 从连接池中获取连接
    conn = pool.connection()
    # 创建游标对象
    cursor = conn.cursor()
    # 插入
    cursor.execute(sql, params)
    conn.commit()
    # 关闭游标对象和数据库连接
    cursor.close()
    conn.close()


def batchInsert(sql: str, params: list):
    # 从连接池中获取连接
    conn = pool.connection()
    # 创建游标对象
    cursor = conn.cursor()
    # 插入
    cursor.executemany(sql, params)
    conn.commit()
    # 关闭游标对象和数据库连接
    cursor.close()
    conn.close()


def query(sql):
    # 从连接池中获取连接
    conn = pool.connection()
    # 创建游标对象
    cursor = conn.cursor()
    # 查询
    cursor.execute(sql)
    result = cursor.fetchall()
    return result


def query_single_col(sql):
    # 从连接池中获取连接
    conn = pool.connection()
    # 创建游标对象
    cursor = conn.cursor()
    # 查询
    cursor.execute(sql)
    result = cursor.fetchone()
    return result
