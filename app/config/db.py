from psycopg2 import pool
from sqlalchemy import create_engine

engine = create_engine(
    "postgresql://postgres:11111@43.136.114.133:5432",
    database="stock_data",
    schema="cn_stock",
    echo=True,  # echo 设为 True 会打印出实际执行的 sql，调试的时候更方便
    future=True,  # 使用 SQLAlchemy 2.0 API，向后兼容
    pool_size=50,  # 连接池的大小默认为 5 个，设置为 0 时表示连接无限制
    pool_recycle=3600,  # 设置时间以限制数据库自动断开
)


def batch_insert(sql, params):
    with engine.connect() as conn:
        conn.execute(sql, params)


# # 创建连接池
# dbpool = pool.SimpleConnectionPool(
#     minconn=10,
#     maxconn=50,
#     host="43.136.114.133",
#     database="stock_data",
#     user="postgres",
#     password="guilin@2022",
#     options="-c search_path=cn_stock",
# )


# def execute(sql: str):
#     # 从连接池中获取连接
#     conn = dbpool.getconn()
#     # 创建游标对象
#     cursor = conn.cursor()
#     # 插入
#     cursor.execute(sql)
#     conn.commit()
#     # 关闭游标对象和数据库连接
#     cursor.close()
#     dbpool.putconn(conn)


# def insert(sql: str, params: tuple):
#     # 从连接池中获取连接
#     conn = dbpool.getconn()
#     # 创建游标对象
#     cursor = conn.cursor()
#     # 插入
#     cursor.execute(sql, params)
#     conn.commit()
#     # 关闭游标对象和数据库连接
#     cursor.close()
#     dbpool.putconn(conn)


# def batchInsert(sql: str, params: list):
#     # 从连接池中获取连接
#     conn = dbpool.getconn()
#     # 创建游标对象
#     cursor = conn.cursor()
#     # 插入
#     cursor.executemany(sql, params)
#     conn.commit()
#     # 关闭游标对象和数据库连接
#     cursor.close()
#     dbpool.putconn(conn)


# def query(sql):
#     # 从连接池中获取连接
#     conn = dbpool.getconn()
#     # 创建游标对象
#     cursor = conn.cursor()
#     # 查询
#     cursor.execute(sql)
#     result = cursor.fetchall()
#     # 关闭游标对象和数据库连接
#     cursor.close()
#     dbpool.putconn(conn)
#     return result


# def query_single_col(sql):
#     # 从连接池中获取连接
#     conn = dbpool.getconn()
#     # 创建游标对象
#     cursor = conn.cursor()
#     # 查询
#     cursor.execute(sql)
#     result = cursor.fetchone()
#     # 关闭游标对象和数据库连接
#     cursor.close()
#     dbpool.putconn(conn)
#     return result


# def copy_from(file, table, sep, null, columns):
#     conn = dbpool.getconn()
#     cursor = conn.cursor()
#     cursor.copy_from(file=file, table=table, sep=sep, null=null, columns=columns)
#     conn.commit()
#     # 关闭游标对象和数据库连接
#     cursor.close()
#     dbpool.putconn(conn)


# def query_for_obj(sql):
#     # 从连接池中获取连接
#     conn = dbpool.getconn()
#     # 创建游标对象
#     cursor = conn.cursor()
#     # 查询
#     cursor.execute(sql)
#     result = cursor.fetchall()
#     rows = [dict(zip([column[0] for column in cursor.description], row)) for row in result]
#     # 关闭游标对象和数据库连接
#     cursor.close()
#     dbpool.putconn(conn)
#     return rows
