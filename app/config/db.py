import configparser
from urllib.parse import quote
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker, Session

config = configparser.ConfigParser()
config.read("config.ini")

username = config.get("database", "username")
password = quote(config.get("database", "password"))
host = config.get("database", "host")
port = config.get("database", "port")
name = config.get("database", "name")

url = f"postgresql://{username}:{password}@{host}:{port}/{name}"

engine = create_engine(
    url,
    connect_args={"options": "-csearch_path=cn_stock"},
    echo=True,  # echo 设为 True 会打印出实际执行的 sql，调试的时候更方便
    future=True,  # 使用 SQLAlchemy 2.0 API，向后兼容
    pool_size=50,  # 连接池的大小默认为 5 个，设置为 0 时表示连接无限制
)

SessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=engine)


def singleSession() -> Session:
    return SessionLocal()


# Dependency
def getSesion() -> Session:
    db = SessionLocal()
    try:
        yield db
    finally:
        db.close()


def batch_insert(sql, params):
    with engine.connect() as conn:
        conn.execute(statement=sql, parameters=params)
        conn.commit()


def copy_from(file, table, sep, null, columns):
    conn = engine.raw_connection()
    cursor = conn.cursor()
    cursor.copy_from(file=file, table=table, sep=sep, null=null, columns=columns)
    conn.commit()
    # 关闭游标对象和数据库连接
    cursor.close()
    conn.close()
