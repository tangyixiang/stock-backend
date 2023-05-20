import configparser
from urllib.parse import quote
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker

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


# Dependency
def getSesion():
    db = SessionLocal()
    try:
        yield db
    finally:
        db.close()


def batch_insert(sql, params):
    with engine.connect() as conn:
        conn.execute(sql, params)
