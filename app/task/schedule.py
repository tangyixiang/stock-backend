from apscheduler.schedulers.blocking import BlockingScheduler
import db.mysql as db
import datetime

scheduler = BlockingScheduler()


def sync_today_data():
    print("开始同步数据")
    sql = "select max(date) from cn_stock_data"
    result = db.query(sql)
    date = result[0][0]
    # todo 同步数据


# 每隔 1 分钟执行一次 job 函数
scheduler.add_job(sync_today_data, "interval", minute=1)
# 启动调度器
scheduler.start()
