import datetime
import calendar
from fastapi import APIRouter
import db.mysql as db
import akshare as ak
import pandas as pd
import pandas_ta as ta
import json
from app.utils.cn_stock_utils import save_symbol_his_data
import warnings

warnings.filterwarnings("ignore")

router = APIRouter(prefix="/cn")


@router.get("/all/symbol")
async def cn_all_symbol():
    sql = "replace into cn_stock_info(symbol,name,description) values(%s,%s,%s)"
    exist_sql = "select * from cn_stock_info where symbol= '{0}'"
    df_today_data = ak.stock_zh_a_spot_em()
    for index, row in df_today_data[["代码", "名称", "总市值"]].iterrows():
        code = row["代码"]
        name = row["名称"]
        market_value = row["总市值"]
        result = db.query(exist_sql.format(code))
        if result[0][2] is None:
            try:
                scope_df = ak.stock_zyjs_ths(symbol=code)
                scope = scope_df["经营范围"].iloc[-1]
                # result.append()
                db.insert(sql, (code, name, scope))
            except Exception as e:
                db.insert(sql, (code, name, ""))
                print("异常:", code)
                print(e)
        db.insert(
            "update cn_stock_info set market_value = %s, name=%s where symbol = %s ",
            (str(market_value), str(name), code),
        )
    return {"message": "ok"}


@router.get("/history/data")
async def cn_history_data(
    start_date: str, end_date: str, period: str = "daily", code: str = None
):
    print("当日数据")
    if code:
        save_symbol_his_data(code, start_date, end_date, period)
    else:
        df = ak.stock_zh_a_spot_em()
        symbol_list = df["代码"].tolist()
        for symbol in symbol_list:
            try:
                save_symbol_his_data(symbol, start_date, end_date, period)
            except Exception as e:
                print("未知错误：", e)


@router.get("/today/analysis")
async def cn_today_analysis(code: str = None):
    today = datetime.date.today()
    weekday = today.weekday()
    if weekday >= 5:  # 如果是周六或周日
        days_to_friday = (calendar.FRIDAY - weekday) % 7
        today = today - datetime.timedelta(days=days_to_friday)
    two_weeks_ago = today - datetime.timedelta(weeks=4)
    start_date = two_weeks_ago.strftime("%Y-%m-%d")
    end_date = today.strftime("%Y-%m-%d")
    if code:
        sql = "SELECT * FROM cn_stock_data WHERE symbol = '{}'".format(code)
    else:
        sql = "SELECT * FROM cn_stock_data WHERE DATE BETWEEN '{0}' AND '{1}'".format(
            start_date, end_date
        )
    df = pd.read_sql(sql, db.pool.connection())
    df_ta = ta.cdl_pattern(
        df["open"],
        df["high"],
        df["low"],
        df["close"],
        [
            "darkcloudcover",
            "dojistar",
            "engulfing",
            "hangingman",
            "hammer",
            "belthold",
            "3starsinsouth",
        ],
    )
    df_ta["date"] = df["date"]
    df_ta = df_ta.loc[(df_ta.iloc[:, :-1] != 0.0).any(axis=1)]
    cols = list(df_ta.columns)
    cols.remove("date")
    df_ta = df_ta[["date"] + cols]
    new_columns = ["date"] + [
        col.replace("CDL_", "").lower() for col in df_ta.columns if col != "date"
    ]
    df_ta.columns = new_columns
    merged_df = pd.merge(df, df_ta, on="date", how="left")
    return {
        "length": len(merged_df),
        "data": json.loads(merged_df.to_json(orient="records")),
    }


@router.get("/data")
async def cn_symbol_data_daily(symbol: str, start_date: str, end_date: str):
    """
    根据日期获取symbol数据
    """
    sql = "select * from cn_stock_data where symbol = '{0}' and date BETWEEN '{1}' AND '{2}'".format(
        symbol, start_date, end_date
    )
    df = pd.read_sql_query(sql, db.pool.connection())
    return json.loads(df.to_json(orient="records"))


@router.get("/stock/boll")
async def bbands():
    """
    布林区间
    """
    all_symbol_sql = "select symbol from cn_stock_info"
    max_date_sql = "select max(date) from cn_stock_data"
    max_date = db.query_single_col(max_date_sql)[0]

    cache_data_sql = "select value from cn_stock_indicators where date = '{0}' and name = 'boll' ".format(
        max_date
    )
    cache_data = db.query_single_col(cache_data_sql)
    if cache_data:
        return json.loads(cache_data[0])

    result = db.query(all_symbol_sql)
    boll_mid_list = []
    boll_top_list = []
    for symbol in result:
        sql = "select * from cn_stock_data where symbol = '{0}' order by date desc limit 300".format(
            symbol[0]
        )
        df = pd.read_sql_query(sql, db.pool.connection())
        pd.to_datetime(df["date"])
        df = df.sort_values("date")
        boll = ta.bbands(df["close"])
        df = pd.concat([df, boll], axis=1)
        # 最后一天在布林中线
        if df["BBP_5_2.0"].iloc[-1] > 0.4 and df["BBP_5_2.0"].iloc[-1] < 0.6:
            boll_mid_list.append(df["symbol"].iloc[-1])
        # 最后一天在布林线上方
        if df["close"].iloc[-1] > df["BBU_5_2.0"].iloc[-1]:
            boll_top_list.append(df["symbol"].iloc[-1])
    boll_data = {"mid": boll_mid_list, "top": boll_top_list}
    cache_sql = "insert into cn_stock_indicators values(%s,%s,%s)"
    db.insert(cache_sql, [max_date, "boll", json.dumps(boll_data)])
    return boll_data


@router.get("/capital/flow")
async def capital_flows():
    """
    资金流向
    """
    df = ak.stock_fund_flow_individual(symbol="5日排行")
    return json.loads(df.to_json(orient="records"))


@router.get("/concept/type")
async def concept_flow():
    """
    概念类别
    """
    sql = "truncate table cn_stock_concept"
    db.execute(sql)
    sql = "insert into cn_stock_concept values(%s,%s,%s,%s,%s)"
    df = ak.stock_board_concept_name_ths()
    df = df.dropna()
    data_list = df.values.tolist()
    for data in data_list:
        data[0] = data[0].strftime("%Y-%m-%d")
    db.batchInsert(sql, data_list)
    return {"message": "ok"}


@router.get("/concept/flow")
async def concept_flow():
    """
    概念资金流向
    """
    df = ak.stock_fund_flow_concept(symbol="3日排行")
    sql = "select name ,code from cn_stock_concept where name in {0}".format(
        tuple(df["行业"].values.tolist())
    )
    result = db.query(sql)
    df2 = pd.DataFrame(columns=["行业", "code"], data=result)
    df = pd.merge(df, df2, on="行业")
    return json.loads(df.to_json(orient="records"))


@router.get("/concept/flow/detail")
async def concept_flow(symbol):
    """
    成份股数据
    """
    df = ak.stock_board_cons_ths(symbol=symbol)
    return json.loads(df.to_json(orient="records"))
