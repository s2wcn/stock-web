import uvicorn
import importlib
import sys
import os
import time
import math
import random
import pandas as pd
import numpy as np
from fastapi import FastAPI, Request, BackgroundTasks, Body
from fastapi.templating import Jinja2Templates
from fastapi.responses import HTMLResponse
from fastapi.staticfiles import StaticFiles
from contextlib import asynccontextmanager
from apscheduler.schedulers.background import BackgroundScheduler
from apscheduler.triggers.cron import CronTrigger
from datetime import datetime
from tzlocal import get_localzone 

import akshare as ak

# 引入数据库集合
from database import stock_collection, config_collection, template_collection
import crawler_hk as crawler
from crawler_state import status 

# [新增] 引入分析服务
from services.analysis_service import AnalysisService

# 初始化调度器
scheduler = BackgroundScheduler(timezone=str(get_localzone()))
analysis_service = AnalysisService(stock_collection, status)

# 默认定时配置
DEFAULT_SCHEDULE = {
    "type": "daily",      
    "day_of_week": "5",   
    "hour": 17, 
    "minute": 0
}

# === 任务逻辑区域 ===

def analyze_trend_task():
    # 代理给 Service 处理
    analysis_service.analyze_trend()

# 动态任务包装器
def dynamic_task_wrapper():
    if not status.is_running:
        try:
            print("🔄 热加载爬虫模块...")
            importlib.reload(crawler)
            
            # 1. 运行爬虫
            crawler.run_crawler_task()
            
            # 2. 爬虫完成后，自动运行趋势分析
            if not status.should_stop:
                print("🔗 爬虫结束，自动启动趋势分析...")
                analyze_trend_task()
                
        except Exception as e:
            print(f"❌ 任务出错: {e}")
            status.finish(f"任务异常: {e}")

def recalculate_db_task():
    print("🔄 开始执行离线补全指标...")
    cursor = stock_collection.find({})
    all_docs = list(cursor) 
    total = len(all_docs)
    status.start(total)
    status.message = "正在读取数据库..."

    for i, doc in enumerate(all_docs):
        if status.should_stop:
            status.finish("补全任务已终止")
            return

        code = doc["_id"]
        if code.startswith("8"):
             stock_collection.delete_one({"_id": code})
             continue

        name = doc["name"]
        status.update(i + 1, message=f"正在清洗重算: {name}")
        
        history = doc.get("history", [])
        if not history: continue
        
        updated_history = []
        latest_record = {}

        for item in history:
            def get_f(keys):
                for k in keys:
                    val = item.get(k)
                    if val is not None:
                        try:
                            return float(str(val).replace(',', ''))
                        except:
                            pass
                return None

            pe = get_f(['市盈率', 'PE'])
            eps = get_f(['基本每股收益(元)', '基本每股收益'])
            bvps = get_f(['每股净资产(元)', '每股净资产'])
            growth = get_f(['净利润滚动环比增长(%)', '净利润环比增长'])
            div_yield = get_f(['股息率TTM(%)', '股息率'])
            ocf_ps = get_f(['每股经营现金流(元)', '每股经营现金流'])
            roe = get_f(['股东权益回报率(%)', 'ROE'])
            roa = get_f(['总资产回报率(%)', 'ROA'])
            net_margin = get_f(['销售净利率(%)', '销售净利率'])

            derived_keys = [
                'PEG', 'PEGY', '彼得林奇估值', '净现比', '市现率', 
                '财务杠杆', '总资产周转率', '格雷厄姆数', '合理股价'
            ]
            for key in derived_keys:
                item.pop(key, None)

            if pe and pe > 0 and growth and growth != 0:
                item['PEG'] = round(pe / growth, 4)

            if pe and pe > 0 and growth is not None and div_yield is not None:
                total_return = growth + div_yield
                if total_return > 0:
                    item['PEGY'] = round(pe / total_return, 4)
            
            if eps is not None and growth is not None:
                fair_price = eps * (8.5 + 2 * growth)
                if fair_price > 0:
                    item['合理股价'] = round(fair_price, 2)
            
            if ocf_ps is not None and eps and eps > 0:
                item['净现比'] = round(ocf_ps / eps, 2)
            
            if pe and pe > 0 and eps and eps > 0 and ocf_ps and ocf_ps != 0:
                price = pe * eps
                item['市现率'] = round(price / ocf_ps, 2)

            if roe is not None and roa and roa != 0:
                item['财务杠杆'] = round(roe / roa, 2)

            if roa is not None and net_margin and net_margin != 0:
                item['总资产周转率'] = round(roa / net_margin, 2)

            if eps is not None and bvps is not None:
                val = 22.5 * eps * bvps
                if val > 0:
                    item['格雷厄姆数'] = round(math.sqrt(val), 2)
            
            updated_history.append(item)
            latest_record = item

        stock_collection.update_one(
            {"_id": code},
            {"$set": {"history": updated_history, "latest_data": latest_record}}
        )

    status.finish("全库清洗重算完成")

# === 调度器逻辑 ===
def update_scheduler_job(config: dict):
    try:
        hour = config.get('hour', 17)
        minute = config.get('minute', 0)
        sched_type = config.get('type', 'daily')
        day_of_week = config.get('day_of_week', '5')
        
        local_tz = str(get_localzone())

        if scheduler.get_job('crawler_job'):
            scheduler.remove_job('crawler_job')
        
        if sched_type == 'weekly':
            trigger = CronTrigger(day_of_week=int(day_of_week), hour=hour, minute=minute, timezone=local_tz)
        else:
            trigger = CronTrigger(hour=hour, minute=minute, timezone=local_tz)

        scheduler.add_job(dynamic_task_wrapper, trigger, id='crawler_job')
        return True
    except Exception as e:
        print(f"❌ 更新定时任务失败: {e}")
        return False

@asynccontextmanager
async def lifespan(app: FastAPI):
    config = config_collection.find_one({"_id": "schedule_config"})
    if not config:
        config = DEFAULT_SCHEDULE
        config_collection.insert_one({"_id": "schedule_config", **DEFAULT_SCHEDULE})
    
    update_scheduler_job(config)
    scheduler.start()
    
    yield
    scheduler.shutdown()

app = FastAPI(lifespan=lifespan)
app.mount("/static", StaticFiles(directory="static"), name="static")
templates = Jinja2Templates(directory="templates")

# === 字段配置 (复用原有的) ===
COLUMN_CONFIG = [
    {"key": "所属行业", "label": "行业", "desc": "公司所属行业板块", "tip": "按东财/GICS分类标准划分", "no_sort": True, "no_chart": True},
    {"key": "bull_label", "label": "长牛评级", "desc": "长牛分级筛选", "tip": "基于5年走势算法筛选。<br>需满足：<br>1. R²>0.8<br>2. 年化10%-60%<br>3. <b>日均成交 > 500万</b><br>4. <b>ROE > 0</b>", "no_chart": True},
    {"key": "trend_analysis.r_squared", "label": "趋势R²", "desc": "对应周期的拟合度", "tip": "股价走势越接近直线，该值越接近1。<br><b>>0.8</b> 表示极度平稳。", "no_chart": True},
    {"key": "trend_analysis.annual_return_pct", "label": "年化%", "desc": "对应周期的年化收益", "tip": "基于回归斜率推算的年化涨幅。", "suffix": "%", "no_chart": True},
    {"key": "昨收", "label": "昨收", "desc": "最新收盘价", "tip": "最近一个交易日的收盘价格", "no_chart": False},
    {"key": "昨涨跌幅", "label": "涨跌%", "desc": "日涨跌幅", "tip": "最近一个交易日的涨跌百分比", "suffix": "%"},
    {"key": "昨成交量", "label": "成交量", "desc": "日成交量(股)", "tip": "最近一个交易日的成交股数", },
    {"key": "昨换手率", "label": "换手%", "desc": "交易活跃度", "tip": "成交量 ÷ 流通股本", "suffix": "%"},
    {"key": "近一周涨跌幅", "label": "周涨跌%", "desc": "短期动量", "tip": "当前价格相比5个交易日前的涨跌幅", "suffix": "%"},
    {"key": "近一月涨跌幅", "label": "月涨跌%", "desc": "中期动量", "tip": "当前价格相比20个交易日前的涨跌幅", "suffix": "%"},
    {"key": "市盈率", "label": "市盈率(PE)", "desc": "回本年限", "tip": "股价 ÷ 每股收益"},
    {"key": "PEG", "label": "PEG", "desc": "成长估值比", "tip": "PE ÷ (净利增长率 × 100)"},
    {"key": "PEGY", "label": "PEGY", "desc": "股息修正PEG", "tip": "PE ÷ (净利增长率 + 股息率)"},
    {"key": "合理股价", "label": "合理股价", "desc": "格雷厄姆估值", "tip": "EPS × (8.5 + 2 × 盈利增长率)"},
    {"key": "格雷厄姆数", "label": "格雷厄姆数", "desc": "价值上限", "tip": "√(22.5 × EPS × 每股净资产)"},
    {"key": "净现比", "label": "净现比", "desc": "盈利含金量", "tip": "每股经营现金流 ÷ EPS"},
    {"key": "市现率", "label": "市现率", "desc": "现金流估值", "tip": "股价 ÷ 每股经营现金流"},
    {"key": "财务杠杆", "label": "财务杠杆", "desc": "权益乘数", "tip": "总资产 ÷ 股东权益"},
    {"key": "总资产周转率", "label": "周转率", "desc": "营运能力", "tip": "营业收入 ÷ 总资产"},
    {"key": "基本每股收益同比增长率", "label": "EPS同比%", "desc": "盈利增速", "tip": "衡量归属股东利润的增长速度", "suffix": "%"},
    {"key": "营业收入同比增长率", "label": "营收同比%", "desc": "规模增速", "tip": "衡量业务规模的扩张速度", "suffix": "%"},
    {"key": "营业利润率同比增长率", "label": "利润率同比%", "desc": "获利能力变动", "tip": "反映产品竞争力的变化趋势", "suffix": "%"},
    {"key": "基本每股收益(元)", "label": "EPS(元)", "desc": "每股所获利润", "tip": ""},
    {"key": "每股净资产(元)", "label": "BPS(元)", "desc": "每股归属权益", "tip": ""},
    {"key": "每股经营现金流(元)", "label": "每股现金流", "desc": "每股进账现金", "tip": ""},
    {"key": "市净率", "label": "市净率(PB)", "desc": "净资产溢价", "tip": "股价 ÷ 每股净资产"},
    {"key": "股息率TTM(%)", "label": "股息率%", "desc": "分红回报率", "tip": "过去12个月分红总额 ÷ 市值", "suffix": "%"},
    {"key": "每股股息TTM(港元)", "label": "每股股息", "desc": "每股分到的钱", "tip": ""},
    {"key": "派息比率(%)", "label": "派息比%", "desc": "分红慷慨度", "tip": "总分红 ÷ 总净利润", "suffix": "%"},
    {"key": "营业总收入", "label": "营收", "desc": "总生意额", "tip": ""},
    {"key": "营业总收入滚动环比增长(%)", "label": "营收环比%", "desc": "营收短期趋势", "tip": "", "suffix": "%"},
    {"key": "净利润", "label": "净利润", "desc": "最终落袋利润", "tip": ""},
    {"key": "净利润滚动环比增长(%)", "label": "净利环比%", "desc": "净利短期趋势", "tip": "", "suffix": "%"},
    {"key": "销售净利率(%)", "label": "净利率%", "desc": "产品暴利程度", "tip": "净利润 ÷ 营收", "suffix": "%"},
    {"key": "股东权益回报率(%)", "label": "ROE%", "desc": "净资产收益率", "tip": "衡量管理层用股东的钱生钱的能力", "suffix": "%"},
    {"key": "总资产回报率(%)", "label": "ROA%", "desc": "总资产收益率", "tip": "衡量所有资产(含负债)的综合利用效率", "suffix": "%"},
    {"key": "总市值(港元)", "label": "总市值", "desc": "", "tip": ""},
    {"key": "港股市值(港元)", "label": "港股市值", "desc": "", "tip": ""},
    {"key": "法定股本(股)", "label": "法定股本", "desc": "", "tip": "", "no_sort": True, "no_chart": True},
    {"key": "已发行股本(股)", "label": "发行股本", "desc": "", "tip": "", "no_sort": True, "no_chart": True},
    {"key": "已发行股本-H股(股)", "label": "H股股本", "desc": "", "tip": "", "no_sort": True, "no_chart": True},
    {"key": "每手股", "label": "每手股", "desc": "", "tip": "", "no_sort": True, "no_chart": True},
]

# === 路由区域 ===

@app.get("/", response_class=HTMLResponse)
async def read_root(request: Request):
    # [修改] 不再加载所有数据，只传元数据
    # 获取最后更新时间
    last_time = status.last_finished_time
    if not last_time:
        try:
            latest_doc = stock_collection.find_one(sort=[("updated_at", -1)])
            if latest_doc and "updated_at" in latest_doc:
                last_time = latest_doc["updated_at"]
        except:
            pass
    last_time_str = last_time.strftime("%Y-%m-%d %H:%M") if last_time else "从未"

    return templates.TemplateResponse("index.html", {
        "request": request, 
        "columns": COLUMN_CONFIG,
        "last_updated": last_time_str
    })

# === [新增] 通用分页查询接口 ===
@app.post("/api/stocks/query")
async def query_stocks(
    page: int = Body(1), 
    page_size: int = Body(50), 
    sort_key: str = Body(None), 
    sort_dir: str = Body("asc"),
    filters: dict = Body(None),
    search: str = Body(None)
):
    query = {}
    
    # 1. 搜索
    if search:
        query["$or"] = [
            {"_id": {"$regex": search, "$options": "i"}},
            {"name": {"$regex": search, "$options": "i"}}
        ]
    
    # 2. 筛选
    if filters:
        filter_conditions = []
        for key, range_val in filters.items():
            # 获取数据库实际字段名
            db_key = key
            # 特殊字段处理
            if key in ["bull_label", "所属行业", "is_ggt"]:
                continue # 暂时不处理这类非数值范围筛选
            elif key.startswith("trend_analysis."):
                db_key = key # 保持原样 (如 trend_analysis.r_squared)
            elif key in ["昨收", "昨涨跌幅", "昨成交量", "昨换手率", "近一周涨跌幅", "近一月涨跌幅"]:
                db_key = f"latest_data.{key}" # 在 latest_data 下但不需要前缀
            else:
                # 默认都在 latest_data 下
                db_key = f"latest_data.{key}"

            min_v = range_val.get("min")
            max_v = range_val.get("max")
            
            range_query = {}
            if min_v is not None and min_v != "":
                range_query["$gte"] = float(min_v)
            if max_v is not None and max_v != "":
                range_query["$lte"] = float(max_v)
            
            if range_query:
                cond = {db_key: range_query}
                filter_conditions.append(cond)
        
        if filter_conditions:
            if "$or" in query:
                query = {"$and": [query, *filter_conditions]}
            else:
                if len(filter_conditions) == 1:
                    query.update(filter_conditions[0])
                else:
                    query["$and"] = filter_conditions

    # 3. 排序
    sort_stage = [("_id", 1)]
    if sort_key:
        db_sort_key = sort_key
        if sort_key not in ["_id", "name", "bull_label"] and not sort_key.startswith("trend_analysis"):
             db_sort_key = f"latest_data.{sort_key}"
             
        direction = 1 if sort_dir == "asc" else -1
        sort_stage = [(db_sort_key, direction)]

    # 4. 执行
    total_count = stock_collection.count_documents(query)
    cursor = stock_collection.find(query).sort(sort_stage).skip((page - 1) * page_size).limit(page_size)
    
    data = []
    for doc in cursor:
        latest = doc.get('latest_data', {})
        trend = doc.get("trend_analysis", {})
        
        # 扁平化处理
        item = {
            "code": doc["_id"],
            "name": doc["name"],
            "date": latest.get("date", "-"),
            "intro": doc.get("intro") or latest.get("企业简介", ""),
            "is_ggt": doc.get("is_ggt", False),
            "bull_label": doc.get("bull_label", ""),
            **latest # 展开 latest_data
        }
        # 展开 trend_analysis 并加上前缀，以便前端 key 匹配
        for k, v in trend.items():
            item[f"trend_analysis.{k}"] = v
            
        data.append(item)

    return {
        "total": total_count,
        "page": page,
        "page_size": page_size,
        "data": data
    }

@app.get("/api/history/{code}")
async def get_history(code: str):
    doc = stock_collection.find_one({"_id": code})
    if not doc:
        return {"name": code, "history": []}
    return {"name": doc["name"], "history": doc.get("history", [])}

@app.get("/api/trigger_crawl")
async def trigger_crawl():
    if status.is_running:
        return {"success": False, "message": "任务正在运行中，请勿重复触发"}
    scheduler.add_job(dynamic_task_wrapper)
    return {"success": True, "message": "后台任务已启动 (爬虫 + 自动趋势分析)"}

@app.post("/api/stop_crawl")
async def stop_crawl():
    if not status.is_running:
        return {"success": False, "message": "当前没有运行中的任务"}
    status.request_stop()
    return {"success": True, "message": "正在终止任务，请稍候..."}

@app.post("/api/recalculate")
async def trigger_recalculate(background_tasks: BackgroundTasks):
    if status.is_running:
        return {"success": False, "message": "后台已有任务在运行，请稍候..."}
    background_tasks.add_task(recalculate_db_task)
    return {"success": True, "message": "已开始补全计算，请留意右上角进度条"}

@app.get("/api/status")
async def get_status():
    return {
        "is_running": status.is_running,
        "current": status.current,
        "total": status.total,
        "message": status.message
    }

def restart_program():
    time.sleep(0.5) 
    current_file = os.path.abspath(__file__)
    if os.path.exists(current_file):
        os.utime(current_file, None)

@app.post("/api/restart")
async def restart_service(background_tasks: BackgroundTasks):
    background_tasks.add_task(restart_program)
    return {"success": True, "message": "服务正在重载，页面将在 3 秒后刷新..."}

@app.get("/api/schedule")
async def get_schedule():
    config = config_collection.find_one({"_id": "schedule_config"})
    if not config:
        config = DEFAULT_SCHEDULE
    if "type" not in config: config["type"] = "daily"
    if "day_of_week" not in config: config["day_of_week"] = "5"
    return {
        "type": config.get("type"),
        "day_of_week": config.get("day_of_week"),
        "hour": config.get("hour"),
        "minute": config.get("minute")
    }

@app.post("/api/schedule")
async def set_schedule(data: dict = Body(...)):
    hour = int(data.get("hour"))
    minute = int(data.get("minute"))
    sched_type = data.get("type", "daily")
    day_of_week = str(data.get("day_of_week", "5"))
    
    new_config = {
        "type": sched_type,
        "day_of_week": day_of_week,
        "hour": hour,
        "minute": minute
    }

    config_collection.update_one(
        {"_id": "schedule_config"},
        {"$set": new_config},
        upsert=True
    )
    
    if update_scheduler_job(new_config):
        week_map = ["一", "二", "三", "四", "五", "六", "日"]
        desc = f"每天 {hour:02d}:{minute:02d}" if sched_type == 'daily' else f"每周{week_map[int(day_of_week)]} {hour:02d}:{minute:02d}"
        return {"success": True, "message": f"定时任务已更新: {desc}"}
    else:
        return {"success": False, "message": "调度器更新失败"}

@app.get("/api/templates")
async def get_templates():
    cursor = template_collection.find({}, {"_id": 0}).sort("name", 1)
    return list(cursor)

@app.post("/api/templates")
async def save_template(data: dict = Body(...)):
    name = data.get("name")
    filters = data.get("filters")
    if not name or not name.strip(): return {"success": False, "message": "模版名称不能为空"}
    if not filters: return {"success": False, "message": "模版内容不能为空"}
    
    template_collection.replace_one(
        {"name": name.strip()}, 
        {"name": name.strip(), "filters": filters}, 
        upsert=True
    )
    return {"success": True, "message": "模版已保存"}

@app.delete("/api/templates/{name}")
async def delete_template(name: str):
    result = template_collection.delete_one({"name": name})
    if result.deleted_count > 0:
        return {"success": True, "message": "模版已删除"}
    else:
        return {"success": False, "message": "模版不存在"}

if __name__ == "__main__":
    uvicorn.run("main:app", host="0.0.0.0", port=8000, reload=True)