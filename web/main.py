# 文件路径: web/main.py
import uvicorn
import importlib
import os
import time
from fastapi import FastAPI, Request, BackgroundTasks, Body
from fastapi.templating import Jinja2Templates
from fastapi.responses import HTMLResponse
from fastapi.staticfiles import StaticFiles
from contextlib import asynccontextmanager
from apscheduler.schedulers.background import BackgroundScheduler
from apscheduler.triggers.cron import CronTrigger
from datetime import datetime
from tzlocal import get_localzone 
from typing import Optional, Dict, Any, List
from pydantic import BaseModel, Field 

# 引入项目模块
from database import stock_collection, config_collection, template_collection
import crawler_hk as crawler
from crawler_state import status 
from services.analysis_service import AnalysisService
from services.maintenance_service import MaintenanceService  
from config import COLUMN_CONFIG
from logger import sys_logger as logger

# === Pydantic Request Models (API 数据模型) ===
class ScheduleRequest(BaseModel):
    hour: int = Field(..., ge=0, le=23, description="小时 (0-23)")
    minute: int = Field(..., ge=0, le=59, description="分钟 (0-59)")
    type: str = Field("daily", pattern="^(daily|weekly)$")
    day_of_week: str = "5"

class FilterRange(BaseModel):
    min: Optional[Any] = None
    max: Optional[Any] = None

class StockQueryRequest(BaseModel):
    page: int = Field(1, ge=1)
    page_size: int = Field(50, ge=1, le=1000)
    sort_key: Optional[str] = None
    sort_dir: str = Field("asc", pattern="^(asc|desc)$")
    search: Optional[str] = None
    filters: Optional[Dict[str, Any]] = None

class TemplateRequest(BaseModel):
    name: str
    filters: Dict[str, Any]

# === 初始化服务 ===
scheduler = BackgroundScheduler(timezone=str(get_localzone()))
analysis_service = AnalysisService(stock_collection, status)
maintenance_service = MaintenanceService(stock_collection, status) 

# 默认定时配置
DEFAULT_SCHEDULE = {
    "type": "daily",      
    "day_of_week": "5",   
    "hour": 17, 
    "minute": 0
}

# === 任务与调度 ===
def dynamic_task_wrapper():
    """全自动任务流: 爬虫 -> 分析 -> 策略 -> 通知"""
    if not status.is_running:
        try:
            logger.info("🔄 任务阶段 1/4: 启动爬虫...")
            # reload 确保代码修改后不用重启也能生效 (开发模式用)
            importlib.reload(crawler)
            crawler.run_crawler_task()
            
            if status.should_stop: return

            logger.info("🔄 任务阶段 2/4: 启动趋势分析...")
            analysis_service.analyze_trend()

            if status.should_stop: return

            logger.info("🔄 任务阶段 3/4: 启动策略参数优化...")
            analysis_service.optimize_strategies()
            
            if status.should_stop: return

            # [新增] 阶段 4: 信号检查与通知
            logger.info("🔄 任务阶段 4/4: 检查买卖信号并通知...")
            analysis_service.check_signals_and_notify()
            
            logger.info("🎉 全流程任务执行完毕")
            
        except Exception as e:
            logger.error(f"❌ 任务出错: {e}")
            status.finish(f"任务异常: {e}")

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
        logger.error(f"❌ 更新定时任务失败: {e}")
        return False

@asynccontextmanager
async def lifespan(app: FastAPI):
    # 启动钩子
    config = config_collection.find_one({"_id": "schedule_config"})
    if not config:
        config = DEFAULT_SCHEDULE
        config_collection.insert_one({"_id": "schedule_config", **DEFAULT_SCHEDULE})
    
    update_scheduler_job(config)
    scheduler.start()
    logger.info("✅ 后台调度器已启动")
    yield
    # 关闭钩子
    scheduler.shutdown()
    logger.info("🛑 后台调度器已关闭")

app = FastAPI(lifespan=lifespan, title="港股全维财务监控系统", version="2.1.0")
app.mount("/static", StaticFiles(directory="static"), name="static")
templates = Jinja2Templates(directory="templates")

# === API 路由 ===

@app.get("/", response_class=HTMLResponse)
async def read_root(request: Request):
    last_time = status.last_finished_time
    if not last_time:
        try:
            latest_doc = stock_collection.find_one(sort=[("updated_at", -1)])
            if latest_doc and "updated_at" in latest_doc:
                last_time = latest_doc["updated_at"]
        except: pass
    last_time_str = last_time.strftime("%Y-%m-%d %H:%M") if last_time else "从未"

    return templates.TemplateResponse("index.html", {
        "request": request, 
        "columns": COLUMN_CONFIG,
        "last_updated": last_time_str
    })

@app.post("/api/stocks/query")
async def query_stocks(req: StockQueryRequest):
    """
    通用股票查询接口
    """
    query = {}
    
    # 1. 搜索
    if req.search:
        query["$or"] = [
            {"_id": {"$regex": req.search, "$options": "i"}},
            {"name": {"$regex": req.search, "$options": "i"}}
        ]
    
    # 2. 筛选
    if req.filters:
        filter_conditions = []
        for key, range_val in req.filters.items():
            db_key = key
            if key == "code": db_key = "_id"
            elif key.startswith("trend_analysis.") or key.startswith("ma_strategy.") or key == "bull_label": db_key = key 
            elif key == "所属行业": db_key = "latest_data.所属行业"
            elif key not in ["_id", "name"]: db_key = f"latest_data.{key}"

            min_v = range_val.get("min")
            max_v = range_val.get("max")
            
            # 长牛评级特殊处理 (例如 1-5年)
            if key == "bull_label":
                try:
                    target_labels = []
                    start_year = int(float(min_v)) if (min_v is not None and min_v != "") else 1
                    end_year = int(float(max_v)) if (max_v is not None and max_v != "") else 5
                    for y in range(1, 6):
                        if start_year <= y <= end_year:
                            target_labels.append(f"长牛{y}年")
                    if target_labels:
                        filter_conditions.append({db_key: {"$in": target_labels}})
                    continue
                except ValueError: pass

            # 文本模糊匹配
            if key in ["所属行业", "bull_label"]:
                if min_v: filter_conditions.append({db_key: {"$regex": str(min_v), "$options": "i"}})
                continue 

            # 数值范围匹配
            range_query = {}
            if min_v is not None and min_v != "":
                try: range_query["$gte"] = float(min_v)
                except: pass
            if max_v is not None and max_v != "":
                try: range_query["$lte"] = float(max_v)
                except: pass
            if range_query:
                filter_conditions.append({db_key: range_query})
        
        if filter_conditions:
            if "$or" in query: query = {"$and": [query, *filter_conditions]}
            else: 
                if len(filter_conditions) == 1: query.update(filter_conditions[0])
                else: query["$and"] = filter_conditions

    # 3. 排序
    sort_stage = [("_id", 1)]
    if req.sort_key:
        db_sort_key = req.sort_key
        if req.sort_key == "code": db_sort_key = "_id"
        elif req.sort_key not in ["_id", "name", "bull_label"] and not req.sort_key.startswith("trend_analysis") and not req.sort_key.startswith("ma_strategy"):
             db_sort_key = f"latest_data.{req.sort_key}"
             
        direction = 1 if req.sort_dir == "asc" else -1
        sort_stage = [(db_sort_key, direction)]

    # 4. 执行查询
    total_count = stock_collection.count_documents(query)
    cursor = stock_collection.find(query).sort(sort_stage).skip((req.page - 1) * req.page_size).limit(req.page_size)
    
    data = []
    for doc in cursor:
        latest = doc.get('latest_data', {})
        trend = doc.get("trend_analysis", {})
        ma_strat = doc.get("ma_strategy", {}) 
        
        item = {
            "code": doc["_id"],
            "name": doc["name"],
            "date": latest.get("date", "-"),
            "intro": doc.get("intro") or latest.get("企业简介", ""),
            "is_ggt": doc.get("is_ggt", False),
            "bull_label": doc.get("bull_label", ""),
            **latest 
        }
        for k, v in trend.items(): item[f"trend_analysis.{k}"] = v

        if ma_strat:
            item["ma_strategy.total_return"] = ma_strat.get("total_return")
            item["ma_strategy.benchmark_return"] = ma_strat.get("benchmark_return")
            params = ma_strat.get("params", {})
            item["ma_strategy.buy_bias"] = params.get("buy_ma60_bias")
            item["ma_strategy.sell_bias"] = params.get("sell_ma5_bias")
            metrics = ma_strat.get("metrics", {})
            item["ma_strategy.win_rate"] = metrics.get("win_rate")
            item["ma_strategy.trades"] = metrics.get("trades")
            
        data.append(item)

    return {
        "total": total_count,
        "page": req.page,
        "page_size": req.page_size,
        "data": data
    }

@app.get("/api/history/{code}")
async def get_history(code: str):
    doc = stock_collection.find_one({"_id": code}, {"name": 1, "history": 1})
    if not doc: return {"name": code, "history": []}
    return {"name": doc["name"], "history": doc.get("history", [])}

@app.get("/api/trigger_crawl")
async def trigger_crawl(background_tasks: BackgroundTasks):
    if status.is_running:
        return {"success": False, "message": "任务正在运行中，请勿重复触发"}
    background_tasks.add_task(dynamic_task_wrapper)
    return {"success": True, "message": "后台任务已启动 (爬虫 + 趋势分析 + 策略优化 + 钉钉通知)"}

@app.post("/api/stop_crawl")
async def stop_crawl():
    if not status.is_running:
        return {"success": False, "message": "当前没有运行中的任务"}
    status.request_stop()
    return {"success": True, "message": "正在终止任务，请稍候..."}

@app.post("/api/recalculate")
async def trigger_recalculate(background_tasks: BackgroundTasks):
    if status.is_running:
        return {"success": False, "message": "后台已有任务在运行"}
    background_tasks.add_task(maintenance_service.run_recalculate_task)
    return {"success": True, "message": "已开始补全计算"}

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
        os.utime(current_file, None) # 触发 uvicorn reload

@app.post("/api/restart")
async def restart_service(background_tasks: BackgroundTasks):
    background_tasks.add_task(restart_program)
    return {"success": True, "message": "服务正在重载，页面将在 3 秒后刷新..."}

@app.get("/api/schedule")
async def get_schedule():
    config = config_collection.find_one({"_id": "schedule_config"})
    if not config: config = DEFAULT_SCHEDULE
    return config

@app.post("/api/schedule")
async def set_schedule(req: ScheduleRequest):
    new_config = req.dict()
    config_collection.update_one({"_id": "schedule_config"}, {"$set": new_config}, upsert=True)
    
    if update_scheduler_job(new_config):
        return {"success": True, "message": "定时任务已更新"}
    else:
        return {"success": False, "message": "调度器更新失败"}

@app.get("/api/templates")
async def get_templates():
    cursor = template_collection.find({}, {"_id": 0}).sort("name", 1)
    return list(cursor)

@app.post("/api/templates")
async def save_template(req: TemplateRequest):
    if not req.name.strip(): return {"success": False, "message": "模版名称不能为空"}
    if not req.filters: return {"success": False, "message": "模版内容不能为空"}
    
    template_collection.replace_one(
        {"name": req.name.strip()}, 
        {"name": req.name.strip(), "filters": req.filters}, 
        upsert=True
    )
    return {"success": True, "message": "模版已保存"}

@app.delete("/api/templates/{name}")
async def delete_template(name: str):
    result = template_collection.delete_one({"name": name})
    return {"success": result.deleted_count > 0, "message": "模版已删除" if result.deleted_count > 0 else "模版不存在"}

if __name__ == "__main__":
    uvicorn.run("main:app", host="0.0.0.0", port=8000, reload=True)