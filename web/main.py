import uvicorn
import importlib
from fastapi import FastAPI, Request
from fastapi.templating import Jinja2Templates
from fastapi.responses import HTMLResponse
from contextlib import asynccontextmanager
from apscheduler.schedulers.background import BackgroundScheduler

# 导入新的 database 和 crawler
from database import stock_collection
import crawler

# === 1. 初始化调度器 ===
scheduler = BackgroundScheduler()

def dynamic_task_wrapper():
    try:
        print("🔄 热加载爬虫模块...")
        importlib.reload(crawler)
        crawler.run_crawler_task()
    except Exception as e:
        print(f"❌ 任务出错: {e}")

@asynccontextmanager
async def lifespan(app: FastAPI):
    # 每天下午 17:00 执行
    scheduler.add_job(dynamic_task_wrapper, 'cron', hour=17, minute=0, id='crawler_job')
    print("⏰ MongoDB 爬虫调度已启动...")
    scheduler.start()
    yield
    scheduler.shutdown()

# === 2. 初始化 FastAPI App (关键步骤，必须在 @app.get 之前) ===
app = FastAPI(lifespan=lifespan)
templates = Jinja2Templates(directory="templates")

# === 3. 定义常量 ===
DISPLAY_FIELDS = [
    "基本每股收益(元)", "每股净资产(元)", "法定股本(股)", "每手股", 
    "每股股息TTM(港元)", "派息比率(%)", "已发行股本(股)", "已发行股本-H股(股)", 
    "每股经营现金流(元)", "股息率TTM(%)", "总市值(港元)", "港股市值(港元)", 
    "营业总收入", "营业总收入滚动环比增长(%)", "销售净利率(%)", "净利润", 
    "净利润滚动环比增长(%)", "股东权益回报率(%)", "市盈率", "PEG", "市净率", 
    "总资产回报率(%)"
]

# === 4. API 接口 ===

@app.get("/", response_class=HTMLResponse)
async def read_root(request: Request):
    # 查询列表：只查 code, name 和 latest_data
    cursor = stock_collection.find({}, {"history": 0}).limit(200)
    
    stocks = []
    for doc in cursor:
        latest = doc.get('latest_data', {})
        stock_item = {
            "code": doc["_id"],
            "name": doc["name"],
            "date": latest.get("date", "-")
        }
        
        # 动态填充所有财务字段
        for field in DISPLAY_FIELDS:
            val = latest.get(field)
            if isinstance(val, (int, float)):
                stock_item[field] = f"{val:,.2f}" # 添加千分位
            else:
                stock_item[field] = val if val else "-"
                
        stocks.append(stock_item)
    
    return templates.TemplateResponse("index.html", {
        "request": request, 
        "stocks": stocks,
        "fields": DISPLAY_FIELDS 
    })

@app.get("/api/history/{code}")
async def get_history(code: str):
    # 查询单只股票完整信息
    doc = stock_collection.find_one({"_id": code})
    
    if not doc:
        return {"dates": [], "pe": [], "peg": [], "name": code}

    history = doc.get("history", [])
    
    dates = [h.get("date") for h in history]
    pe_values = []
    peg_values = []
    
    for h in history:
        # 尝试找 PE
        pe = next((h[k] for k in h if "市盈率" in k or k == "PE"), None)
        pe_values.append(pe)
        peg_values.append(h.get("PEG", h.get("peg"))) # 兼容大小写
    
    return {
        "dates": dates,
        "pe": pe_values,
        "peg": peg_values,
        "name": doc["name"]
    }

@app.get("/api/trigger_crawl")
async def trigger_crawl():
    scheduler.add_job(dynamic_task_wrapper)
    return {"message": "后台任务已触发"}

if __name__ == "__main__":
    uvicorn.run("main:app", host="0.0.0.0", port=8000, reload=True)