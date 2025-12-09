import uvicorn
import importlib
import sys
import os
import time
import math
from fastapi import FastAPI, Request, BackgroundTasks
from fastapi.templating import Jinja2Templates
from fastapi.responses import HTMLResponse
from contextlib import asynccontextmanager
from apscheduler.schedulers.background import BackgroundScheduler
from datetime import datetime

from database import stock_collection
import crawler
from crawler_state import status 

scheduler = BackgroundScheduler()

def dynamic_task_wrapper():
    if not status.is_running:
        try:
            print("🔄 热加载爬虫模块...")
            importlib.reload(crawler)
            crawler.run_crawler_task()
        except Exception as e:
            print(f"❌ 任务出错: {e}")
            status.finish()

def recalculate_db_task():
    print("🔄 开始执行离线补全指标...")
    cursor = stock_collection.find({})
    all_docs = list(cursor) 
    total = len(all_docs)
    status.start(total)
    status.message = "正在读取数据库..."

    for i, doc in enumerate(all_docs):
        code = doc["_id"]
        name = doc["name"]
        status.update(i + 1, message=f"正在重算: {name}")
        
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

            if "PEG" not in item and pe and growth and growth != 0:
                item['PEG'] = round(pe / growth, 4)

            if pe and growth is not None and div_yield is not None:
                total_return = growth + div_yield
                if total_return > 0:
                    item['PEGY'] = round(pe / total_return, 4)
            
            if growth is not None and div_yield is not None:
                item['彼得林奇估值'] = round(growth + div_yield, 2)
            
            if ocf_ps is not None and eps and eps != 0:
                item['净现比'] = round(ocf_ps / eps, 2)
            
            if pe and eps and ocf_ps and ocf_ps != 0:
                price = pe * eps
                item['市现率'] = round(price / ocf_ps, 2)

            if roe is not None and roa and roa != 0:
                item['财务杠杆'] = round(roe / roa, 2)

            if roa is not None and net_margin and net_margin != 0:
                item['总资产周转率'] = round(roa / net_margin, 2)

            if eps and bvps:
                val = 22.5 * eps * bvps
                if val > 0:
                    item['格雷厄姆数'] = round(math.sqrt(val), 2)
            
            updated_history.append(item)
            latest_record = item

        stock_collection.update_one(
            {"_id": code},
            {"$set": {"history": updated_history, "latest_data": latest_record}}
        )

    status.finish()
    print("✅ 离线补全完成")

@asynccontextmanager
async def lifespan(app: FastAPI):
    scheduler.add_job(dynamic_task_wrapper, 'cron', hour=17, minute=0, id='crawler_job')
    print("⏰ MongoDB 爬虫调度已启动...")
    scheduler.start()
    yield
    scheduler.shutdown()

app = FastAPI(lifespan=lifespan)
templates = Jinja2Templates(directory="templates")

# === 核心修改：完整字段配置列表（30个字段全集） ===
COLUMN_CONFIG = [
    # --- 1. 核心估值 (最前) ---
    {
        "key": "PEG", "label": "PEG", 
        "desc": "市盈率 ÷ 盈利增长率", "tip": "小于 1 低估；大于 2 高估。"
    },
    
    # --- 2. 高级分析指标 ---
    {
        "key": "PEGY", "label": "PEGY", 
        "desc": "考虑股息的PEG", "tip": "小于 1 极具吸引力。"
    },
    {
        "key": "彼得林奇估值", "label": "彼得林奇值", 
        "desc": "增长率 + 股息率", "tip": "若 > PE 的 1.5 倍，则低估。"
    },
    {
        "key": "格雷厄姆数", "label": "格雷厄姆数", 
        "desc": "√(22.5×EPS×BVPS)", "tip": "股价低于此数则安全边际高。"
    },
    {
        "key": "净现比", "label": "净现比", 
        "desc": "每股现金流 ÷ EPS", "tip": ">1 盈利质量高；<1 警惕纸面富贵。"
    },
    {
        "key": "市现率", "label": "市现率", 
        "desc": "股价 ÷ 每股现金流", "tip": "越低越好，<10 为佳。"
    },
    {
        "key": "财务杠杆", "label": "财务杠杆", 
        "desc": "权益乘数", "tip": "过高意味着高负债风险。"
    },
    {
        "key": "总资产周转率", "label": "周转率", 
        "desc": "营收 ÷ 总资产", "tip": "越高代表资产利用效率越高。"
    },

    # --- 3. 基础财务字段 (完整恢复) ---
    # 盈利与资产
    {"key": "基本每股收益(元)", "label": "EPS(元)", "desc": "", "tip": ""},
    {"key": "每股净资产(元)", "label": "BPS(元)", "desc": "", "tip": ""},
    {"key": "每股经营现金流(元)", "label": "每股现金流", "desc": "", "tip": ""},
    
    # 估值基础
    {"key": "市盈率", "label": "市盈率(PE)", "desc": "", "tip": ""},
    {"key": "市净率", "label": "市净率(PB)", "desc": "", "tip": ""},
    
    # 股息分红
    {"key": "股息率TTM(%)", "label": "股息率%", "desc": "", "tip": ""},
    {"key": "每股股息TTM(港元)", "label": "每股股息", "desc": "", "tip": ""},
    {"key": "派息比率(%)", "label": "派息比%", "desc": "", "tip": ""},
    
    # 营收与利润
    {"key": "营业总收入", "label": "营收", "desc": "", "tip": ""},
    {"key": "营业总收入滚动环比增长(%)", "label": "营收增长%", "desc": "", "tip": ""},
    {"key": "净利润", "label": "净利润", "desc": "", "tip": ""},
    {"key": "净利润滚动环比增长(%)", "label": "净利增长%", "desc": "", "tip": ""},
    {"key": "销售净利率(%)", "label": "净利率%", "desc": "", "tip": ""},
    
    # 回报率 (带专业Tooltip)
    {
        "key": "股东权益回报率(%)", "label": "ROE%", 
        "desc": "净利润 ÷ 股东权益", "tip": "巴菲特最看重的指标。<br>>15% 优秀；长期>20% 为极品。"
    },
    {
        "key": "总资产回报率(%)", "label": "ROA%", 
        "desc": "净利润 ÷ 总资产", "tip": "衡量资产综合利用效率。<br>一般行业 >5% 算不错。"
    },
    
    # 市值与股本结构
    {"key": "总市值(港元)", "label": "总市值", "desc": "", "tip": ""},
    {"key": "港股市值(港元)", "label": "港股市值", "desc": "", "tip": ""},
    {"key": "法定股本(股)", "label": "法定股本", "desc": "", "tip": ""},
    {"key": "已发行股本(股)", "label": "发行股本", "desc": "", "tip": ""},
    {"key": "已发行股本-H股(股)", "label": "H股股本", "desc": "", "tip": ""},
    {"key": "每手股", "label": "每手股", "desc": "", "tip": ""}
]

@app.get("/", response_class=HTMLResponse)
async def read_root(request: Request):
    cursor = stock_collection.find({}, {"history": 0})
    stocks = []
    
    for doc in cursor:
        latest = doc.get('latest_data', {})
        stock_item = {
            "code": doc["_id"],
            "name": doc["name"],
            "date": latest.get("date", "-")
        }
        
        for col in COLUMN_CONFIG:
            key = col["key"]
            val = latest.get(key)
            if isinstance(val, (int, float)):
                stock_item[key] = f"{val:,.2f}"
            else:
                stock_item[key] = val if val else "-"     
        stocks.append(stock_item)

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
        "stocks": stocks,
        "columns": COLUMN_CONFIG,
        "last_updated": last_time_str
    })

@app.get("/api/history/{code}")
async def get_history(code: str):
    doc = stock_collection.find_one({"_id": code})
    if not doc:
        return {"dates": [], "pe": [], "peg": [], "name": code}

    history = doc.get("history", [])
    dates = [h.get("date") for h in history]
    pe_values = []
    peg_values = []
    for h in history:
        pe = next((h[k] for k in h if "市盈率" in k or k == "PE"), None)
        pe_values.append(pe)
        peg_values.append(h.get("PEG", h.get("peg")))
    
    return {"dates": dates, "pe": pe_values, "peg": peg_values, "name": doc["name"]}

@app.get("/api/trigger_crawl")
async def trigger_crawl():
    if status.is_running:
        return {"success": False, "message": "任务正在运行中，请勿重复触发"}
    scheduler.add_job(dynamic_task_wrapper)
    return {"success": True, "message": "后台任务已启动"}

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
    print("🔄 接收到重启指令，正在触发热重载...")
    time.sleep(0.5) 
    current_file = os.path.abspath(__file__)
    if os.path.exists(current_file):
        os.utime(current_file, None)
    else:
        print("❌ 无法找到文件，热重载失败")

@app.post("/api/restart")
async def restart_service(background_tasks: BackgroundTasks):
    background_tasks.add_task(restart_program)
    return {"success": True, "message": "服务正在重载，页面将在 3 秒后刷新..."}

if __name__ == "__main__":
    uvicorn.run("main:app", host="0.0.0.0", port=8000, reload=True)