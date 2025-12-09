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
            status.finish("任务异常")

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
            print("🛑 补全任务由用户终止")
            return

        code = doc["_id"]
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

            # 清除旧指标
            derived_keys = [
                'PEG', 'PEGY', '彼得林奇估值', '净现比', '市现率', 
                '财务杠杆', '总资产周转率', '格雷厄姆数'
            ]
            for key in derived_keys:
                item.pop(key, None)

            # 重新计算
            if pe and pe > 0 and growth and growth != 0:
                item['PEG'] = round(pe / growth, 4)

            if pe and pe > 0 and growth is not None and div_yield is not None:
                total_return = growth + div_yield
                if total_return > 0:
                    item['PEGY'] = round(pe / total_return, 4)
            
            if growth is not None and div_yield is not None:
                item['彼得林奇估值'] = round(growth + div_yield, 2)
            
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
    print("✅ 全库清洗重算完成")

@asynccontextmanager
async def lifespan(app: FastAPI):
    scheduler.add_job(dynamic_task_wrapper, 'cron', hour=17, minute=0, id='crawler_job')
    print("⏰ MongoDB 爬虫调度已启动...")
    scheduler.start()
    yield
    scheduler.shutdown()

app = FastAPI(lifespan=lifespan)
templates = Jinja2Templates(directory="templates")

# === 字段配置 (包含详细 Tooltip + 适配说明) ===
COLUMN_CONFIG = [
    # 0. 静态
    {
        "key": "所属行业", "label": "行业", 
        "desc": "公司所属行业板块", "tip": "按东财/GICS分类标准划分",
        "no_sort": True, "no_chart": True
    },
    # 1. 估值
    {
        "key": "市盈率", "label": "市盈率(PE)", 
        "desc": "回本年限", 
        "tip": (
            "<b>【公式】</b> 股价 ÷ 每股收益<br>"
            "<b>【原理】</b> 投资回本需要的年限。<br>"
            "<b>【评价】</b> 越低越好，但需警惕'价值陷阱'。<br>"
            "<b>【适配】</b> 盈利稳定的消费、医药、公用事业股。<b>不适合</b>亏损股或周期股。"
        )
    },
    {
        "key": "PEG", "label": "PEG", 
        "desc": "成长估值比", 
        "tip": (
            "<b>【公式】</b> PE ÷ (净利增长率 × 100)<br>"
            "<b>【原理】</b> 弥补PE无法反映成长性的缺陷。<br>"
            "<b>【评价】</b> < 1 低估；1-2 合理；> 2 高估。<br>"
            "<b>【适配】</b> 快速成长的科技、新能源、生物医药股。"
        )
    },
    {
        "key": "PEGY", "label": "PEGY", 
        "desc": "股息修正PEG", 
        "tip": (
            "<b>【公式】</b> PE ÷ (净利增长率 + 股息率)<br>"
            "<b>【原理】</b> 将股息视为成长的一部分，对高分红股更公平。<br>"
            "<b>【评价】</b> < 1 极具吸引力。<br>"
            "<b>【适配】</b> 兼具成长与分红的成熟企业（如格力、神华）。"
        )
    },
    {
        "key": "彼得林奇估值", "label": "彼得林奇值", 
        "desc": "林奇公允PE", 
        "tip": (
            "<b>【公式】</b> 净利增长率 + 股息率<br>"
            "<b>【原理】</b> 合理PE值应等于其(成长率+股息率)。<br>"
            "<b>【评价】</b> 若此数值 > 现价PE的1.5倍，则低估。<br>"
            "<b>【适配】</b> 稳健增长型股票。"
        )
    },
    {
        "key": "格雷厄姆数", "label": "格雷厄姆数", 
        "desc": "价值上限", 
        "tip": (
            "<b>【公式】</b> √(22.5 × EPS × 每股净资产)<br>"
            "<b>【原理】</b> 结合PE和PB的保守估值上限。<br>"
            "<b>【评价】</b> 股价 < 格雷厄姆数，具备安全边际。<br>"
            "<b>【适配】</b> 传统制造业、周期股、资产重型企业。<b>不适合</b>轻资产科技股。"
        )
    },
    {
        "key": "净现比", "label": "净现比", 
        "desc": "盈利含金量", 
        "tip": (
            "<b>【公式】</b> 每股经营现金流 ÷ EPS<br>"
            "<b>【原理】</b> 检验利润是否收到了真金白银。<br>"
            "<b>【评价】</b> > 1 优秀；< 1 需警惕纸面富贵。<br>"
            "<b>【适配】</b> 全行业通用，排雷神器。"
        )
    },
    {
        "key": "市现率", "label": "市现率", 
        "desc": "现金流估值", 
        "tip": (
            "<b>【公式】</b> 股价 ÷ 每股经营现金流<br>"
            "<b>【原理】</b> 现金流比利润更难造假，估值更严谨。<br>"
            "<b>【评价】</b> 越低越好，通常 < 10 为佳。<br>"
            "<b>【适配】</b> 折旧摊销大的重资产行业（如基建、电信）。"
        )
    },
    {
        "key": "财务杠杆", "label": "财务杠杆", 
        "desc": "权益乘数", 
        "tip": (
            "<b>【公式】</b> 总资产 ÷ 股东权益<br>"
            "<b>【原理】</b> 衡量企业负债经营的程度。<br>"
            "<b>【评价】</b> 过高=高风险，过低=资金利用率低。<br>"
            "<b>【适配】</b> 银行、地产、保险等高杠杆行业需重点关注。"
        )
    },
    {
        "key": "总资产周转率", "label": "周转率", 
        "desc": "营运能力", 
        "tip": (
            "<b>【公式】</b> 营业收入 ÷ 总资产<br>"
            "<b>【原理】</b> 衡量每一块钱资产能带来多少生意。<br>"
            "<b>【评价】</b> 越高代表资产利用效率越高。<br>"
            "<b>【适配】</b> 零售、贸易、薄利多销型企业（如沃尔玛）。"
        )
    },
    # 2. 成长
    {
        "key": "基本每股收益同比增长率", "label": "EPS同比%", 
        "desc": "盈利增速", "tip": "衡量归属股东利润的增长速度。<br><b>【适配】</b> 成长股核心指标。", "suffix": "%"
    },
    {
        "key": "营业收入同比增长率", "label": "营收同比%", 
        "desc": "规模增速", "tip": "衡量业务规模的扩张速度。<br><b>【适配】</b> 处于抢占市场阶段的企业（如互联网早期）。", "suffix": "%"
    },
    {
        "key": "营业利润率同比增长率", "label": "利润率同比%", 
        "desc": "获利能力变动", "tip": "反映产品竞争力的变化趋势。<br><b>【适配】</b> 制造业、竞争激烈的行业。", "suffix": "%"
    },
    # 3. 基础
    {"key": "基本每股收益(元)", "label": "EPS(元)", "desc": "每股所获利润", "tip": ""},
    {"key": "每股净资产(元)", "label": "BPS(元)", "desc": "每股归属权益", "tip": "若股价低于此值，称为'破净'。<br><b>【适配】</b> 银行、地产、钢铁。"},
    {"key": "每股经营现金流(元)", "label": "每股现金流", "desc": "每股进账现金", "tip": "企业的血液，比利润更重要。"},
    {
        "key": "市净率", "label": "市净率(PB)", 
        "desc": "净资产溢价", 
        "tip": "股价 ÷ 每股净资产。<br><b>【适配】</b> 银行、保险、券商、周期股。<b>不适合</b>轻资产/服务业。"
    },
    {"key": "股息率TTM(%)", "label": "股息率%", "desc": "分红回报率", "tip": "过去12个月分红总额 ÷ 市值。<br><b>【适配】</b> 长期收息党（高速公路、水电）。", "suffix": "%"},
    {"key": "每股股息TTM(港元)", "label": "每股股息", "desc": "每股分到的钱", "tip": ""},
    {"key": "派息比率(%)", "label": "派息比%", "desc": "分红慷慨度", "tip": "总分红 ÷ 总净利润。<br><b>【评价】</b> >30% 算慷慨，但过高(>100%)不可持续。", "suffix": "%"},
    {"key": "营业总收入", "label": "营收", "desc": "总生意额", "tip": ""},
    {"key": "营业总收入滚动环比增长(%)", "label": "营收环比%", "desc": "营收短期趋势", "tip": "", "suffix": "%"},
    {"key": "净利润", "label": "净利润", "desc": "最终落袋利润", "tip": ""},
    {"key": "净利润滚动环比增长(%)", "label": "净利环比%", "desc": "净利短期趋势", "tip": "", "suffix": "%"},
    {"key": "销售净利率(%)", "label": "净利率%", "desc": "产品暴利程度", "tip": "净利润 ÷ 营收。<br><b>【适配】</b> 衡量护城河深浅（茅台50%，商超2%）。", "suffix": "%"},
    {
        "key": "股东权益回报率(%)", "label": "ROE%", 
        "desc": "净资产收益率", 
        "tip": (
            "<b>【重要】</b> 巴菲特最看重的指标。<br>"
            "衡量管理层用股东的钱生钱的能力。<br>"
            "<b>【评价】</b> 长期 > 20% 为极品。<br>"
            "<b>【适配】</b> 几乎所有行业（除高杠杆强周期峰值时）。"
        ), 
        "suffix": "%"
    },
    {
        "key": "总资产回报率(%)", "label": "ROA%", 
        "desc": "总资产收益率", 
        "tip": "衡量所有资产(含负债)的综合利用效率。<br><b>【适配】</b> 制造业、重资产行业。", "suffix": "%"
    },
    {"key": "总市值(港元)", "label": "总市值", "desc": "", "tip": ""},
    {"key": "港股市值(港元)", "label": "港股市值", "desc": "", "tip": ""},
    {"key": "法定股本(股)", "label": "法定股本", "desc": "", "tip": "", "no_sort": True, "no_chart": True},
    {"key": "已发行股本(股)", "label": "发行股本", "desc": "", "tip": "", "no_sort": True, "no_chart": True},
    {"key": "已发行股本-H股(股)", "label": "H股股本", "desc": "", "tip": "", "no_sort": True, "no_chart": True},
    {"key": "每手股", "label": "每手股", "desc": "", "tip": "", "no_sort": True, "no_chart": True},
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
            "date": latest.get("date", "-"),
            "intro": doc.get("intro") or latest.get("企业简介", ""),
            "is_ggt": doc.get("is_ggt", False)
        }
        
        for col in COLUMN_CONFIG:
            key = col["key"]
            val = latest.get(key)
            if isinstance(val, (int, float)):
                stock_item[key] = val
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
        return {"name": code, "history": []}
    return {"name": doc["name"], "history": doc.get("history", [])}

@app.get("/api/trigger_crawl")
async def trigger_crawl():
    if status.is_running:
        return {"success": False, "message": "任务正在运行中，请勿重复触发"}
    scheduler.add_job(dynamic_task_wrapper)
    return {"success": True, "message": "后台任务已启动"}

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