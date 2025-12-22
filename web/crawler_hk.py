# 文件路径: web/crawler_hk.py
import akshare as ak
import pandas as pd
import time
import random
import math
import asyncio
import functools
import aiohttp
from pymongo import UpdateOne
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime, timedelta
from database import stock_collection
from crawler_state import status
from config import NUMERIC_FIELDS
from logger import crawl_logger as logger

# === 全局并发配置 ===
EXECUTOR = ThreadPoolExecutor(max_workers=5)

async def async_ak_call(func, *args, **kwargs):
    loop = asyncio.get_running_loop()
    pfunc = functools.partial(func, *args, **kwargs)
    return await loop.run_in_executor(EXECUTOR, pfunc)

async def async_db_call(func, *args, **kwargs):
    loop = asyncio.get_running_loop()
    pfunc = functools.partial(func, *args, **kwargs)
    return await loop.run_in_executor(EXECUTOR, pfunc)

def check_critical_error(e):
    err_str = str(e)
    if "Remote end closed connection" in err_str or "Connection aborted" in err_str or "RemoteDisconnected" in err_str:
        logger.critical(f"🛑 严重错误检测: {err_str}")
        status.message = "❌ 警告：IP可能被封或连接中断，任务强制终止！"
        status.should_stop = True 
        return True
    return False

def is_derivative(name):
    if not name: return False
    keywords = ['购', '沽', '牛', '熊', '界内', '購']
    for kw in keywords:
        if kw in name: return True
    return False

def get_ggt_codes():
    logger.info("📡 正在获取港股通成分股名单...")
    try:
        df = ak.stock_hk_ggt_components_em()
        if df is not None and not df.empty:
            codes = df['代码'].astype(str).tolist()
            logger.info(f"✅ 获取到 {len(codes)} 只港股通股票")
            return set(codes)
    except Exception as e:
        logger.warning(f"⚠️ 接口获取港股通名单失败: {e} (已忽略错误，尝试加载历史数据...)")
    
    logger.info("⚠️ 尝试从数据库加载【历史港股通数据】...")
    try:
        cursor = stock_collection.find({"is_ggt": True}, {"_id": 1})
        codes = [doc["_id"] for doc in cursor]
        if codes: return set(codes)
    except Exception as db_e:
        logger.error(f"❌ 读取数据库失败: {db_e}")
    return None 

def get_hk_codes_from_sina():
    logger.info("📡 连接接口获取全市场清单...")
    try:
        df = ak.stock_hk_spot()
        if df is None or df.empty: return {}
        codes = df['代码'].astype(str).tolist()
        names = df['中文名称'].tolist()
        return dict(zip(codes, names))
    except Exception as e:
        check_critical_error(e)
        logger.error(f"❌ 获取列表失败: {e}")
        return {}

def compute_market_performance(df, h_share_capital=None):
    performance = {}
    if df is None or df.empty: return performance

    try:
        df = df.sort_values(by="date")
        if len(df) > 45: df = df.iloc[-45:]

        latest_row = df.iloc[-1]
        close_val = float(latest_row["close"])
        open_val = float(latest_row["open"])
        volume_val = float(latest_row["volume"])
        
        performance["昨收"] = close_val
        performance["昨成交量"] = volume_val
        
        turnover_rate = 0.0
        if h_share_capital and h_share_capital > 0:
            try: turnover_rate = (volume_val / h_share_capital) * 100
            except: turnover_rate = 0.0
        
        performance["昨换手率"] = round(turnover_rate, 2)

        if len(df) >= 2:
            prev_close = float(df.iloc[-2]["close"])
            if prev_close > 0:
                pct = (close_val - prev_close) / prev_close * 100
                performance["昨涨跌幅"] = round(pct, 2)
            else:
                performance["昨涨跌幅"] = 0.0
        else:
            if open_val > 0:
                pct = (close_val - open_val) / open_val * 100
                performance["昨涨跌幅"] = round(pct, 2)
            else:
                performance["昨涨跌幅"] = 0.0
        
        total_rows = len(df)
        if total_rows >= 6:
            prev_week_close = float(df.iloc[-6]["close"])
            if prev_week_close > 0:
                pct = (close_val - prev_week_close) / prev_week_close * 100
                performance["近一周涨跌幅"] = round(pct, 2)
        
        if total_rows >= 21:
            prev_month_close = float(df.iloc[-21]["close"])
            if prev_month_close > 0:
                pct = (close_val - prev_month_close) / prev_month_close * 100
                performance["近一月涨跌幅"] = round(pct, 2)

    except Exception as e:
        logger.warning(f"⚠️ 计算行情指标出错: {e}")
        pass
    return performance

async def fetch_single_stock_op_async(code, name, is_ggt=None):
    if status.should_stop: return None
    if is_derivative(name): return None

    try:
        # === 1. 定义并发任务组 ===

        # 任务A: 东财数据组 (包含财务、成长性、公司简介)
        async def fetch_em_group():
            try:
                df_fin = await async_ak_call(ak.stock_hk_financial_indicator_em, symbol=code)
                await asyncio.sleep(0.3)
                df_growth = None
                try: df_growth = await async_ak_call(ak.stock_hk_growth_comparison_em, symbol=code)
                except: pass
                await asyncio.sleep(0.3)
                df_profile = None
                try: df_profile = await async_ak_call(ak.stock_hk_company_profile_em, symbol=code)
                except: pass
                return df_fin, df_growth, df_profile
            except Exception as e:
                if check_critical_error(e): raise e
                logger.warning(f"[{code}] 获取财务数据失败: {str(e)[:100]}")
                return None, None, None

        # 任务B: 雪球数据 (简介)
        async def fetch_xq_intro():
            try:
                df_info = await async_ak_call(ak.stock_individual_basic_info_hk_xq, symbol=code)
                if df_info is not None and not df_info.empty:
                    mask = df_info['item'] == 'comintr'
                    if not mask.empty and mask.any():
                        return str(df_info.loc[mask, 'value'].iloc[0])
            except: pass
            return ""

        # 任务C: 行情数据 (日线-不复权) - 用于计算昨日涨跌
        async def fetch_market_daily():
            try:
                # 不复权，反映真实价格
                df = await async_ak_call(ak.stock_hk_daily, symbol=code, adjust="")
                return df
            except Exception as e:
                if check_critical_error(e): raise e
                return None

        # [新增] 任务D: 历史数据 (QFQ-前复权) - 用于后续长牛回测
        # 预加载5年以上数据，一劳永逸
        async def fetch_qfq_history():
            try:
                df = await async_ak_call(
                    ak.stock_hk_hist, 
                    symbol=code, 
                    period="daily", 
                    start_date="20180101", 
                    end_date="22220101", 
                    adjust="qfq"
                )
                return df
            except Exception as e:
                # 历史数据失败不影响主流程
                logger.warning(f"[{code}] 获取QFQ历史失败: {e}")
                return None

        # === 2. 并发执行所有请求 ===
        task_em = asyncio.create_task(fetch_em_group())
        task_xq = asyncio.create_task(fetch_xq_intro())
        task_market = asyncio.create_task(fetch_market_daily())
        task_qfq = asyncio.create_task(fetch_qfq_history())

        (df, df_growth_raw, df_profile_raw), intro_val, df_market_raw, df_qfq_raw = await asyncio.gather(
            task_em, task_xq, task_market, task_qfq
        )

        if df is None or df.empty: return None

        # === 3. 数据处理 (同步) ===
        date_col = None
        for col in ['日期', 'date', 'Date', '统计日期']:
            if col in df.columns:
                date_col = col
                break
        
        if date_col is None:
            today = datetime.now().strftime("%Y-%m-%d")
            df['日期'] = today
            date_col = '日期'
            if len(df) > 1: df = df.iloc[[-1]]

        df[date_col] = pd.to_datetime(df[date_col]).dt.strftime("%Y-%m-%d")
        df.rename(columns={date_col: 'date'}, inplace=True)
        df = df.sort_values(by='date')

        h_share_capital = 0.0
        try:
            if not df.empty:
                last_row = df.iloc[-1]
                if "已发行股本-H股(股)" in last_row:
                    val = last_row["已发行股本-H股(股)"]
                    if pd.notna(val): h_share_capital = float(str(val).replace(',', ''))
        except: h_share_capital = 0.0

        growth_data = {}
        if df_growth_raw is not None and not df_growth_raw.empty:
            try:
                row_growth = df_growth_raw.iloc[0]
                target_keys = ["基本每股收益同比增长率", "营业收入同比增长率", "营业利润率同比增长率"]
                for key in target_keys:
                    if key in df_growth_raw.columns:
                        val = row_growth[key]
                        if pd.notna(val) and val != "":
                            try: growth_data[key] = float(str(val).replace(',', ''))
                            except: growth_data[key] = val
            except: pass

        industry_val = ""
        if df_profile_raw is not None and not df_profile_raw.empty:
            if "所属行业" in df_profile_raw.columns:
                industry_val = str(df_profile_raw["所属行业"].iloc[0])

        market_data = compute_market_performance(df_market_raw, h_share_capital=h_share_capital)
        if status.should_stop: return None

        # [新增] 处理 QFQ 历史数据
        qfq_records = []
        if df_qfq_raw is not None and not df_qfq_raw.empty:
            try:
                # 统一列名
                rename_map = {
                    "日期": "date", "收盘": "close", "开盘": "open", 
                    "最高": "high", "最低": "low", "成交量": "volume"
                }
                df_qfq_raw.rename(columns=rename_map, inplace=True)
                df_qfq_raw['date'] = pd.to_datetime(df_qfq_raw['date']).dt.strftime("%Y-%m-%d")
                # 转换为字典列表
                qfq_records = df_qfq_raw.to_dict('records')
            except Exception as e:
                logger.warning(f"[{code}] QFQ历史数据清洗失败: {e}")

        # === 4. 构建数据库操作 ===
        def prepare_db_op():
            existing_doc = stock_collection.find_one({"_id": code})
            history_map = {item["date"]: item for item in existing_doc.get("history", [])} if existing_doc else {}

            final_is_ggt = is_ggt if is_ggt is not None else existing_doc.get("is_ggt", False) if existing_doc else False
            latest_record = {}
            
            for _, row in df.iterrows():
                row_date = row['date']
                raw_data = row.to_dict()
                new_data = {}
                
                for k, v in raw_data.items():
                    if pd.isna(v): continue
                    should_convert = (k in NUMERIC_FIELDS)
                    clean_val = v
                    if should_convert:
                        try: clean_val = float(str(v).replace(',', ''))
                        except: clean_val = v
                    else:
                        if isinstance(v, str):
                             try:
                                 if "-" not in v and ":" not in v: 
                                     clean_val = float(v.replace(',', ''))
                             except: pass
                    new_data[k] = clean_val
                
                if industry_val: new_data['所属行业'] = industry_val
                if intro_val: new_data['企业简介'] = intro_val
                new_data["date"] = row_date

                # 计算衍生指标
                def get_v(keys):
                    for k in keys:
                        if k in new_data and isinstance(new_data[k], (int, float)): return new_data[k]
                    return None

                pe, eps, growth = get_v(['市盈率','PE']), get_v(['基本每股收益(元)','基本每股收益']), get_v(['净利润滚动环比增长(%)','净利润环比增长'])
                dividend_yield, ocf_ps = get_v(['股息率TTM(%)','股息率']), get_v(['每股经营现金流(元)','每股经营现金流'])
                
                if "PEG" not in new_data and pe and pe > 0 and growth and growth != 0: new_data['PEG'] = round(pe / growth, 4)
                if pe and pe > 0 and growth is not None and dividend_yield is not None:
                    tr = growth + dividend_yield
                    if tr > 0: new_data['PEGY'] = round(pe / tr, 4)
                if growth is not None and eps is not None:
                    fp = eps * (8.5 + 2 * growth)
                    if fp > 0: new_data['合理股价'] = round(fp, 2)
                if ocf_ps and eps and eps > 0: new_data['净现比'] = round(ocf_ps / eps, 2)

                if row_date in history_map: history_map[row_date].update(new_data)
                else: history_map[row_date] = new_data
                latest_record = history_map[row_date]

            if latest_record:
                if growth_data: latest_record.update(growth_data)
                if market_data: latest_record.update(market_data)
                if latest_record["date"] in history_map: history_map[latest_record["date"]].update(latest_record)

            sorted_history = sorted(history_map.values(), key=lambda x: x["date"])

            # [新增] 将 QFQ 历史数据也放入 $set
            update_fields = {
                "name": name,
                "updated_at": datetime.now(),
                "latest_data": latest_record,
                "history": sorted_history,
                "industry": industry_val,
                "intro": intro_val,
                "is_ggt": final_is_ggt
            }
            if qfq_records:
                update_fields["qfq_history"] = qfq_records

            op = UpdateOne({"_id": code}, {"$set": update_fields}, upsert=True)
            return op

        op = await async_db_call(prepare_db_op)
        return op

    except aiohttp.ClientError as ne:
        logger.error(f"[{code}] 网络请求异常: {ne}")
    except asyncio.TimeoutError:
        logger.warning(f"[{code}] 请求超时")
    except Exception as e:
        if check_critical_error(e): return None
        logger.error(f"[{code}] 处理异常: {e}", exc_info=True)
        return None

def run_crawler_task():
    logger.info(f"[{datetime.now()}] 🚀 开始 MongoDB 采集任务 (HK) - 增强数据版...")
    
    logger.info("🧹 正在清理 8XXXX (人民币柜台) 重复数据...")
    stock_collection.delete_many({"_id": {"$regex": "^8"}})
    
    code_map = get_hk_codes_from_sina()
    if status.should_stop or not code_map: 
        status.finish("初始化失败" if not code_map else status.message)
        return

    ggt_codes = get_ggt_codes()
    if ggt_codes:
        logger.info("⚡️ 刷新全库港股通状态...")
        try:
            l = list(ggt_codes)
            stock_collection.update_many({"_id": {"$in": l}}, {"$set": {"is_ggt": True}})
            stock_collection.update_many({"_id": {"$nin": l}}, {"$set": {"is_ggt": False}})
        except: pass

    all_codes = [(c, n) for c, n in code_map.items() if not c.startswith("8")]
    total = len(all_codes)
    logger.info(f"📊 任务目标: {total} 只股票")
    
    status.start(total)
    BATCH_SIZE = 50

    async def main_crawl_loop():
        batch_ops = []
        for i, (code, name) in enumerate(all_codes):
            if status.should_stop:
                status.finish("任务终止")
                return

            if code.startswith("043") and 4330 <= int(code) <= 4339:
                status.update(i + 1, message=f"跳过(试验计划): {name}")
                continue

            status.update(i + 1, message=f"正在处理: {name}")
            op = await fetch_single_stock_op_async(code, name, is_ggt=(code in ggt_codes if ggt_codes else None))
            
            if op: batch_ops.append(op)
            
            if len(batch_ops) >= BATCH_SIZE:
                try:
                    logger.info(f"⚡️ 提交 {len(batch_ops)} 条数据...")
                    await async_db_call(stock_collection.bulk_write, batch_ops, ordered=False)
                    batch_ops = []
                except Exception as e:
                    logger.error(f"❌ 批量写入失败: {e}")
                    batch_ops = []
            
            await asyncio.sleep(random.uniform(0.5, 1.5))
        
        if batch_ops:
            try:
                await async_db_call(stock_collection.bulk_write, batch_ops, ordered=False)
            except: pass

    try:
        asyncio.run(main_crawl_loop())
    except Exception as e:
        logger.error(f"❌ 循环异常: {e}")
        status.finish(f"异常: {e}")
        return
    
    status.finish("采集完成")
    logger.info(f"[{datetime.now()}] 🎉 采集任务结束")