# 文件路径: web/crawler_hk.py
import akshare as ak
import pandas as pd
import asyncio
import functools
import aiohttp
import random
import time
from typing import Optional, List, Dict, Any, Tuple, Set
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime
from pymongo import UpdateOne
from database import stock_collection
from crawler_state import status
from config import NUMERIC_FIELDS, SystemConfig
from logger import crawl_logger as logger

# === 线程池配置 ===
# 使用配置中的线程数
EXECUTOR = ThreadPoolExecutor(max_workers=SystemConfig.CRAWLER_MAX_WORKERS)

async def async_ak_call(func, *args, **kwargs) -> Any:
    """通用异步包装器"""
    loop = asyncio.get_running_loop()
    pfunc = functools.partial(func, *args, **kwargs)
    return await loop.run_in_executor(EXECUTOR, pfunc)

async def async_db_call(func, *args, **kwargs) -> Any:
    """通用数据库异步包装器"""
    loop = asyncio.get_running_loop()
    pfunc = functools.partial(func, *args, **kwargs)
    return await loop.run_in_executor(EXECUTOR, pfunc)

def get_ggt_codes() -> Optional[Set[str]]:
    """获取港股通标的列表"""
    logger.info("📡 正在获取港股通成分股名单...")
    try:
        df = ak.stock_hk_ggt_components_em()
        if df is not None and not df.empty:
            codes = df['代码'].astype(str).tolist()
            logger.info(f"✅ 获取到 {len(codes)} 只港股通股票")
            return set(codes)
    except Exception as e:
        logger.warning(f"⚠️ 接口获取港股通名单失败: {e} (尝试从数据库加载)")
    
    try:
        cursor = stock_collection.find({"is_ggt": True}, {"_id": 1})
        codes = [doc["_id"] for doc in cursor]
        if codes: return set(codes)
    except Exception: pass
    return None

def get_hk_codes_from_sina() -> Dict[str, str]:
    """获取港股全市场代码列表 (同步函数)"""
    df = ak.stock_hk_spot()
    if df is None or df.empty:
        raise ValueError("接口返回数据为空")
    codes = df['代码'].astype(str).tolist()
    names = df['中文名称'].tolist()
    return dict(zip(codes, names))

def check_data_freshness(threshold: float = 0.95) -> bool:
    """
    检查数据库中的数据是否已经是最新。
    """
    try:
        # 1. 获取总数 (排除8开头)
        total_count = stock_collection.count_documents({"_id": {"$not": {"$regex": "^8"}}})
        if total_count == 0: return False

        # 2. 找到最近的日期
        latest_doc = stock_collection.find_one(
            {"latest_data.date": {"$exists": True}}, 
            sort=[("latest_data.date", -1)]
        )
        if not latest_doc: return False
            
        max_date = latest_doc.get("latest_data", {}).get("date")
        if not max_date: return False

        # 3. 统计覆盖率
        fresh_count = stock_collection.count_documents({
            "latest_data.date": max_date,
            "_id": {"$not": {"$regex": "^8"}}
        })

        ratio = fresh_count / total_count
        logger.info(f"🔍 数据新鲜度检查: 最新日期 [{max_date}], 覆盖率 {fresh_count}/{total_count} ({ratio:.1%})")

        if ratio >= threshold:
            logger.info("✅ 数据已是最新，跳过爬虫阶段。")
            return True
        else:
            logger.info("⚠️ 数据覆盖率不足，准备启动爬虫...")
            return False

    except Exception as e:
        logger.error(f"❌ 新鲜度检查失败: {e}")
        return False

def compute_market_performance(df: pd.DataFrame, h_share_capital: float = 0.0) -> Dict[str, float]:
    """计算行情指标"""
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
        if h_share_capital > 0:
            try: turnover_rate = (volume_val / h_share_capital) * 100
            except: pass
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

async def fetch_single_stock_op_async(code: str, name: str, is_ggt: Optional[bool] = None) -> Optional[UpdateOne]:
    """核心爬虫逻辑"""
    if status.should_stop: return None

    try:
        # 任务A: 东财数据组
        async def fetch_em_group():
            try:
                # 使用 wait_for 增加单次请求的超时保护
                df_fin = await asyncio.wait_for(
                    async_ak_call(ak.stock_hk_financial_indicator_em, symbol=code), 
                    timeout=SystemConfig.API_TIMEOUT
                )
                await asyncio.sleep(SystemConfig.CRAWLER_REQUEST_DELAY)
                
                df_growth = None
                try: 
                    df_growth = await asyncio.wait_for(
                        async_ak_call(ak.stock_hk_growth_comparison_em, symbol=code),
                        timeout=SystemConfig.API_TIMEOUT
                    )
                except: pass
                
                df_profile = None
                try: 
                    df_profile = await asyncio.wait_for(
                        async_ak_call(ak.stock_hk_company_profile_em, symbol=code),
                        timeout=SystemConfig.API_TIMEOUT
                    )
                except: pass
                return df_fin, df_growth, df_profile
            except asyncio.TimeoutError:
                logger.warning(f"[{code}] 财务数据接口超时")
                return None, None, None
            except Exception as e:
                logger.warning(f"[{code}] 获取财务数据失败: {str(e)[:100]}")
                return None, None, None

        # 任务B: 雪球简介
        async def fetch_xq_intro():
            try:
                df_info = await asyncio.wait_for(
                    async_ak_call(ak.stock_individual_basic_info_hk_xq, symbol=code),
                    timeout=10
                )
                if df_info is not None and not df_info.empty:
                    mask = df_info['item'] == 'comintr'
                    if not mask.empty and mask.any():
                        return str(df_info.loc[mask, 'value'].iloc[0])
            except: pass
            return ""

        # 任务C: 行情数据 (日线)
        async def fetch_market_daily():
            try:
                return await asyncio.wait_for(
                    async_ak_call(ak.stock_hk_daily, symbol=code, adjust=""),
                    timeout=SystemConfig.API_TIMEOUT
                )
            except: return None

        # 任务D: 历史数据 (QFQ)
        async def fetch_qfq_history():
            try:
                return await asyncio.wait_for(
                    async_ak_call(
                        ak.stock_hk_hist, 
                        symbol=code, 
                        period="daily", 
                        start_date=SystemConfig.HISTORY_START_DATE, 
                        end_date=SystemConfig.HISTORY_END_DATE, 
                        adjust="qfq"
                    ),
                    timeout=SystemConfig.API_TIMEOUT
                )
            except: return None

        (df, df_growth_raw, df_profile_raw), intro_val, df_market_raw, df_qfq_raw = await asyncio.gather(
            fetch_em_group(), fetch_xq_intro(), fetch_market_daily(), fetch_qfq_history()
        )

        if df is None or df.empty: return None

        # === 数据清洗 ===
        date_col = None
        for col in ['日期', 'date', 'Date', '统计日期']:
            if col in df.columns:
                date_col = col
                break
        
        if date_col is None:
            df['日期'] = datetime.now().strftime("%Y-%m-%d")
            date_col = '日期'

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

        qfq_records = []
        if df_qfq_raw is not None and not df_qfq_raw.empty:
            try:
                rename_map = {"日期": "date", "收盘": "close", "开盘": "open", "最高": "high", "最低": "low", "成交量": "volume"}
                df_qfq_raw.rename(columns=rename_map, inplace=True)
                df_qfq_raw['date'] = pd.to_datetime(df_qfq_raw['date']).dt.strftime("%Y-%m-%d")
                qfq_records = df_qfq_raw.to_dict('records')
            except Exception as e:
                logger.warning(f"[{code}] QFQ历史数据清洗失败: {e}")

        # === 数据库操作构建 ===
        def prepare_db_op():
            existing_doc = stock_collection.find_one({"_id": code})
            history_map = {item["date"]: item for item in existing_doc.get("history", [])} if existing_doc else {}
            final_is_ggt = is_ggt if is_ggt is not None else existing_doc.get("is_ggt", False) if existing_doc else False
            
            latest_record = {}
            for _, row in df.iterrows():
                row_date = row['date']
                new_data = row.to_dict()
                
                for k, v in new_data.items():
                    if pd.isna(v): continue
                    should_convert = (k in NUMERIC_FIELDS)
                    clean_val = v
                    if should_convert:
                        try: clean_val = float(str(v).replace(',', ''))
                        except: clean_val = v
                    else:
                        if isinstance(v, str) and "-" not in v and ":" not in v:
                             try: clean_val = float(v.replace(',', ''))
                             except: pass
                    new_data[k] = clean_val
                
                if industry_val: new_data['所属行业'] = industry_val
                if intro_val: new_data['企业简介'] = intro_val
                new_data["date"] = row_date

                def get_v(keys):
                    for k in keys:
                        if k in new_data and isinstance(new_data[k], (int, float)): return new_data[k]
                    return None
                pe = get_v(['市盈率','PE'])
                eps = get_v(['基本每股收益(元)','基本每股收益'])
                growth = get_v(['净利润滚动环比增长(%)','净利润环比增长'])
                div_yield = get_v(['股息率TTM(%)','股息率'])
                ocf_ps = get_v(['每股经营现金流(元)','每股经营现金流'])
                
                if "PEG" not in new_data and pe and pe > 0 and growth and growth != 0: 
                    new_data['PEG'] = round(pe / growth, 4)
                if pe and pe > 0 and growth is not None and div_yield is not None:
                    tr = growth + div_yield
                    if tr > 0: new_data['PEGY'] = round(pe / tr, 4)
                if growth is not None and eps is not None:
                    fp = eps * (8.5 + 2 * growth)
                    if fp > 0: new_data['合理股价'] = round(fp, 2)
                if ocf_ps and eps and eps > 0: 
                    new_data['净现比'] = round(ocf_ps / eps, 2)

                if row_date in history_map: history_map[row_date].update(new_data)
                else: history_map[row_date] = new_data
                latest_record = history_map[row_date]

            if latest_record:
                if growth_data: latest_record.update(growth_data)
                if market_data: latest_record.update(market_data)
                if latest_record["date"] in history_map: history_map[latest_record["date"]].update(latest_record)

            sorted_history = sorted(history_map.values(), key=lambda x: x["date"])

            update_fields = {
                "name": name, "updated_at": datetime.now(), "latest_data": latest_record,
                "history": sorted_history, "industry": industry_val, "intro": intro_val, "is_ggt": final_is_ggt
            }
            if qfq_records: update_fields["qfq_history"] = qfq_records

            op = UpdateOne({"_id": code}, {"$set": update_fields}, upsert=True)
            return op

        op = await async_db_call(prepare_db_op)
        return op

    except Exception as e:
        logger.error(f"[{code}] 处理异常: {e}")
        return None

def run_crawler_task(force_update: bool = False):
    """爬虫任务主入口"""
    # === [新增] 检查数据是否最新 ===
    # 如果数据库中 95% 以上的数据日期都是最新的，且不强制更新，则跳过爬虫
    if not force_update:
        if check_data_freshness():
            return
    else:
        logger.info("🔥 用户通过指令强制启动爬虫 (忽略新鲜度检查)")

    logger.info(f"[{datetime.now()}] 🚀 开始 MongoDB 采集任务 (HK) - 稳健版...")
    stock_collection.delete_many({"_id": {"$regex": "^8"}})
    
    # === 带超时和重试的列表获取 ===
    code_map = {}
    for attempt in range(SystemConfig.API_MAX_RETRIES):
        if status.should_stop: return
        try:
            logger.info(f"📡 连接接口获取全市场清单 (第 {attempt+1} 次尝试)...")
            
            # 使用 asyncio.wait_for 强制超时熔断
            code_map = asyncio.run(asyncio.wait_for(
                async_ak_call(get_hk_codes_from_sina), 
                timeout=SystemConfig.API_TIMEOUT
            ))
            
            if code_map:
                logger.info(f"✅ 成功获取 {len(code_map)} 只港股")
                break
                
        except asyncio.TimeoutError:
            logger.warning(f"⚠️ 接口响应超时 ({SystemConfig.API_TIMEOUT}s)，等待重试...")
        except Exception as e:
            logger.warning(f"⚠️ 接口报错: {e}，等待重试...")
        
        time.sleep(3)
        
    if not code_map:
        status.finish("初始化失败: 无法连接行情接口")
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
    status.start(len(all_codes))
    
    BATCH_SIZE = 50
    async def main_crawl_loop():
        batch_ops = []
        for i, (code, name) in enumerate(all_codes):
            if status.should_stop:
                status.finish("任务终止")
                return

            if code.startswith("043") and 4330 <= int(code) <= 4339:
                status.update(i + 1, message=f"跳过: {name}")
                continue

            status.update(i + 1, message=f"处理: {name}")
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
            try: await async_db_call(stock_collection.bulk_write, batch_ops, ordered=False)
            except: pass

    try:
        asyncio.run(main_crawl_loop())
    except Exception as e:
        logger.error(f"❌ 循环异常: {e}")
        status.finish(f"异常: {e}")
        return
    
    status.finish("采集完成")
    logger.info(f"[{datetime.now()}] 🎉 采集任务结束")