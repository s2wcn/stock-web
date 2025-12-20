# 文件路径: web/crawler_hk.py
import akshare as ak
import pandas as pd
import time
import random
import math
import asyncio
import functools
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime, timedelta
from database import stock_collection
from crawler_state import status
# 引入集中配置
from config import NUMERIC_FIELDS

# === 全局并发配置 ===
# 建议不要设置过大，防止触发反爬或数据库连接池耗尽
EXECUTOR = ThreadPoolExecutor(max_workers=5)

async def async_ak_call(func, *args, **kwargs):
    """
    将同步的 Akshare 库调用包装为异步非阻塞调用
    """
    loop = asyncio.get_running_loop()
    pfunc = functools.partial(func, *args, **kwargs)
    return await loop.run_in_executor(EXECUTOR, pfunc)

async def async_db_call(func, *args, **kwargs):
    """
    将同步的 MongoDB 操作包装为异步非阻塞调用
    """
    loop = asyncio.get_running_loop()
    pfunc = functools.partial(func, *args, **kwargs)
    return await loop.run_in_executor(EXECUTOR, pfunc)

def check_critical_error(e):
    """
    检查是否为严重连接错误（IP被封/连接中断）
    """
    err_str = str(e)
    if "Remote end closed connection" in err_str or "Connection aborted" in err_str or "RemoteDisconnected" in err_str:
        print(f"🛑 严重错误检测: {err_str}")
        status.message = "❌ 警告：IP可能被封或连接中断，任务强制终止！"
        status.should_stop = True 
        return True
    return False

def is_derivative(name):
    if not name: return False
    keywords = ['购', '沽', '牛', '熊', '界内', '購']
    for kw in keywords:
        if kw in name:
            return True
    return False

def get_ggt_codes():
    print("📡 正在获取港股通成分股名单...")
    try:
        df = ak.stock_hk_ggt_components_em()
        if df is not None and not df.empty:
            codes = df['代码'].astype(str).tolist()
            print(f"✅ 获取到 {len(codes)} 只港股通股票")
            return set(codes)
    except Exception as e:
        print(f"⚠️ 接口获取港股通名单失败: {e} (已忽略错误，尝试加载历史数据...)")
    
    print("⚠️ 尝试从数据库加载【历史港股通数据】...")
    try:
        cursor = stock_collection.find({"is_ggt": True}, {"_id": 1})
        codes = [doc["_id"] for doc in cursor]
        if codes:
            print(f"✅ 成功加载 {len(codes)} 只历史港股通股票")
            return set(codes)
        else:
            print("⚠️ 数据库中无历史港股通记录")
    except Exception as db_e:
        print(f"❌ 读取数据库失败: {db_e}")

    return None 

def get_hk_codes_from_sina():
    print("📡 连接接口获取全市场清单...")
    try:
        df = ak.stock_hk_spot()
        if df is None or df.empty: return {}
        codes = df['代码'].astype(str).tolist()
        names = df['中文名称'].tolist()
        return dict(zip(codes, names))
    except Exception as e:
        check_critical_error(e)
        print(f"❌ 获取列表失败: {e}")
        return {}

def compute_market_performance(df, h_share_capital=None):
    """
    纯计算函数：根据 K 线 DataFrame 计算涨跌幅和换手率等指标
    """
    performance = {}
    if df is None or df.empty:
        return performance

    try:
        # 确保按日期排序
        df = df.sort_values(by="date")
        if len(df) > 45:
            df = df.iloc[-45:]

        latest_row = df.iloc[-1]
        close_val = float(latest_row["close"])
        open_val = float(latest_row["open"])
        volume_val = float(latest_row["volume"])
        
        performance["昨收"] = close_val
        performance["昨成交量"] = volume_val
        
        turnover_rate = 0.0
        if h_share_capital and h_share_capital > 0:
            try:
                turnover_rate = (volume_val / h_share_capital) * 100
            except:
                turnover_rate = 0.0
        
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
        print(f"⚠️ 计算行情指标出错: {e}")
        pass
        
    return performance

async def fetch_and_save_single_stock_async(code, name, is_ggt=None):
    if status.should_stop: return 
    if is_derivative(name): return

    try:
        # === 1. 定义并发任务组 ===

        # 任务A: 东财数据组 (包含财务、成长性、公司简介)
        async def fetch_em_group():
            try:
                # 财务指标
                df_fin = await async_ak_call(ak.stock_hk_financial_indicator_em, symbol=code)
                await asyncio.sleep(0.3) # 微小间隔
                
                # 成长能力
                df_growth = None
                try:
                    df_growth = await async_ak_call(ak.stock_hk_growth_comparison_em, symbol=code)
                except: pass
                await asyncio.sleep(0.3)

                # 公司资料 (行业)
                df_profile = None
                try:
                    df_profile = await async_ak_call(ak.stock_hk_company_profile_em, symbol=code)
                except: pass
                
                return df_fin, df_growth, df_profile
            except Exception as e:
                if check_critical_error(e): raise e
                print(f"⚠️ 获取财务数据失败 {code}: {e}")
                return None, None, None

        # 任务B: 雪球数据 (简介)
        async def fetch_xq_intro():
            try:
                # 这是一个完全不同的数据源，可以大胆并行
                df_info = await async_ak_call(ak.stock_individual_basic_info_hk_xq, symbol=code)
                if df_info is not None and not df_info.empty:
                    mask = df_info['item'] == 'comintr'
                    if not mask.empty and mask.any():
                        return str(df_info.loc[mask, 'value'].iloc[0])
            except: 
                pass
            return ""

        # 任务C: 行情数据 (日线)
        async def fetch_market_history():
            try:
                # 显式指定 adjust="" 获取不复权的真实价格用于计算昨收
                df = await async_ak_call(ak.stock_hk_daily, symbol=code, adjust="")
                return df
            except Exception as e:
                if check_critical_error(e): raise e
                return None

        # === 2. 并发执行所有请求 ===
        
        task_em = asyncio.create_task(fetch_em_group())
        task_xq = asyncio.create_task(fetch_xq_intro())
        task_market = asyncio.create_task(fetch_market_history())

        # 等待所有结果
        (df, df_growth_raw, df_profile_raw), intro_val, df_market_raw = await asyncio.gather(task_em, task_xq, task_market)

        if df is None or df.empty: return

        # === 3. 数据处理 (CPU 密集型，耗时极短，直接同步执行) ===
        
        # 3.1 处理主表日期
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

        # 3.2 提取股本 (从 EM 数据)
        h_share_capital = 0.0
        try:
            if not df.empty:
                last_row = df.iloc[-1]
                if "已发行股本-H股(股)" in last_row:
                    val = last_row["已发行股本-H股(股)"]
                    if pd.notna(val):
                        h_share_capital = float(str(val).replace(',', ''))
        except:
            h_share_capital = 0.0

        # 3.3 处理成长数据
        growth_data = {}
        if df_growth_raw is not None and not df_growth_raw.empty:
            try:
                row_growth = df_growth_raw.iloc[0]
                target_keys = ["基本每股收益同比增长率", "营业收入同比增长率", "营业利润率同比增长率"]
                for key in target_keys:
                    if key in df_growth_raw.columns:
                        val = row_growth[key]
                        if pd.notna(val) and val != "":
                            try:
                                growth_data[key] = float(str(val).replace(',', ''))
                            except:
                                growth_data[key] = val
            except: pass

        # 3.4 处理行业数据
        industry_val = ""
        if df_profile_raw is not None and not df_profile_raw.empty:
            if "所属行业" in df_profile_raw.columns:
                industry_val = str(df_profile_raw["所属行业"].iloc[0])

        # 3.5 计算行情指标
        market_data = compute_market_performance(df_market_raw, h_share_capital=h_share_capital)
        if status.should_stop: return 

        # === 4. 数据库读写 (IO 密集，放入线程池) ===
        
        # [核心修复] 这里必须是同步函数(def)，不能是异步函数(async def)
        # 因为它要在 ThreadPoolExecutor 中运行，如果加了 async，线程只会返回一个未等待的协程对象
        def update_database():
            existing_doc = stock_collection.find_one({"_id": code})
            history_map = {item["date"]: item for item in existing_doc.get("history", [])} if existing_doc else {}

            final_is_ggt = False
            if is_ggt is not None:
                final_is_ggt = is_ggt
            elif existing_doc:
                final_is_ggt = existing_doc.get("is_ggt", False)

            latest_record = {}
            
            for _, row in df.iterrows():
                row_date = row['date']
                raw_data = row.to_dict()
                new_data = {}
                
                for k, v in raw_data.items():
                    if pd.isna(v): continue
                    
                    # 健壮的数值清洗逻辑
                    should_convert = (k in NUMERIC_FIELDS)
                    clean_val = v
                    if should_convert:
                        try:
                            clean_val = float(str(v).replace(',', ''))
                        except:
                            clean_val = v
                    else:
                        if isinstance(v, str):
                             try:
                                 if "-" not in v and ":" not in v: 
                                     clean_val = float(v.replace(',', ''))
                             except:
                                 pass

                    new_data[k] = clean_val
                
                if industry_val: new_data['所属行业'] = industry_val
                if intro_val: new_data['企业简介'] = intro_val
                
                new_data["date"] = row_date

                # === 计算衍生指标 ===
                def get_v(keys):
                    for k in keys:
                        if k in new_data and isinstance(new_data[k], (int, float)):
                            return new_data[k]
                    return None

                pe = get_v(['市盈率', 'PE'])
                eps = get_v(['基本每股收益(元)', '基本每股收益'])
                bvps = get_v(['每股净资产(元)', '每股净资产'])
                growth = get_v(['净利润滚动环比增长(%)', '净利润环比增长'])
                dividend_yield = get_v(['股息率TTM(%)', '股息率'])
                ocf_ps = get_v(['每股经营现金流(元)', '每股经营现金流'])
                roe = get_v(['股东权益回报率(%)', 'ROE'])
                roa = get_v(['总资产回报率(%)', 'ROA'])
                net_margin = get_v(['销售净利率(%)', '销售净利率'])

                if "PEG" not in new_data and pe is not None and pe > 0 and growth is not None:
                    if growth != 0:
                        new_data['PEG'] = round(pe / growth, 4)

                if pe is not None and pe > 0 and growth is not None and dividend_yield is not None:
                    total_return = growth + dividend_yield
                    if total_return > 0:
                        new_data['PEGY'] = round(pe / total_return, 4)

                if growth is not None and eps is not None:
                    fair_price = eps * (8.5 + 2 * growth)
                    if fair_price > 0:
                        new_data['合理股价'] = round(fair_price, 2)

                if ocf_ps is not None and eps is not None and eps > 0:
                    new_data['净现比'] = round(ocf_ps / eps, 2)

                if pe is not None and pe > 0 and eps is not None and eps > 0 and ocf_ps is not None and ocf_ps != 0:
                    price = pe * eps
                    new_data['市现率'] = round(price / ocf_ps, 2)

                if roe is not None and roa is not None and roa != 0:
                    new_data['财务杠杆'] = round(roe / roa, 2)

                if roa is not None and net_margin is not None and net_margin != 0:
                    new_data['总资产周转率'] = round(roa / net_margin, 2)

                if eps is not None and bvps is not None:
                    val = 22.5 * eps * bvps
                    if val > 0:
                        new_data['格雷厄姆数'] = round(math.sqrt(val), 2)

                if row_date in history_map:
                    history_map[row_date].update(new_data)
                else:
                    history_map[row_date] = new_data
                
                latest_record = history_map[row_date]

            if latest_record:
                if growth_data:
                    latest_record.update(growth_data)
                if market_data:
                    latest_record.update(market_data)
                if latest_record["date"] in history_map:
                    history_map[latest_record["date"]].update(latest_record)

            sorted_history = sorted(history_map.values(), key=lambda x: x["date"])

            doc = {
                "_id": code,
                "name": name,
                "updated_at": datetime.now(),
                "latest_data": latest_record,
                "history": sorted_history,
                "industry": industry_val,
                "intro": intro_val,
                "is_ggt": final_is_ggt
            }

            stock_collection.replace_one({"_id": code}, doc, upsert=True)

        # 异步执行数据库写入 (传入同步函数)
        await async_db_call(update_database)

    except Exception as e:
        if check_critical_error(e): return
        print(f"⚠️ 处理 {code} 异常: {e}")

def run_crawler_task():
    print(f"[{datetime.now()}] 🚀 开始 MongoDB 采集任务 (HK) - 异步并发加速版...")
    
    # 清理任务 (同步执行即可，很快)
    print("🧹 正在清理 8XXXX (人民币柜台) 重复数据...")
    del_result = stock_collection.delete_many({"_id": {"$regex": "^8"}})
    print(f"✅ 已删除 {del_result.deleted_count} 条重复数据")

    # 获取代码列表 (同步)
    code_map = get_hk_codes_from_sina()
    if status.should_stop: 
        status.finish(status.message)
        return
    if not code_map: 
        status.finish("初始化失败：无法获取股票清单")
        return

    ggt_codes = get_ggt_codes()
    if ggt_codes is not None:
        print(f"⚡️ 获取到最新名单，正在批量刷新全库港股通状态...")
        try:
            ggt_list = list(ggt_codes)
            stock_collection.update_many(
                {"_id": {"$in": ggt_list}}, 
                {"$set": {"is_ggt": True}}
            )
            stock_collection.update_many(
                {"_id": {"$nin": ggt_list}}, 
                {"$set": {"is_ggt": False}}
            )
            print("✅ 全库港股通状态刷新完毕")
        except Exception as e:
            print(f"❌ 批量刷新状态出错: {e}")

    # 过滤 8 开头的股票
    all_codes = [
        (code, name) for code, name in code_map.items() 
        if not code.startswith("8")
    ]
    
    total = len(all_codes)
    print(f"📊 本次任务将抓取 {total} 只股票 (已过滤 8XXXX)...")
    
    status.start(total)

    # === 创建事件循环运行异步爬虫 ===
    async def main_crawl_loop():
        for i, (code, name) in enumerate(all_codes):
            if status.should_stop:
                print("🛑 接到停止指令，爬虫任务终止。")
                status.finish(status.message if status.message.startswith("❌") else "任务已由用户终止")
                return

            status.update(i + 1, message=f"正在处理: {name}")
            
            if ggt_codes is None:
                is_ggt_stock = None
            else:
                is_ggt_stock = code in ggt_codes

            # 异步处理单只股票
            await fetch_and_save_single_stock_async(code, name, is_ggt=is_ggt_stock)
            
            if status.should_stop: 
                break
            
            # 股票之间的间隔 (使用异步 sleep)
            await asyncio.sleep(random.uniform(0.5, 1.5))
    
    # 启动异步循环
    try:
        asyncio.run(main_crawl_loop())
    except Exception as e:
        print(f"❌ 爬虫循环异常: {e}")
        status.finish(f"循环异常: {e}")
        return
    
    if status.should_stop:
        final_msg = status.message if status.message.startswith("❌") else "任务已由用户终止"
        status.finish(final_msg)
    else:
        status.finish("采集完成")
    
    print(f"[{datetime.now()}] 🎉 采集任务结束")