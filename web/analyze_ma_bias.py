import akshare as ak
import pandas as pd
import numpy as np
import time
import random
import asyncio
import os
from datetime import datetime, timedelta
from concurrent.futures import ProcessPoolExecutor
from tqdm import tqdm
from numba import jit
from pymongo import MongoClient 

# 仅导入配置，用于子进程重建连接
from database import MONGO_URI, DB_NAME

# === 1. 参数配置区域 ===

BUY_RANGE = np.arange(-0.10, 0.021, 0.005)
SELL_RANGE = np.arange(0.00, 0.151, 0.005)
HARD_STOP_LOSS = -0.15 
COMMISSION = 0.002 

MAX_WORKERS = min(os.cpu_count(), 4) 
TASK_TIMEOUT = 600  

def get_bull_period_days(bull_label):
    if not bull_label: return 0
    if "5年" in bull_label: return 250 * 5
    if "4年" in bull_label: return 250 * 4
    if "3年" in bull_label: return 250 * 3
    if "2年" in bull_label: return 250 * 2
    if "1年" in bull_label: return 250 * 1
    return 0

# === Numba 极速回测逻辑 ===
@jit(nopython=True)
def backtest_numba(close_arr, bias5_arr, bias20_arr, buy_bias_threshold, sell_bias_threshold):
    capital = 10000.0
    hold_shares = 0.0
    cost_price = 0.0
    in_market = False 
    
    trade_count = 0
    win_count = 0
    
    n = len(close_arr)
    hard_stop_loss = -0.15
    commission = 0.002
    
    for i in range(n):
        current_price = close_arr[i]
        
        # [保护] 如果价格为0 (脏数据)，跳过当天
        if current_price <= 0.0001:
            continue

        b5 = bias5_arr[i]
        b20 = bias20_arr[i]
        
        if in_market:
            # [保护] cost_price 理论上不为0，因为买入时必须有价格，但加层保险
            if cost_price <= 0.0001:
                in_market = False
                hold_shares = 0.0
                continue
                
            current_profit = (current_price - cost_price) / cost_price
            if b5 >= sell_bias_threshold or current_profit <= hard_stop_loss:
                revenue = hold_shares * current_price * (1 - commission)
                capital = revenue
                in_market = False
                hold_shares = 0.0
                
                trade_count += 1
                if current_profit > 0:
                    win_count += 1

        else:
            if b20 <= buy_bias_threshold:
                cost_after_fee = current_price * (1 + commission)
                hold_shares = capital / cost_after_fee
                cost_price = current_price
                in_market = True
                
    final_value = capital
    if in_market:
        final_value = hold_shares * close_arr[-1] * (1 - commission)
        
    return_pct = (final_value - 10000.0) / 10000.0 * 100
    return return_pct, trade_count, win_count

# === 数据同步与获取逻辑 ===
def sync_qfq_history(code, name, db_collection):
    """
    检查数据库，增量更新 QFQ 历史数据，并返回完整的 DataFrame
    """
    doc = db_collection.find_one({"_id": code}, {"qfq_history": 1})
    existing_data = doc.get("qfq_history", []) if doc else []
    
    start_date = "19700101"
    
    # 2. 检查数据是否新鲜
    if existing_data:
        last_record = existing_data[-1]
        last_date_str = last_record.get("date") 
        
        try:
            last_dt = datetime.strptime(last_date_str, "%Y-%m-%d")
            yesterday = datetime.now() - timedelta(days=1)
            # 如果最后一条数据是昨天或今天，跳过
            if last_dt.date() >= yesterday.date():
                return pd.DataFrame(existing_data)
            
            # 否则增量更新
            next_day = last_dt + timedelta(days=1)
            start_date = next_day.strftime("%Y%m%d")
            
        except Exception:
            start_date = "19700101"
            existing_data = []

    # 3. 调用 AkShare 增量抓取
    try:
        time.sleep(random.uniform(0.5, 1.5))
        
        df_new = ak.stock_hk_hist(
            symbol=code, 
            period="daily", 
            start_date=start_date, 
            end_date="22220101", 
            adjust="qfq"
        )
        
        if df_new is None or df_new.empty:
            return pd.DataFrame(existing_data) if existing_data else None

        # 4. 数据清洗
        rename_map = {
            "日期": "date", "收盘": "close", "开盘": "open", 
            "最高": "high", "最低": "low", "成交量": "volume"
        }
        df_new.rename(columns=rename_map, inplace=True)
        
        if "close" not in df_new.columns:
            return pd.DataFrame(existing_data) if existing_data else None
            
        df_new['date'] = pd.to_datetime(df_new['date']).dt.strftime("%Y-%m-%d")
        
        # [新增] 强制类型转换，防止字符串导致的错误
        for col in ["close", "open", "high", "low", "volume"]:
            if col in df_new.columns:
                df_new[col] = pd.to_numeric(df_new[col], errors='coerce')
        
        new_records = df_new.to_dict('records')
        
        # 5. 保存回数据库
        if not existing_data:
            db_collection.update_one(
                {"_id": code}, 
                {"$set": {"qfq_history": new_records}}
            )
            return df_new
        else:
            db_collection.update_one(
                {"_id": code}, 
                {"$push": {"qfq_history": {"$each": new_records}}}
            )
            return pd.DataFrame(existing_data + new_records)

    except Exception as e:
        print(f"❌ [{code}] 同步数据失败: {e}")
        return pd.DataFrame(existing_data) if existing_data else None

# === 子进程执行函数 ===
def optimize_single_stock_process(code, name, days):
    """
    [独立进程函数] 
    """
    local_client = None
    try:
        local_client = MongoClient(MONGO_URI)
        local_db = local_client[DB_NAME]
        local_collection = local_db["stocks"]

        df = sync_qfq_history(code, name, local_collection)
        
        if df is None or len(df) < 60: 
            return None
            
        # [新增] 核心修复：强力清洗脏数据 (收盘价 <= 0 的行)
        if 'close' in df.columns:
            df['close'] = pd.to_numeric(df['close'], errors='coerce')
            df = df[df['close'] > 0.0001] # 剔除 0 或负数价格
        
        if len(df) < 60:
            return None
        
        # 3. 截取分析周期
        df = df.iloc[-days:].copy().reset_index(drop=True)
        
        # 4. 预计算
        close_series = df['close'].astype(float)
        df['ma5'] = close_series.rolling(window=5).mean()
        df['ma20'] = close_series.rolling(window=20).mean()
        
        # 使用 numpy 的错误处理上下文，防止除以 0 报错 (变成 inf/nan)
        with np.errstate(divide='ignore', invalid='ignore'):
            df['bias_5'] = (close_series - df['ma5']) / df['ma5']
            df['bias_20'] = (close_series - df['ma20']) / df['ma20']
        
        df.dropna(subset=['ma20', 'bias_5', 'bias_20'], inplace=True)
        df.reset_index(drop=True, inplace=True)
        
        if len(df) == 0: return None

        # 5. 准备 Numba 数据
        close_arr = df['close'].astype(float).values
        bias5_arr = df['bias_5'].astype(float).values
        bias20_arr = df['bias_20'].astype(float).values

        # [修复] 基准回报率计算保护
        start_price = close_arr[0]
        if start_price <= 0.0001:
            benchmark_return = 0.0
        else:
            end_price = close_arr[-1]
            benchmark_return = (end_price - start_price) / start_price * 100

        best_result = {
            "total_return": -999,
            "benchmark_return": round(benchmark_return, 2),
            "params": {"buy_bias": 0, "sell_bias": 0},
            "metrics": {"win_rate": 0, "trades": 0}
        }
        
        # 6. 网格搜索
        for b in BUY_RANGE:
            for s in SELL_RANGE:
                ret, trades, wins = backtest_numba(close_arr, bias5_arr, bias20_arr, float(b), float(s))
                if trades < 3: continue 
                if ret > best_result["total_return"]:
                    win_rate = (wins / trades * 100) if trades > 0 else 0
                    best_result.update({
                        "total_return": round(ret, 2),
                        "params": {
                            "buy_ma20_bias": round(b * 100, 1), 
                            "sell_ma5_bias": round(s * 100, 1)  
                        },
                        "metrics": {
                            "win_rate": round(win_rate, 1),
                            "trades": trades
                        }
                    })
        
        if best_result["total_return"] == -999:
            return None
        
        return code, name, best_result

    except Exception as e:
        print(f"❌ [{code}] 计算进程异常: {e}")
        return None
    finally:
        if local_client:
            local_client.close()

def check_network():
    print("📡 正在进行网络连通性测试 (测试代码: 00700)...")
    try:
        test_df = ak.stock_hk_hist(symbol="00700", period="daily", start_date="20230101", end_date="20230105", adjust="qfq")
        if test_df is not None and not test_df.empty:
            print("✅ 网络测试通过！")
            return True
    except Exception as e:
        print(f"❌ 网络测试失败: {e}")
    return False

def clean_non_bull_data():
    """
    清理非长牛股的历史数据 (使用主进程连接)
    """
    print("🧹 正在清理非长牛股的 QFQ 历史数据...")
    try:
        # 这里需要临时建立连接，或者复用 global_collection 
        # 因为在 main 之前运行，确保有连接
        temp_client = MongoClient(MONGO_URI)
        temp_db = temp_client[DB_NAME]
        temp_col = temp_db["stocks"]
        
        result = temp_col.update_many(
            {"$or": [{"bull_label": {"$exists": False}}, {"bull_label": None}]},
            {"$unset": {"qfq_history": ""}}
        )
        print(f"✅ 清理完成: 删除了 {result.modified_count} 只股票的历史数据")
        temp_client.close()
    except Exception as e:
        print(f"❌ 清理失败: {e}")

async def main():
    print("🚀 开始执行【均线乖离率策略】优化 (V5: 数据清洗版)...")
    
    if not check_network():
        return
    
    clean_non_bull_data()

    print(f"⚙️  CPU核心数: {os.cpu_count()} | 启用进程数: {MAX_WORKERS} | 超时: {TASK_TIMEOUT}s")
    
    # 获取任务列表 (建立临时连接)
    client = MongoClient(MONGO_URI)
    db = client[DB_NAME]
    global_collection = db["stocks"]

    query = {"bull_label": {"$exists": True, "$ne": None}}
    cursor = global_collection.find(query, {"_id": 1, "name": 1, "bull_label": 1})
    stocks = list(cursor)
    
    print(f"📊 待分析长牛股: {len(stocks)} 只")
    
    update_count = 0
    loop = asyncio.get_running_loop()
    
    with ProcessPoolExecutor(max_workers=MAX_WORKERS) as pool:
        sem = asyncio.Semaphore(MAX_WORKERS)

        async def sem_task(doc):
            async with sem:
                code = doc["_id"]
                name = doc["name"]
                days = get_bull_period_days(doc["bull_label"])
                if days == 0: return None
                
                future = loop.run_in_executor(pool, optimize_single_stock_process, code, name, days)
                
                try:
                    result = await asyncio.wait_for(future, timeout=TASK_TIMEOUT)
                    return result
                except asyncio.TimeoutError:
                    print(f"⏰ [{code}] {name}: 任务超时 (> {TASK_TIMEOUT}s)，跳过！")
                    return None
                except Exception as e:
                    print(f"💥 [{code}] 系统级异常: {e}")
                    return None

        task_list = [sem_task(doc) for doc in stocks]
        
        for f in tqdm(asyncio.as_completed(task_list), total=len(task_list), desc="Processing"):
            res = await f
            if res:
                code, name, data = res
                global_collection.update_one({"_id": code}, {"$set": {"ma_strategy": data}})
                update_count += 1
                
                strat_ret = data["total_return"]
                bench_ret = data["benchmark_return"]
                p = data["params"]
                
                if strat_ret > 30:
                    icon = "🔥" if strat_ret > bench_ret else "🐢"
                    tqdm.write(
                        f"{icon} {name}: 策略回报 {strat_ret}% (基准 {bench_ret}%) | "
                        f"买[MA20 {p['buy_ma20_bias']}%]"
                    )
    
    client.close()
    print(f"\n✅ 完成！已更新 {update_count} 只股票的均线策略参数。")

if __name__ == "__main__":
    asyncio.run(main())