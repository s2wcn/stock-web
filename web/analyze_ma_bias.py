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

# [参数说明] 买入阈值范围 (MA60 乖离率)
# 含义：股价相对于 MA60 的偏离程度。
# 范围：-5% 到 +5%，步长 0.2%。
# 逻辑：负数代表跌破均线（抄底），正数代表回踩均线上方（右侧交易）。
BUY_RANGE = np.arange(-0.1, 0.101, 0.002)

# [参数说明] 卖出阈值范围 (MA5 乖离率)
# 含义：股价相对于 MA5 的偏离程度。
# 范围：0% 到 15%，步长 0.2%。
# 逻辑：正数越大，代表短线冲高越急，是止盈信号。
SELL_RANGE = np.arange(0.00, 0.151, 0.002)

# [参数说明] 交易手续费
# 含义：模拟港股交易的印花税、佣金等总成本。
# 0.002 代表双向各约 0.1%，即千分之二。
COMMISSION = 0.002 

# [参数说明] 并发控制
# MAX_WORKERS: 进程池大小，根据 CPU 核心数自动调整，最大不超过 4，避免卡死机器。
MAX_WORKERS = min(os.cpu_count(), 4) 
# TASK_TIMEOUT: 单个进程任务的最长执行时间（秒），防止僵死进程。
TASK_TIMEOUT = 600  

def get_bull_years(bull_label):
    if not bull_label: return 0
    if "5年" in bull_label: return 5
    if "4年" in bull_label: return 4
    if "3年" in bull_label: return 3
    if "2年" in bull_label: return 2
    if "1年" in bull_label: return 1
    return 0

# === Numba 极速回测逻辑 ===
# 注意：bias20_arr 参数名已在逻辑上改为接收 bias60 数据
@jit(nopython=True)
def backtest_numba(close_arr, bias5_arr, bias60_arr, buy_bias_threshold, sell_bias_threshold):
    """
    Numba 加速的回测内核
    :param close_arr: 收盘价数组
    :param bias5_arr: MA5 乖离率数组
    :param bias60_arr: MA60 乖离率数组 (原 MA20)
    :param buy_bias_threshold: 买入阈值 (针对 MA60)
    :param sell_bias_threshold: 卖出阈值 (针对 MA5)
    """
    capital = 10000.0  # 初始资金
    hold_shares = 0.0  # 持仓股数
    cost_price = 0.0   # 持仓成本
    in_market = False  # 是否在场内
    
    trade_count = 0
    win_count = 0
    
    n = len(close_arr)
    # [修改说明] 已取消硬性止损 (HARD_STOP_LOSS)
    # hard_stop_loss = -0.15 
    commission = 0.002
    
    for i in range(n):
        current_price = close_arr[i]
        
        # [保护] 如果价格为0 (脏数据)，跳过当天
        if current_price <= 0.0001:
            continue

        b5 = bias5_arr[i]
        b60 = bias60_arr[i] # 这里实际使用的是 MA60 的乖离率
        
        if in_market:
            # 持仓状态：检查是否卖出
            if cost_price <= 0.0001:
                # 异常数据保护：如果成本价坏了，强制卖出清零
                in_market = False
                hold_shares = 0.0
                continue
                
            current_profit = (current_price - cost_price) / cost_price
            
            # [修改说明] 卖出条件仅保留：MA5乖离率过大 (止盈)
            # 取消了 `or current_profit <= hard_stop_loss` 的止损判断
            if b5 >= sell_bias_threshold:
                revenue = hold_shares * current_price * (1 - commission)
                capital = revenue
                in_market = False
                hold_shares = 0.0
                
                trade_count += 1
                if current_profit > 0:
                    win_count += 1

        else:
            # 空仓状态：检查是否买入
            # 逻辑：当 MA60 乖离率 <= 设定的阈值 (例如 -2%) 时买入
            if b60 <= buy_bias_threshold:
                cost_after_fee = current_price * (1 + commission)
                hold_shares = capital / cost_after_fee
                cost_price = current_price
                in_market = True
                
    # 计算最终市值
    final_value = capital
    if in_market:
        final_value = hold_shares * close_arr[-1] * (1 - commission)
        
    return_pct = (final_value - 10000.0) / 10000.0 * 100
    return return_pct, trade_count, win_count

# === 数据同步逻辑 ===
def sync_qfq_history(code, name, db_collection):
    """
    [强制刷新] 获取最新的 QFQ 历史数据并覆盖数据库。
    """
    try:
        # 随机休眠防封
        time.sleep(random.uniform(0.5, 1.2))
        
        # 拉取全量数据
        df_new = ak.stock_hk_hist(
            symbol=code, 
            period="daily", 
            start_date="20180101", 
            end_date="22220101", 
            adjust="qfq"
        )
        
        if df_new is None or df_new.empty:
            doc = db_collection.find_one({"_id": code}, {"qfq_history": 1})
            existing_data = doc.get("qfq_history", []) if doc else []
            return pd.DataFrame(existing_data) if existing_data else None

        rename_map = {
            "日期": "date", "收盘": "close", "开盘": "open", 
            "最高": "high", "最低": "low", "成交量": "volume"
        }
        df_new.rename(columns=rename_map, inplace=True)
        
        if "close" not in df_new.columns:
            return None
            
        df_new['date'] = pd.to_datetime(df_new['date']).dt.strftime("%Y-%m-%d")
        
        for col in ["close", "open", "high", "low", "volume"]:
            if col in df_new.columns:
                df_new[col] = pd.to_numeric(df_new[col], errors='coerce')
        
        new_records = df_new.to_dict('records')
        
        # 强制覆盖
        db_collection.update_one(
            {"_id": code}, 
            {"$set": {"qfq_history": new_records}}
        )
        
        return df_new

    except Exception as e:
        print(f"❌ [{code}] 数据同步失败: {e}")
        doc = db_collection.find_one({"_id": code}, {"qfq_history": 1})
        existing_data = doc.get("qfq_history", []) if doc else []
        return pd.DataFrame(existing_data) if existing_data else None

# === 子进程执行函数 ===
def optimize_single_stock_process(code, name, years):
    local_client = None
    try:
        local_client = MongoClient(MONGO_URI)
        local_db = local_client[DB_NAME]
        local_collection = local_db["stocks"]

        df = sync_qfq_history(code, name, local_collection)
        
        # [修改说明] MA60 需要更多数据，至少60天，这里放宽到 100 天保险
        if df is None or len(df) < 100: return None
        if 'close' not in df.columns: return None

        # 清洗脏数据
        df['close'] = pd.to_numeric(df['close'], errors='coerce')
        df = df[df['close'] > 0.0001].copy().reset_index(drop=True)
        
        # 1. 确保日期格式
        df['date'] = pd.to_datetime(df['date'])
        if df.empty: return None

        # === [核心修改 1] 计算 MA60 指标 ===
        close_series = df['close'].astype(float)
        df['ma5'] = close_series.rolling(window=5).mean()
        # [修改] 原 MA20 改为 MA60
        df['ma60'] = close_series.rolling(window=60).mean()
        
        with np.errstate(divide='ignore', invalid='ignore'):
            df['bias_5'] = (close_series - df['ma5']) / df['ma5']
            # [修改] 计算 MA60 乖离率
            df['bias_60'] = (close_series - df['ma60']) / df['ma60']
        
        # === [核心修复 2] 定位切片点和基准价格 ===
        latest_date = df['date'].iloc[-1]
        try:
            target_start_date = latest_date - pd.DateOffset(years=years)
        except:
            target_start_date = latest_date - timedelta(days=365 * years)
        
        # 找到大于等于目标日期的所有行的索引
        mask = df['date'] >= target_start_date
        if not mask.any(): return None
        
        start_idx = mask.idxmax()
        
        # 计算基准回报的成本价
        if start_idx > 0:
            benchmark_cost = df.iloc[start_idx - 1]['close']
        else:
            benchmark_cost = df.iloc[start_idx]['open']

        # 切片用于策略回测
        df_slice = df.iloc[start_idx:].copy().reset_index(drop=True)
        
        # [修改] 清洗无效的 MA60
        df_slice.dropna(subset=['ma60', 'bias_5', 'bias_60'], inplace=True)
        df_slice.reset_index(drop=True, inplace=True)
        
        if len(df_slice) == 0: return None

        # === 准备数据 ===
        close_arr = df_slice['close'].astype(float).values
        bias5_arr = df_slice['bias_5'].astype(float).values
        # [修改] 使用 bias_60 数组
        bias60_arr = df_slice['bias_60'].astype(float).values

        # === 计算基准回报 ===
        end_price = close_arr[-1]
        
        if benchmark_cost <= 0.0001:
            benchmark_return = 0.0
        else:
            benchmark_return = (end_price - benchmark_cost) / benchmark_cost * 100

        best_result = {
            "total_return": -999,
            "benchmark_return": round(benchmark_return, 2),
            "params": {"buy_bias": 0, "sell_bias": 0},
            "metrics": {"win_rate": 0, "trades": 0}
        }
        
        # 6. 网格搜索 (Grid Search)
        for b in BUY_RANGE:
            for s in SELL_RANGE:
                # [修改] 传入 bias60_arr
                ret, trades, wins = backtest_numba(close_arr, bias5_arr, bias60_arr, float(b), float(s))
                
                if trades < 3: continue # 交易次数太少不具备统计意义
                
                if ret > best_result["total_return"]:
                    win_rate = (wins / trades * 100) if trades > 0 else 0
                    best_result.update({
                        "total_return": round(ret, 2),
                        "params": {
                            # [重要] 保持 Key 名称为 'buy_ma20_bias' 以兼容 main.py 和前端
                            # 但其数值含义已经是 MA60 的偏离度了
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
    print("🧹 正在清理非长牛股的 QFQ 历史数据...")
    try:
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
    print("🚀 开始执行【MA60 均线乖离率策略】优化 (改进版: 无止损 + MA60)...")
    
    if not check_network():
        return
    
    clean_non_bull_data()

    print(f"⚙️  CPU核心数: {os.cpu_count()} | 启用进程数: {MAX_WORKERS} | 超时: {TASK_TIMEOUT}s")
    
    client = MongoClient(MONGO_URI)
    db = client[DB_NAME]
    global_collection = db["stocks"]

    # 仅针对有长牛评级的股票进行策略优化
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
                years = get_bull_years(doc["bull_label"])
                if years == 0: return None
                
                future = loop.run_in_executor(pool, optimize_single_stock_process, code, name, years)
                
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
                    # [修改] 日志打印明确显示为 MA60
                    tqdm.write(
                        f"{icon} {name}: 策略回报 {strat_ret}% (基准 {bench_ret}%) | "
                        f"买[MA60 {p['buy_ma20_bias']}%] 卖[MA5 {p['sell_ma5_bias']}%]"
                    )
    
    client.close()
    print(f"\n✅ 完成！已更新 {update_count} 只股票的 MA60 策略参数。")

if __name__ == "__main__":
    asyncio.run(main())