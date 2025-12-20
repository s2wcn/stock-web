import akshare as ak
import pandas as pd
import numpy as np
import time
import random
from tqdm import tqdm
from database import stock_collection  # 复用你的数据库连接

# === 1. 参数配置区域 ===

# 买入阈值范围 (针对 20日线): 从 -10% 到 +2%，步长 0.5%
# 含义：负数代表跌破均线买入，正数代表回踩均线附近买入
BUY_RANGE = np.arange(-0.10, 0.021, 0.005)

# 卖出阈值范围 (针对 5日线): 从 0% 到 +15%，步长 0.5%
# 含义：股价超过5日线多少时止盈
SELL_RANGE = np.arange(0.00, 0.151, 0.005)

# 基础风控：固定止损 (防止单笔极度深套)
HARD_STOP_LOSS = -0.15 
# 交易费率
COMMISSION = 0.002 

def get_bull_period_days(bull_label):
    if not bull_label: return 0
    if "5年" in bull_label: return 250 * 5
    if "4年" in bull_label: return 250 * 4
    if "3年" in bull_label: return 250 * 3
    if "2年" in bull_label: return 250 * 2
    if "1年" in bull_label: return 250 * 1
    return 0

def backtest_ma_bias(df, buy_bias_threshold, sell_bias_threshold):
    """
    均线乖离策略回测
    buy_bias_threshold: 针对MA20的偏离阈值 (如 -0.02)
    sell_bias_threshold: 针对MA5的偏离阈值 (如 0.05)
    """
    capital = 10000.0
    hold_shares = 0
    cost_price = 0
    in_market = False
    
    trade_count = 0
    win_count = 0
    
    # 遍历每一天 (从数据足够计算MA的那一天开始)
    for i in range(len(df)):
        row = df.iloc[i]
        
        # 必须有均线数据才能交易
        if pd.isna(row['ma20']) or pd.isna(row['ma5']):
            continue
            
        current_price = row['close']
        
        # 1. 持仓状态：检查卖出
        if in_market:
            # 策略卖出：偏离5日线过大 或 触发硬止损
            current_profit = (current_price - cost_price) / cost_price
            
            if row['bias_5'] >= sell_bias_threshold or current_profit <= HARD_STOP_LOSS:
                # 执行卖出
                revenue = hold_shares * current_price * (1 - COMMISSION)
                capital = revenue
                in_market = False
                hold_shares = 0
                
                trade_count += 1
                if current_profit > 0: win_count += 1

        # 2. 空仓状态：检查买入
        else:
            # 策略买入：踩到 20日线特定位置
            if row['bias_20'] <= buy_bias_threshold:
                cost_after_fee = current_price * (1 + COMMISSION)
                hold_shares = capital / cost_after_fee
                cost_price = current_price
                in_market = True
                
    # 结算最后一天
    final_value = capital
    if in_market:
        final_value = hold_shares * df.iloc[-1]['close'] * (1 - COMMISSION)
        
    return_pct = (final_value - 10000.0) / 10000.0 * 100
    return return_pct, trade_count, win_count

def optimize_single_stock(code, name, days):
    try:
        df = ak.stock_hk_daily(symbol=code, adjust="qfq")
        if df is None or len(df) < 60: return None
        
        # 截取长牛周期
        df = df.iloc[-days:].copy().reset_index(drop=True)
        
        # === 预计算均线和乖离率 ===
        df['ma5'] = df['close'].rolling(window=5).mean()
        df['ma20'] = df['close'].rolling(window=20).mean()
        
        df['bias_5'] = (df['close'] - df['ma5']) / df['ma5']
        df['bias_20'] = (df['close'] - df['ma20']) / df['ma20']
        
        # 剔除前期均线计算导致的 NaN
        df.dropna(subset=['ma20'], inplace=True)
        df.reset_index(drop=True, inplace=True)
        
        # === [新增] 计算基准回报率 (Buy & Hold) ===
        if len(df) > 0:
            start_price = df.iloc[0]['close']
            end_price = df.iloc[-1]['close']
            # 基准回报 = (终价 - 始价) / 始价
            benchmark_return = (end_price - start_price) / start_price * 100
        else:
            benchmark_return = 0

        best_result = {
            "total_return": -999,
            "benchmark_return": round(benchmark_return, 2), # 存储基准回报
            "params": {"buy_bias": 0, "sell_bias": 0},
            "metrics": {"win_rate": 0, "trades": 0}
        }
        
        # === 网格搜索 ===
        for b in BUY_RANGE:
            for s in SELL_RANGE:
                ret, trades, wins = backtest_ma_bias(df, b, s)
                
                # 过滤：必须有一定交易次数，避免偶然
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
            
        return best_result

    except Exception as e:
        print(f"❌ {code} 计算出错: {e}")
        return None

def main():
    print("🚀 开始执行【均线乖离率策略】优化...")
    print("对比: 策略回报率 (高抛低吸) vs 基准回报率 (持有不动)")
    
    # 查找长牛股
    query = {"bull_label": {"$exists": True, "$ne": None}}
    cursor = stock_collection.find(query, {"_id": 1, "name": 1, "bull_label": 1})
    stocks = list(cursor)
    
    print(f"📊 待分析股票: {len(stocks)} 只")
    
    update_count = 0
    for doc in tqdm(stocks, desc="Optimizing"):
        code = doc["_id"]
        name = doc["name"]
        
        days = get_bull_period_days(doc["bull_label"])
        if days == 0: continue
        
        res = optimize_single_stock(code, name, days)
        
        if res:
            # 存入数据库
            stock_collection.update_one(
                {"_id": code},
                {"$set": {"ma_strategy": res}}
            )
            update_count += 1
            
            # [修改] 打印双重回报率
            strat_ret = res["total_return"]
            bench_ret = res["benchmark_return"]
            p = res["params"]
            
            # 只有策略回报 > 30% 且 交易次数合理才显示
            if strat_ret > 30:
                # 添加一个简单的评价图标
                icon = "🔥" if strat_ret > bench_ret else "🐢"
                
                tqdm.write(
                    f"{icon} {name}: 策略回报 {strat_ret}% (基准 {bench_ret}%) | "
                    f"买[MA20 {p['buy_ma20_bias']}%], 卖[MA5 {p['sell_ma5_bias']}%]"
                )
                
        time.sleep(random.uniform(0.1, 0.3))
        
    print(f"\n✅ 完成！已更新 {update_count} 只股票的均线策略参数。")

if __name__ == "__main__":
    main()