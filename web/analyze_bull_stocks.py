import akshare as ak
import pandas as pd
import numpy as np
from scipy import stats
import time
import random
from datetime import datetime
from tqdm import tqdm  # 进度条库，如果没有请 pip install tqdm

# 引入数据库连接
from database import stock_collection

# === 1. 参数配置 ===
ANALYSIS_DAYS = 250        # 分析周期：250交易日 (约1年)
MIN_R_SQUARED = 0.80       # 拟合度阈值：大于0.8才算稳
MIN_ANNUAL_RETURN = 10.0   # 最低年化收益率 (%)
MAX_ANNUAL_RETURN = 60.0   # 最高年化收益率 (%)，剔除短期妖股
WAIT_TIME = (0.5, 1.5)     # 随机等待时间(秒)，防封

def analyze_single_stock(code):
    """
    对单只股票进行长周期趋势分析
    """
    try:
        # 必须使用前复权 (qfq)，否则分红除权会破坏K线连续性
        df = ak.stock_hk_daily(symbol=code, adjust="qfq")
        
        if df is None or df.empty:
            return None
        
        # 确保数据量足够
        if len(df) < ANALYSIS_DAYS * 0.8: # 允许少量数据缺失
            return None

        # 截取最近 N 天
        df_subset = df.iloc[-ANALYSIS_DAYS:].copy()
        
        # 准备回归数据
        y_data = df_subset['close'].astype(float).values
        x_data = np.arange(len(y_data))
        
        # 避免价格为0或负数导致的log错误
        if np.any(y_data <= 0): 
            return None
            
        # 核心算法：对数线性回归 (Log-Linear Regression)
        # log(Price) = Slope * Time + Intercept
        log_y_data = np.log(y_data)
        slope, intercept, r_value, p_value, std_err = stats.linregress(x_data, log_y_data)
        
        # 计算指标
        r_squared = r_value ** 2
        
        # 年化收益率推算: (e^(slope * 250) - 1) * 100%
        annualized_return = (np.exp(slope * 250) - 1) * 100
        
        return {
            "r_squared": round(r_squared, 4),
            "slope": round(slope, 6),
            "annualized_return": round(annualized_return, 2),
            "data_count": len(df_subset)
        }
        
    except Exception as e:
        # 忽略个别网络错误，不中断循环
        return None

def run_analysis_task():
    print(f"🚀 开始执行【长牛趋势筛选】任务...")
    print(f"⚙️  配置: 周期={ANALYSIS_DAYS}天, 稳定性阈值 R2 > {MIN_R_SQUARED}")
    
    # 1. 获取所有股票代码
    cursor = stock_collection.find({}, {"_id": 1, "name": 1})
    all_stocks = list(cursor)
    total = len(all_stocks)
    
    print(f"📊 待分析股票总数: {total}")
    
    bull_count = 0
    updated_count = 0
    
    # 使用 tqdm 显示进度条
    for i, doc in enumerate(tqdm(all_stocks, desc="Analyzing", unit="stock")):
        code = doc["_id"]
        name = doc.get("name", "Unknown")
        
        # 执行分析
        metrics = analyze_single_stock(code)
        
        is_bull = False
        analysis_result = {}
        
        if metrics:
            # 判断是否符合“长牛”标准
            if (metrics["r_squared"] >= MIN_R_SQUARED and 
                metrics["slope"] > 0 and 
                MIN_ANNUAL_RETURN <= metrics["annualized_return"] <= MAX_ANNUAL_RETURN):
                is_bull = True
                bull_count += 1
            
            analysis_result = {
                "r_squared": metrics["r_squared"],
                "annual_return_pct": metrics["annualized_return"],
                "slope": metrics["slope"],
                "updated_at": datetime.now()
            }
        
        # 更新数据库
        # 即使不符合，也更新 trend_analysis 字段（记录 R2 等数据以便查看），但标记 is_slow_bull 为 False
        update_doc = {
            "$set": {
                "trend_analysis": analysis_result,
                "is_slow_bull": is_bull
            }
        }
        
        stock_collection.update_one({"_id": code}, update_doc)
        updated_count += 1
        
        # 随机延时，保护接口
        time.sleep(random.uniform(*WAIT_TIME))
        
    print("\n" + "="*40)
    print(f"🎉 分析完成！")
    print(f"✅ 成功遍历: {updated_count} 只")
    print(f"🐂 发现长牛股: {bull_count} 只")
    print("="*40)

if __name__ == "__main__":
    run_analysis_task()