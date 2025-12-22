# 文件路径: web/services/analysis_service.py
import numpy as np
import pandas as pd
from scipy import stats
from datetime import datetime, timedelta
import akshare as ak
import time
import random
import os
from concurrent.futures import ProcessPoolExecutor
from numba import jit
from logger import analysis_logger as logger

# === Numba 优化回测内核 (独立函数) ===
@jit(nopython=True)
def backtest_numba(close_arr, bias5_arr, bias60_arr, buy_bias_threshold, sell_bias_threshold):
    capital = 10000.0
    hold_shares = 0.0
    cost_price = 0.0
    in_market = False
    
    trade_count = 0
    win_count = 0
    n = len(close_arr)
    commission = 0.002
    
    for i in range(n):
        current_price = close_arr[i]
        if current_price <= 0.0001: continue

        b5 = bias5_arr[i]
        b60 = bias60_arr[i]
        
        if in_market:
            if cost_price <= 0.0001:
                in_market = False
                hold_shares = 0.0
                continue
            current_profit = (current_price - cost_price) / cost_price
            if b5 >= sell_bias_threshold:
                revenue = hold_shares * current_price * (1 - commission)
                capital = revenue
                in_market = False
                hold_shares = 0.0
                trade_count += 1
                if current_profit > 0: win_count += 1
        else:
            if b60 <= buy_bias_threshold:
                cost_after_fee = current_price * (1 + commission)
                hold_shares = capital / cost_after_fee
                cost_price = current_price
                in_market = True
                
    final_value = capital
    if in_market:
        final_value = hold_shares * close_arr[-1] * (1 - commission)
    return_pct = (final_value - 10000.0) / 10000.0 * 100
    return return_pct, trade_count, win_count

# === 多进程 Worker 函数 (必须在类外部) ===
def _worker_optimize_stock(doc_data):
    """
    子进程执行函数：接收包含 QFQ 历史的数据字典，计算最佳参数
    """
    code = doc_data["_id"]
    name = doc_data.get("name", "")
    qfq_list = doc_data.get("qfq_history", [])
    bull_label = doc_data.get("bull_label", "")

    # 解析长牛年份
    years = 0
    if "5年" in bull_label: years = 5
    elif "4年" in bull_label: years = 4
    elif "3年" in bull_label: years = 3
    elif "2年" in bull_label: years = 2
    elif "1年" in bull_label: years = 1
    
    if years == 0 or not qfq_list: return None

    try:
        df = pd.DataFrame(qfq_list)
        if 'close' not in df.columns: return None
        
        df['close'] = pd.to_numeric(df['close'], errors='coerce')
        df = df[df['close'] > 0.0001].copy().reset_index(drop=True)
        df['date'] = pd.to_datetime(df['date'])
        
        if len(df) < 100: return None

        # 计算指标
        close_series = df['close'].astype(float)
        df['ma5'] = close_series.rolling(window=5).mean()
        df['ma60'] = close_series.rolling(window=60).mean()
        
        with np.errstate(divide='ignore', invalid='ignore'):
            df['bias_5'] = (close_series - df['ma5']) / df['ma5']
            df['bias_60'] = (close_series - df['ma60']) / df['ma60']

        # 切片
        latest_date = df['date'].iloc[-1]
        try: target_start = latest_date - pd.DateOffset(years=years)
        except: target_start = latest_date - timedelta(days=365 * years)
        
        mask = df['date'] >= target_start
        if not mask.any(): return None
        start_idx = mask.idxmax()
        
        # 基准回报
        if start_idx > 0: benchmark_cost = df.iloc[start_idx - 1]['close']
        else: benchmark_cost = df.iloc[start_idx]['open']

        df_slice = df.iloc[start_idx:].copy().reset_index(drop=True)
        df_slice.dropna(subset=['ma60', 'bias_5', 'bias_60'], inplace=True)
        
        if df_slice.empty: return None

        close_arr = df_slice['close'].astype(float).values
        bias5_arr = df_slice['bias_5'].astype(float).values
        bias60_arr = df_slice['bias_60'].astype(float).values

        benchmark_return = 0.0
        if benchmark_cost > 0.0001:
            benchmark_return = (close_arr[-1] - benchmark_cost) / benchmark_cost * 100

        # 网格搜索
        best_result = {
            "total_return": -999,
            "benchmark_return": round(benchmark_return, 2),
            "params": {"buy_ma60_bias": 0, "sell_ma5_bias": 0},
            "metrics": {"win_rate": 0, "trades": 0}
        }

        # 参数范围
        buy_range = np.arange(-0.1, 0.101, 0.002)
        sell_range = np.arange(0.00, 0.151, 0.002)

        for b in buy_range:
            for s in sell_range:
                ret, trades, wins = backtest_numba(close_arr, bias5_arr, bias60_arr, float(b), float(s))
                if trades < 3: continue
                
                if ret > best_result["total_return"]:
                    wr = (wins / trades * 100) if trades > 0 else 0
                    best_result.update({
                        "total_return": round(ret, 2),
                        "params": {
                            "buy_ma60_bias": round(b * 100, 1),
                            "sell_ma5_bias": round(s * 100, 1)
                        },
                        "metrics": {"win_rate": round(wr, 1), "trades": trades}
                    })
        
        if best_result["total_return"] == -999: return None
        return code, name, best_result

    except Exception as e:
        return None

# === Service 类 ===
class AnalysisService:
    def __init__(self, db_collection, status_tracker=None):
        self.collection = db_collection
        self.status = status_tracker

    def analyze_trend(self):
        """ 执行长牛趋势分析 (Trend Analysis) """
        logger.info("🚀 Service: 开始执行【5年长牛分级筛选】...")
        cursor = self.collection.find({}, {"_id": 1, "name": 1, "latest_data": 1})
        all_stocks = list(cursor)
        
        if self.status:
            self.status.start(len(all_stocks))
            self.status.message = "正在初始化趋势分析..."

        MIN_R_SQUARED = 0.80       
        MIN_ANNUAL_RETURN = 10.0   
        MAX_ANNUAL_RETURN = 150.0   
        MIN_TURNOVER = 50_000_000   
        MIN_MARKET_CAP = 10_000_000_000 

        for i, doc in enumerate(all_stocks):
            if self.status and self.status.should_stop: break
            
            code = doc["_id"]
            if code.startswith("8"): continue

            latest = doc.get("latest_data", {})
            mcap = latest.get("总市值(港元)")
            roe = latest.get("股东权益回报率(%)")

            # 硬性门槛筛选
            if (mcap is None or mcap < MIN_MARKET_CAP) or (roe is None or roe <= 0):
                self.collection.update_one({"_id": code}, {"$unset": {"bull_label": "", "trend_analysis": ""}})
                continue

            if self.status: self.status.update(i + 1, message=f"趋势分析: {doc.get('name')}")
            
            try:
                self._analyze_single_stock(code, MIN_R_SQUARED, MIN_ANNUAL_RETURN, MAX_ANNUAL_RETURN, MIN_TURNOVER)
            except Exception as e:
                logger.warning(f"⚠️ 分析 {code} 失败: {e}")

        logger.info("✅ Service: 趋势分析阶段完成")

    def optimize_strategies(self):
        """ [新增] 执行策略参数优化 (Strategy Optimization) """
        logger.info("🚀 Service: 开始对长牛股进行【策略参数优化】(本地计算)...")
        
        # 1. 找出所有已标记为长牛的股票，并直接取出 QFQ 历史数据
        query = {"bull_label": {"$exists": True, "$ne": None}}
        projection = {"_id": 1, "name": 1, "bull_label": 1, "qfq_history": 1}
        cursor = self.collection.find(query, projection)
        target_stocks = list(cursor)
        
        total = len(target_stocks)
        logger.info(f"📊 待优化策略的长牛股数量: {total}")
        
        if total == 0:
            logger.info("⚠️ 无长牛股，跳过策略优化")
            return

        if self.status:
            self.status.message = f"正在优化 {total} 只长牛股策略..."

        # 2. 多进程并行计算
        max_workers = min(os.cpu_count(), 4)
        updated_count = 0
        
        with ProcessPoolExecutor(max_workers=max_workers) as pool:
            # 提交任务
            results = pool.map(_worker_optimize_stock, target_stocks)
            
            # 处理结果
            for res in results:
                if self.status and self.status.should_stop: break
                
                if res:
                    code, name, strat_data = res
                    self.collection.update_one({"_id": code}, {"$set": {"ma_strategy": strat_data}})
                    updated_count += 1
                    
                    ret = strat_data["total_return"]
                    if ret > 20:
                        logger.info(f"🔥 {name}: 策略优化完成, 回报 {ret}%")
        
        logger.info(f"✅ Service: 策略优化完成，已更新 {updated_count} 只股票参数")
        if self.status: self.status.finish("全流程分析完成")

    def _analyze_single_stock(self, code, min_r2, min_ret, max_ret, min_turnover):
        # [优化] 尝试从数据库读取 QFQ 历史，减少网络请求
        doc = self.collection.find_one({"_id": code}, {"qfq_history": 1})
        df = pd.DataFrame(doc.get("qfq_history", [])) if doc else pd.DataFrame()
        
        if df.empty:
            # 只有库里没有时才联网，作为 fallback
            df = ak.stock_hk_daily(symbol=code, adjust="qfq")
        
        if df is None or df.empty: return

        # 确保格式
        if 'date' in df.columns: df['date'] = pd.to_datetime(df['date'])
        if 'close' in df.columns: df['close'] = df['close'].astype(float)
        
        # 补充 amount_est
        if 'volume' in df.columns:
            df['amount_est'] = df['close'] * df['volume'].astype(float)
        else:
            df['amount_est'] = 0

        # 计算 MA
        df['ma50'] = df['close'].rolling(window=50).mean()
        df['ma250'] = df['close'].rolling(window=250).mean()

        # 熔断检查
        if len(df) > 270:
            curr = df.iloc[-1]
            prev_20 = df.iloc[-20]
            if pd.notna(curr['ma50']) and pd.notna(curr['ma250']):
                if curr['ma50'] < curr['ma250'] and curr['ma250'] < prev_20['ma250']:
                    self.collection.update_one({"_id": code}, {"$unset": {"bull_label": "", "trend_analysis": ""}})
                    return

        latest_date = df['date'].iloc[-1]
        bull_label = None
        trend_data = {}

        for year in [5, 4, 3, 2, 1]:
            try: target_start = latest_date - pd.DateOffset(years=year)
            except: target_start = latest_date - timedelta(days=365 * year)
            
            mask = df['date'] >= target_start
            if not mask.any(): continue
            df_sub = df[mask].copy()
            
            if df_sub.empty: continue
            if (df_sub['date'].iloc[0] - target_start).days > 30: continue
            if df_sub['amount_est'].mean() < min_turnover: continue
            
            if self._check_ma250_interruption(df_sub): continue
            
            y_data = df_sub['close'].values
            if len(y_data) < 20 or np.any(y_data <= 0): continue
            
            start_ts = df_sub['date'].iloc[0]
            x_data = (df_sub['date'] - start_ts).dt.days.values / 365.25
            log_y = np.log(y_data)
            
            slope, intercept, r_value, _, _ = stats.linregress(x_data, log_y)
            r2 = r_value ** 2
            ann_ret = (np.exp(slope) - 1) * 100
            
            if r2 >= min_r2 and slope > 0 and min_ret <= ann_ret <= max_ret:
                bull_label = f"长牛{year}年"
                trend_data = {
                    "r_squared": round(r2, 4),
                    "annual_return_pct": round(ann_ret, 2),
                    "slope": round(slope, 6),
                    "period_years": year,
                    "avg_turnover": round(df_sub['amount_est'].mean(), 0),
                    "updated_at": datetime.now()
                }
                break

        if bull_label:
            self.collection.update_one({"_id": code}, {"$set": {"bull_label": bull_label, "trend_analysis": trend_data}})
        else:
            self.collection.update_one({"_id": code}, {"$unset": {"bull_label": "", "trend_analysis": ""}})

    def _check_ma250_interruption(self, df_subset):
        valid_ma = df_subset.dropna(subset=['ma250'])
        if valid_ma.empty: return True
        is_below = valid_ma['close'] < valid_ma['ma250']
        groups = is_below.ne(is_below.shift()).cumsum()
        consecutive = is_below.groupby(groups).sum()
        return consecutive.max() >= 5