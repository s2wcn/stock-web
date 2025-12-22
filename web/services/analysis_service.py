# 文件路径: web/services/analysis_service.py
import numpy as np
import pandas as pd
from scipy import stats
from datetime import datetime, timedelta
import akshare as ak
import os
import time
from concurrent.futures import ProcessPoolExecutor
from numba import jit
from typing import Optional, Dict, List, Tuple
from config import StrategyConfig, DingTalkConfig
from logger import analysis_logger as logger
from services.notification_service import DingTalkService

# === Numba 加速内核 ===
@jit(nopython=True)
def backtest_numba(
    close_arr: np.ndarray, 
    bias5_arr: np.ndarray, 
    bias60_arr: np.ndarray, 
    buy_bias_threshold: float, 
    sell_bias_threshold: float,
    commission: float,      
    initial_capital: float  
) -> Tuple[float, int, int]:
    
    capital = initial_capital
    hold_shares = 0.0
    cost_price = 0.0
    in_market = False
    
    trade_count = 0
    win_count = 0
    n = len(close_arr)
    
    for i in range(n):
        current_price = close_arr[i]
        if current_price <= 0.0001: continue 

        b5 = bias5_arr[i]
        b60 = bias60_arr[i]
        
        if in_market:
            # 卖出
            if b5 >= sell_bias_threshold:
                revenue = hold_shares * current_price * (1 - commission)
                current_profit = revenue - (hold_shares * cost_price)
                capital = revenue
                in_market = False
                hold_shares = 0.0
                trade_count += 1
                if current_profit > 0: win_count += 1
        else:
            # 买入
            if b60 <= buy_bias_threshold:
                cost_after_fee = current_price * (1 + commission)
                hold_shares = capital / cost_after_fee
                cost_price = current_price
                in_market = True
                
    final_value = capital
    if in_market:
        final_value = hold_shares * close_arr[-1] * (1 - commission)
        
    return_pct = (final_value - initial_capital) / initial_capital * 100
    return return_pct, trade_count, win_count

def _worker_optimize_stock(doc_data: Dict) -> Optional[Tuple[str, str, Dict]]:
    code = doc_data["_id"]
    qfq_list = doc_data.get("qfq_history", [])
    bull_label = doc_data.get("bull_label", "")
    
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

        close_series = df['close'].astype(float)
        df['ma_short'] = close_series.rolling(window=StrategyConfig.MA_SHORT_WINDOW).mean()
        df['ma_long'] = close_series.rolling(window=StrategyConfig.MA_LONG_WINDOW).mean()
        
        with np.errstate(divide='ignore', invalid='ignore'):
            df['bias_short'] = (close_series - df['ma_short']) / df['ma_short']
            df['bias_long'] = (close_series - df['ma_long']) / df['ma_long']

        latest_date = df['date'].iloc[-1]
        try: target_start = latest_date - pd.DateOffset(years=years)
        except: target_start = latest_date - timedelta(days=365 * years)
        
        mask = df['date'] >= target_start
        if not mask.any(): return None
        start_idx = mask.idxmax()
        
        if start_idx > 0: benchmark_cost = df.iloc[start_idx - 1]['close']
        else: benchmark_cost = df.iloc[start_idx]['open']

        df_slice = df.iloc[start_idx:].copy().reset_index(drop=True)
        df_slice.dropna(subset=['ma_long', 'bias_short', 'bias_long'], inplace=True)
        if df_slice.empty: return None

        close_arr = df_slice['close'].astype(float).values
        bias_short_arr = df_slice['bias_short'].astype(float).values
        bias_long_arr = df_slice['bias_long'].astype(float).values

        benchmark_return = 0.0
        if benchmark_cost > 0.0001:
            benchmark_return = (close_arr[-1] - benchmark_cost) / benchmark_cost * 100

        best_result = {
            "total_return": -999,
            "benchmark_return": round(benchmark_return, 2),
            "params": {"buy_ma60_bias": 0, "sell_ma5_bias": 0},
            "metrics": {"win_rate": 0, "trades": 0}
        }
        
        buy_range = np.arange(*StrategyConfig.STRAT_BUY_RANGE)
        sell_range = np.arange(*StrategyConfig.STRAT_SELL_RANGE)

        for b in buy_range:
            for s in sell_range:
                ret, trades, wins = backtest_numba(
                    close_arr, bias_short_arr, bias_long_arr, float(b), float(s),
                    StrategyConfig.STRAT_COMMISSION,
                    StrategyConfig.STRAT_INITIAL_CAPITAL
                )
                
                if trades < StrategyConfig.MIN_STRAT_TRADES: continue 
                
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
        return code, doc_data.get("name", ""), best_result

    except Exception: return None

class AnalysisService:
    def __init__(self, db_collection, status_tracker=None):
        self.collection = db_collection
        self.status = status_tracker

    def analyze_trend(self):
        """执行长牛趋势分析 (内存优化版)"""
        logger.info("🚀 Service: 开始执行【5年长牛分级筛选】(优化内存模式)...")
        
        # [优化点 1] 仅查询 ID 和 基本信息，不要一次性加载所有历史数据 (qfq_history)
        # 否则 2000 只股票 * 5年数据会瞬间撑爆内存
        cursor = self.collection.find({}, {"_id": 1, "name": 1, "latest_data": 1})
        all_basic_docs = list(cursor)
        
        total = len(all_basic_docs)
        if self.status:
            self.status.start(total)
            self.status.message = "正在进行趋势分析..."

        logger.info(f"📊 待分析股票数量: {total}")

        for i, basic_doc in enumerate(all_basic_docs):
            if self.status and self.status.should_stop: break
            
            code = basic_doc["_id"]
            if str(code).startswith("8"): continue

            if self.status: self.status.update(i + 1, message=f"分析: {basic_doc.get('name')}")
            
            # [优化点 2] 每 20 只股票强制休眠 0.1 秒，释放 CPU 给 Web 服务器，防止网页打不开
            if i % 20 == 0:
                time.sleep(0.1)

            try:
                # [优化点 3] 只有分析到当前这只股票时，才去数据库单独查它的历史数据
                # 用完即丢，保证内存占用平稳
                full_doc = self.collection.find_one({"_id": code}, {"qfq_history": 1, "latest_data": 1})
                if full_doc:
                    # 合并 basic_doc 和 full_doc (主要是为了把 name 传进去，虽然 analyze_single_stock 目前主要用 history)
                    # _analyze_single_stock 需要 qfq_history 和 latest_data
                    full_doc["name"] = basic_doc.get("name")
                    self._analyze_single_stock(full_doc)
                    
            except Exception as e:
                logger.warning(f"⚠️ 分析 {code} 失败: {e}")

        logger.info("✅ Service: 趋势分析阶段完成")

    def optimize_strategies(self):
        logger.info("🚀 Service: 开始对长牛股进行【策略参数优化】...")
        
        # 这里数据量相对较少（只有被选出的长牛股），可以直接查询
        target_stocks = list(self.collection.find({"bull_label": {"$exists": True}}))
        
        total = len(target_stocks)
        if total == 0: return

        if self.status: self.status.message = f"正在优化 {total} 只长牛股策略..."

        updated_count = 0
        with ProcessPoolExecutor(max_workers=min(os.cpu_count(), 4)) as pool:
            results = pool.map(_worker_optimize_stock, target_stocks)
            for res in results:
                if self.status and self.status.should_stop: break
                if res:
                    code, _, strat_data = res
                    self.collection.update_one({"_id": code}, {"$set": {"ma_strategy": strat_data}})
                    updated_count += 1
        
        logger.info(f"✅ Service: 策略优化完成，更新 {updated_count} 只")
        if self.status: self.status.finish("全流程分析完成")

    def check_signals_and_notify(self):
        logger.info("🔔 正在检查今日买卖信号...")
        
        query = {
            "bull_label": {"$exists": True}, 
            "ma_strategy": {"$exists": True},
            "qfq_history": {"$exists": True, "$not": {"$size": 0}}
        }
        # 限制历史数据返回数量，只取最近 100 天
        cursor = self.collection.find(query, {"_id": 1, "name": 1, "bull_label": 1, "ma_strategy": 1, "qfq_history": {"$slice": -100}})
        
        buy_signals = []
        sell_signals = []
        approach_buy_signals = []
        approach_sell_signals = []
        
        for doc in cursor:
            try:
                code = doc["_id"]
                name = doc["name"]
                strategy = doc["ma_strategy"]
                history = doc["qfq_history"]
                
                params = strategy.get("params", {})
                buy_threshold_pct = params.get("buy_ma60_bias") 
                sell_threshold_pct = params.get("sell_ma5_bias") 
                
                if buy_threshold_pct is None or sell_threshold_pct is None: continue
                
                df = pd.DataFrame(history)
                df['close'] = pd.to_numeric(df['close'], errors='coerce')
                df = df.dropna(subset=['close'])
                if len(df) < 60: continue
                
                latest = df.iloc[-1]
                latest_date = pd.to_datetime(latest['date']).strftime("%Y-%m-%d")
                
                if (datetime.now() - datetime.strptime(latest_date, "%Y-%m-%d")).days > 5:
                    continue

                ma5 = df['close'].rolling(5).mean().iloc[-1]
                ma60 = df['close'].rolling(60).mean().iloc[-1]
                close = latest['close']
                
                bias_5_pct = (close - ma5) / ma5 * 100
                bias_60_pct = (close - ma60) / ma60 * 100
                
                if bias_60_pct <= buy_threshold_pct:
                    buy_signals.append(f"- **{name}** ({code}): 现偏离 {bias_60_pct:.2f}% (破 {buy_threshold_pct}%) 🟢 买入")
                
                elif (bias_60_pct - buy_threshold_pct) <= abs(buy_threshold_pct * DingTalkConfig.APPROACH_BUFFER):
                    approach_buy_signals.append(f"- {name} ({code}): 现偏离 {bias_60_pct:.2f}% (近 {buy_threshold_pct}%)")

                if bias_5_pct >= sell_threshold_pct:
                    sell_signals.append(f"- **{name}** ({code}): 现偏离 {bias_5_pct:.2f}% (破 {sell_threshold_pct}%) 🔴 卖出")
                
                elif (sell_threshold_pct - bias_5_pct) <= abs(sell_threshold_pct * DingTalkConfig.APPROACH_BUFFER):
                    approach_sell_signals.append(f"- {name} ({code}): 现偏离 {bias_5_pct:.2f}% (近 {sell_threshold_pct}%)")

            except Exception as e:
                logger.error(f"信号检查出错 {code}: {e}")

        if any([buy_signals, sell_signals, approach_buy_signals, approach_sell_signals]):
            title = "📢 港股长牛策略信号"
            content = [f"## {title} ({datetime.now().strftime('%m-%d %H:%M')})"]
            
            if buy_signals:
                content.append("\n### 🟢 触发买入")
                content.extend(buy_signals)
            
            if sell_signals:
                content.append("\n### 🔴 触发卖出")
                content.extend(sell_signals)
                
            if approach_buy_signals:
                content.append("\n#### 📉 接近买点")
                content.extend(approach_buy_signals)

            if approach_sell_signals:
                content.append("\n#### 📈 接近卖点")
                content.extend(approach_sell_signals)
            
            DingTalkService.send_markdown(title, "\n".join(content))
        else:
            logger.info("🔕 今日无重点信号触发")

    def _analyze_single_stock(self, doc: Dict):
        code = doc["_id"]
        latest = doc.get("latest_data", {})
        mcap = latest.get("总市值(港元)")
        roe = latest.get("股东权益回报率(%)")

        if (mcap is None or mcap < StrategyConfig.MIN_MARKET_CAP) or (roe is None or roe <= 0):
            self.collection.update_one({"_id": code}, {"$unset": {"bull_label": "", "trend_analysis": ""}})
            return

        # 这里的 fetch 逻辑在优化版 analyze_trend 中已经通过单独查库获取了 qfq_history
        # 但如果是单个重算调用，可能还需要兼容
        qfq_data = doc.get("qfq_history", [])
        
        if not qfq_data:
             try:
                # 只有当数据真的没有时，才尝试联网补救
                raw_df = ak.stock_hk_daily(symbol=code, adjust="qfq")
                qfq_data = raw_df.to_dict('records') if raw_df is not None else []
             except: pass
        if not qfq_data: return

        df = pd.DataFrame(qfq_data)
        if 'date' in df.columns: df['date'] = pd.to_datetime(df['date'])
        if 'close' in df.columns: df['close'] = df['close'].astype(float)
        
        if 'volume' in df.columns:
            df['amount_est'] = df['close'] * df['volume'].astype(float)
        else:
            df['amount_est'] = 0

        df['trend_short'] = df['close'].rolling(window=StrategyConfig.TREND_MA_SHORT).mean()
        df['trend_long'] = df['close'].rolling(window=StrategyConfig.TREND_MA_LONG).mean()

        if len(df) > StrategyConfig.TREND_BREAK_CHECK_DAYS:
            curr = df.iloc[-1]
            prev_20 = df.iloc[-20]
            if pd.notna(curr['trend_short']) and pd.notna(curr['trend_long']):
                if curr['trend_short'] < curr['trend_long'] and curr['trend_long'] < prev_20['trend_long']:
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
            if df_sub['amount_est'].mean() < StrategyConfig.MIN_TURNOVER: continue
            
            if self._check_ma_interruption(df_sub): continue
            
            y_data = df_sub['close'].values
            if len(y_data) < StrategyConfig.MIN_REGRESSION_SAMPLES or np.any(y_data <= 0): continue
            
            start_ts = df_sub['date'].iloc[0]
            x_data = (df_sub['date'] - start_ts).dt.days.values / 365.25
            log_y = np.log(y_data)
            
            slope, intercept, r_value, _, _ = stats.linregress(x_data, log_y)
            r2 = r_value ** 2
            ann_ret = (np.exp(slope) - 1) * 100
            
            if r2 >= StrategyConfig.MIN_R_SQUARED and slope > 0 and \
               StrategyConfig.MIN_ANNUAL_RETURN <= ann_ret <= StrategyConfig.MAX_ANNUAL_RETURN:
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

    def _check_ma_interruption(self, df_subset):
        col_name = 'trend_long'
        valid_ma = df_subset.dropna(subset=[col_name])
        if valid_ma.empty: return True
        is_below = valid_ma['close'] < valid_ma[col_name]
        groups = is_below.ne(is_below.shift()).cumsum()
        consecutive = is_below.groupby(groups).sum()
        return consecutive.max() >= 5