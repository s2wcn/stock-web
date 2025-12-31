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
from message_templates import DingTalkTemplates 

# === 策略常量配置 (RSI 择时) ===
RSI_PERIOD = 14
RSI_BUY_THRESHOLD = 40.0       # 买入过滤：RSI必须处于弱势区 (防止接飞刀)
RSI_SELL_THRESHOLD = 75.0      # 卖出增强：RSI进入超买区可降低卖出阈值
RSI_SELL_BIAS_FACTOR = 0.8     # 如果 RSI 超买，卖出乖离率阈值打折系数 (例如原定10%卖，超买时8%就卖)

# === Numba 加速内核 (已增加 RSI 逻辑) ===
@jit(nopython=True)
def backtest_numba(
    close_arr: np.ndarray, 
    bias5_arr: np.ndarray, 
    bias60_arr: np.ndarray,
    rsi_arr: np.ndarray,   # [新增] RSI 数组
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
        current_rsi = rsi_arr[i]
        
        if in_market:
            # 卖出逻辑优化:
            # 1. 乖离率完全达标 (标准止盈)
            # 2. 或者: 乖离率达到阈值的 80% 且 RSI > 75 (超买提前止盈)
            condition_normal = b5 >= sell_bias_threshold
            condition_early = (b5 >= sell_bias_threshold * RSI_SELL_BIAS_FACTOR) and (current_rsi > RSI_SELL_THRESHOLD)
            
            if condition_normal or condition_early:
                revenue = hold_shares * current_price * (1 - commission)
                current_profit = revenue - (hold_shares * cost_price)
                capital = revenue
                in_market = False
                hold_shares = 0.0
                trade_count += 1
                if current_profit > 0: win_count += 1
        else:
            # 买入逻辑优化:
            # 1. MA60 乖离率达标 (价格足够便宜)
            # 2. 且 RSI < 40 (确认处于相对底部/弱势区，而非急跌中继)
            if b60 <= buy_bias_threshold and current_rsi < RSI_BUY_THRESHOLD:
                cost_after_fee = current_price * (1 + commission)
                hold_shares = capital / cost_after_fee
                cost_price = current_price
                in_market = True
                
    final_value = capital
    if in_market:
        final_value = hold_shares * close_arr[-1] * (1 - commission)
        
    return_pct = (final_value - initial_capital) / initial_capital * 100
    return return_pct, trade_count, win_count

# === 策略优化子进程函数 ===
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
        
        # [新增] 计算 RSI 指标 (使用 Wilder's Smoothing / EWM 算法)
        delta = close_series.diff()
        up = delta.clip(lower=0)
        down = -1 * delta.clip(upper=0)
        
        # 使用 EWM (com = period - 1) 模拟 Wilder's Smoothing，对齐主流软件
        ma_up = up.ewm(com=RSI_PERIOD - 1, adjust=False).mean()
        ma_down = down.ewm(com=RSI_PERIOD - 1, adjust=False).mean()
        
        rs = ma_up / ma_down
        df['rsi'] = 100 - (100 / (1 + rs))
        df['rsi'] = df['rsi'].fillna(50) # 填充 NaN

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
        # 确保关键列无 NaN
        df_slice.dropna(subset=['ma_long', 'bias_short', 'bias_long', 'rsi'], inplace=True)
        if df_slice.empty: return None

        close_arr = df_slice['close'].astype(float).values
        bias_short_arr = df_slice['bias_short'].astype(float).values
        bias_long_arr = df_slice['bias_long'].astype(float).values
        rsi_arr = df_slice['rsi'].astype(float).values # [新增]

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
                # [修改] 传递 rsi_arr
                ret, trades, wins = backtest_numba(
                    close_arr, bias_short_arr, bias_long_arr, rsi_arr,
                    float(b), float(s),
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
        """执行长牛趋势分析"""
        logger.info("🚀 Service: 开始执行【5年长牛分级筛选】(优化内存模式)...")
        
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
            
            if i % 20 == 0:
                time.sleep(0.1)

            try:
                full_doc = self.collection.find_one({"_id": code}, {"qfq_history": 1, "latest_data": 1})
                if full_doc:
                    full_doc["name"] = basic_doc.get("name")
                    self._analyze_single_stock(full_doc)
                    
            except Exception as e:
                logger.warning(f"⚠️ 分析 {code} 失败: {e}")

        logger.info("✅ Service: 趋势分析阶段完成")

    def optimize_strategies(self):
        logger.info("🚀 Service: 开始对长牛股进行【策略参数优化 (含RSI)】...")
        
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
        """
        检查所有长牛股的最新价格是否触发策略信号，并发送钉钉通知。
        [更新] 引入 RSI 辅助判断 (算法已对齐主流软件)
        """
        logger.info("🔔 正在检查今日买卖信号 (Enhanced with RSI)...")
        
        query = {
            "bull_label": {"$exists": True}, 
            "ma_strategy": {"$exists": True},
            "qfq_history": {"$exists": True, "$not": {"$size": 0}}
        }
        
        # 获取最近 300 天数据
        cursor = self.collection.find(query, {"_id": 1, "name": 1, "bull_label": 1, "ma_strategy": 1, "qfq_history": {"$slice": -300}})
        
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
                
                # 数据预处理
                df = pd.DataFrame(history)
                df['close'] = pd.to_numeric(df['close'], errors='coerce')
                df = df.dropna(subset=['close'])
                if len(df) < 60: continue
                
                latest = df.iloc[-1]
                latest_date_str = pd.to_datetime(latest['date']).strftime("%Y-%m-%d")
                if (datetime.now() - datetime.strptime(latest_date_str, "%Y-%m-%d")).days > 5:
                    continue

                # === 计算指标 ===
                df['ma5'] = df['close'].rolling(5).mean()
                df['ma60'] = df['close'].rolling(60).mean()

                # [新增] 计算 RSI (使用 EWM 对齐股票软件)
                delta = df['close'].diff()
                up = delta.clip(lower=0)
                down = -1 * delta.clip(upper=0)
                
                # 修正: 使用 ewm(com=13) 
                ma_up = up.ewm(com=RSI_PERIOD - 1, adjust=False).mean()
                ma_down = down.ewm(com=RSI_PERIOD - 1, adjust=False).mean()
                
                rs = ma_up / ma_down
                df['rsi'] = 100 - (100 / (1 + rs))
                df['rsi'] = df['rsi'].fillna(50)

                # === 计算辅助函数：回溯持续天数 (考虑 RSI) ===
                def get_duration_info(check_func):
                    duration_days = 0
                    start_date = latest_date_str
                    
                    for i in range(len(df) - 1, -1, -1):
                        row = df.iloc[i]
                        ma5_val = df['ma5'].iloc[i]
                        ma60_val = df['ma60'].iloc[i]
                        rsi_val = df['rsi'].iloc[i]
                        
                        if pd.isna(ma5_val) or pd.isna(ma60_val): break
                        
                        curr_bias_5 = (row['close'] - ma5_val) / ma5_val * 100
                        curr_bias_60 = (row['close'] - ma60_val) / ma60_val * 100
                        
                        if check_func(curr_bias_5, curr_bias_60, rsi_val):
                            duration_days += 1
                            start_date = pd.to_datetime(row['date']).strftime("%Y-%m-%d")
                        else:
                            break
                    return start_date, duration_days

                # 获取最新数据
                ma5_curr = df['ma5'].iloc[-1]
                ma60_curr = df['ma60'].iloc[-1]
                rsi_curr = df['rsi'].iloc[-1]
                close = float(latest['close'])
                
                bias_5_pct = (close - ma5_curr) / ma5_curr * 100
                bias_60_pct = (close - ma60_curr) / ma60_curr * 100
                
                # --- 信号判定逻辑 (同步 backtest 逻辑) ---
                
                # 1. 🟢 触发买入
                # 逻辑: 乖离率 <= 阈值 且 RSI < 40
                if bias_60_pct <= buy_threshold_pct and rsi_curr < RSI_BUY_THRESHOLD:
                    s_date, days = get_duration_info(
                        lambda b5, b60, r: b60 <= buy_threshold_pct and r < RSI_BUY_THRESHOLD
                    )
                    trigger_price = ma60_curr * (1 + buy_threshold_pct / 100)
                    
                    msg = (f"**{name} ({code})**: {s_date}触发买入\n"
                           f"  - 现价: {close:.2f} (触发价 {trigger_price:.2f})\n"
                           f"  - RSI: {rsi_curr:.1f} (<{RSI_BUY_THRESHOLD})\n"
                           f"  - 持续: {days}天")
                    buy_signals.append(msg)
                
                # 2. 📉 接近买点 (观察区) - 不强制 RSI 过滤，仅提示
                elif (bias_60_pct - buy_threshold_pct) <= abs(buy_threshold_pct * DingTalkConfig.APPROACH_BUFFER):
                    target_price = ma60_curr * (1 + buy_threshold_pct / 100)
                    msg = (f"{name} ({code}): 现价{close:.2f} 接近买点:{target_price:.2f} (RSI: {rsi_curr:.1f})")
                    approach_buy_signals.append(msg)

                # 3. 🔴 触发卖出
                # 逻辑: 乖离率 >= 阈值 OR (乖离率 >= 0.8*阈值 且 RSI > 75)
                cond_sell_normal = bias_5_pct >= sell_threshold_pct
                cond_sell_early = (bias_5_pct >= sell_threshold_pct * RSI_SELL_BIAS_FACTOR) and (rsi_curr > RSI_SELL_THRESHOLD)

                if cond_sell_normal or cond_sell_early:
                    s_date, days = get_duration_info(
                        lambda b5, b60, r: (b5 >= sell_threshold_pct) or ((b5 >= sell_threshold_pct * RSI_SELL_BIAS_FACTOR) and (r > RSI_SELL_THRESHOLD))
                    )
                    
                    reason = "标准止盈" if cond_sell_normal else f"RSI超买({rsi_curr:.1f})提前止盈"
                    trigger_price = ma5_curr * (1 + sell_threshold_pct / 100)
                    
                    msg = (f"**{name} ({code})**: {s_date}触发卖出 [{reason}]\n"
                           f"  - 现价: {close:.2f}\n"
                           f"  - 持续: {days}天")
                    sell_signals.append(msg)
                
                # 4. 📈 接近卖点
                elif (sell_threshold_pct - bias_5_pct) <= abs(sell_threshold_pct * DingTalkConfig.APPROACH_BUFFER):
                    target_price = ma5_curr * (1 + sell_threshold_pct / 100)
                    msg = (f"{name} ({code}): 现价{close:.2f} 接近卖点:{target_price:.2f} (RSI: {rsi_curr:.1f})")
                    approach_sell_signals.append(msg)

            except Exception as e:
                logger.error(f"信号检查出错 {code}: {e}")

        # 发送通知
        if any([buy_signals, sell_signals, approach_buy_signals, approach_sell_signals]):
            title, text = DingTalkTemplates.strategy_signal_report(
                buy_signals, 
                sell_signals, 
                approach_buy_signals, 
                approach_sell_signals
            )
            DingTalkService.send_markdown(title, text)
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

        qfq_data = doc.get("qfq_history", [])
        
        if not qfq_data:
             try:
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