import numpy as np
import pandas as pd
from scipy import stats
from datetime import datetime, timedelta
import akshare as ak
import time
import random

class AnalysisService:
    def __init__(self, db_collection, status_tracker=None):
        self.collection = db_collection
        self.status = status_tracker # 引用 crawler_state 中的 status 对象

    def analyze_trend(self):
        """
        执行长牛趋势分析的主逻辑
        """
        print("🚀 Service: 开始执行【5年长牛分级筛选】...")
        
        # 获取待分析股票列表（仅需 ID 和 Name 以及 ROE/市值 检查用的 latest_data）
        cursor = self.collection.find({}, {"_id": 1, "name": 1, "latest_data": 1})
        all_stocks = list(cursor)
        total = len(all_stocks)
        
        if self.status:
            self.status.start(total)
            self.status.message = "正在初始化趋势分析..."

        DAYS_PER_YEAR = 250        
        MIN_R_SQUARED = 0.80       
        MIN_ANNUAL_RETURN = 10.0   
        MAX_ANNUAL_RETURN = 60.0   
        MIN_TURNOVER = 5_000_000   # 日均成交额门槛
        
        # [新增] 市值门槛 100亿
        MIN_MARKET_CAP = 10_000_000_000 

        for i, doc in enumerate(all_stocks):
            if self.status and self.status.should_stop:
                self.status.finish("趋势分析已终止")
                return

            code = doc["_id"]
            name = doc.get("name", "Unknown")
            
            # 过滤 8XXXX (人民币柜台)
            if code.startswith("8"):
                continue

            latest = doc.get("latest_data", {})
            
            # === 1. 市值筛选：必须超过 100 亿 ===
            market_cap = latest.get("总市值(港元)")
            # 这里的 market_cap 已经在 crawler 阶段清洗为 float，如果为空或小于门槛，则跳过
            if market_cap is None or (isinstance(market_cap, (int, float)) and market_cap < MIN_MARKET_CAP):
                self.collection.update_one({"_id": code}, {"$unset": {"bull_label": "", "trend_analysis": ""}})
                continue

            # === 2. 基本面支撑: ROE > 0 ===
            roe = latest.get("股东权益回报率(%)")
            
            # 如果 ROE 不达标，直接清除评级并跳过
            if roe is None or (isinstance(roe, (int, float)) and roe <= 0):
                self.collection.update_one({"_id": code}, {"$unset": {"bull_label": "", "trend_analysis": ""}})
                if self.status: 
                    self.status.update(i + 1, message=f"跳过(ROE低): {name}")
                continue

            if self.status:
                self.status.update(i + 1, message=f"正在分析趋势: {name}")

            try:
                self._analyze_single_stock(code, DAYS_PER_YEAR, MIN_R_SQUARED, 
                                         MIN_ANNUAL_RETURN, MAX_ANNUAL_RETURN, MIN_TURNOVER)
                # 随机休眠防封
                time.sleep(random.uniform(0.2, 0.5))
            except Exception as e:
                print(f"⚠️ 分析 {code} 失败: {e}")
                continue

        if self.status:
            self.status.finish("趋势分析完成")
        print("✅ Service: 趋势分析任务结束")

    def _analyze_single_stock(self, code, days_per_year, min_r2, min_ret, max_ret, min_turnover):
        # 获取后复权数据以保证价格连续性
        df = ak.stock_hk_daily(symbol=code, adjust="qfq")
        
        bull_label = None  
        trend_data = {}    

        if df is not None and not df.empty:
            # === [修改] 确保日期格式为 datetime ===
            if 'date' in df.columns:
                df['date'] = pd.to_datetime(df['date'])
            
            # 预处理成交额
            if 'close' in df.columns and 'volume' in df.columns:
                df['amount_est'] = df['close'].astype(float) * df['volume'].astype(float)
            else:
                df['amount_est'] = 0

            # === [核心更新] 计算 MA50 和 MA200 ===
            # MA50 用于死叉判断，MA200 用于长期趋势支撑
            df['ma50'] = df['close'].rolling(window=50).mean()
            df['ma200'] = df['close'].rolling(window=200).mean()

            # === [策略1：趋势熔断检查] ===
            # 条件：MA50 < MA200 (死叉) 且 MA200 拐头向下 (今日比20日前低)
            # 这是一个“一票否决”的硬性条件，意味着趋势已坏
            if len(df) > 220:
                curr = df.iloc[-1]
                prev_20 = df.iloc[-20] # 取20个交易日前的状态来确认拐头
                
                # 确保数据非空
                if pd.notna(curr['ma50']) and pd.notna(curr['ma200']) and pd.notna(prev_20['ma200']):
                    is_dead_cross = curr['ma50'] < curr['ma200']
                    is_ma200_falling = curr['ma200'] < prev_20['ma200'] # 简单判断拐头向下
                    
                    if is_dead_cross and is_ma200_falling:
                        # 熔断触发：清除之前的评级（如果有），并直接返回
                        self.collection.update_one({"_id": code}, {"$unset": {"bull_label": "", "trend_analysis": ""}})
                        return

            latest_date = df['date'].iloc[-1]

            # 倒序循环：5年 -> 1年
            # 这里的循环逻辑天然支持“从收复之日起重新计算”：
            # 如果5年内有中断，5年的检查会失败；循环继续到3年，如果3年内无中断（即收复后），则评级为3年长牛。
            for year in [5, 4, 3, 2, 1]:
                try:
                    target_start_date = latest_date - pd.DateOffset(years=year)
                except:
                    target_start_date = latest_date - timedelta(days=365 * year)
                
                # 筛选大于等于目标起始日期的数据
                mask = df['date'] >= target_start_date
                if not mask.any(): continue
                
                df_subset = df[mask].copy()
                
                # === 数据覆盖度校验 ===
                if df_subset.empty: continue
                actual_start_date = df_subset['date'].iloc[0]
                if (actual_start_date - target_start_date).days > 30:
                    continue

                # 成交额过滤
                avg_turnover = df_subset['amount_est'].mean()
                if avg_turnover < min_turnover: continue 

                # === [策略2：趋势连续性检查 (MA200)] ===
                # 如果区间内出现连续 5 个交易日低于 MA200，视为趋势中断（本周期不成立）
                if self._check_ma200_interruption(df_subset):
                    continue

                y_data = df_subset['close'].astype(float).values
                if len(y_data) < 20: continue 
                if np.any(y_data <= 0): continue
                    
                x_data = np.arange(len(y_data))
                log_y_data = np.log(y_data)
                
                slope, intercept, r_value, p_value, std_err = stats.linregress(x_data, log_y_data)
                r_squared = r_value ** 2
                
                # 计算年化收益 (基于 Slope * 250交易日/年)
                annualized_return = (np.exp(slope * days_per_year) - 1) * 100
                
                if (r_squared >= min_r2 and slope > 0 and 
                    min_ret <= annualized_return <= max_ret):
                    
                    bull_label = f"长牛{year}年"
                    trend_data = {
                        "r_squared": round(r_squared, 4),
                        "annual_return_pct": round(annualized_return, 2),
                        "slope": round(slope, 6),
                        "period_years": year,
                        "avg_turnover": round(avg_turnover, 0),
                        "updated_at": datetime.now()
                    }
                    break 

        update_op = {}
        if bull_label:
            update_op["$set"] = {"bull_label": bull_label, "trend_analysis": trend_data}
        else:
            update_op["$unset"] = {"bull_label": "", "trend_analysis": ""}

        self.collection.update_one({"_id": code}, update_op)

    def _check_ma200_interruption(self, df_subset):
        """
        检查是否存在连续 5 个交易日低于 MA200 的情况。
        返回 True 表示中断（本周期不成立），False 表示通过。
        """
        # 移除 MA200 为空的行
        valid_ma = df_subset.dropna(subset=['ma200'])
        
        if valid_ma.empty:
            # 如果整个周期都没有 MA200（例如上市不满200天），视为数据不足，不进行长牛评级
            # 稳健起见，返回 True (视为中断/不满足条件)
            return True

        # 找出低于 MA200 的日子
        is_below = valid_ma['close'] < valid_ma['ma200']
        
        # 计算连续 True 的次数
        # 技巧：通过比较当前行与上一行是否不等，生成分组ID，然后按组合计
        groups = is_below.ne(is_below.shift()).cumsum()
        
        # 统计每个分组中 True 的数量
        consecutive_counts = is_below.groupby(groups).sum()
        max_consecutive = consecutive_counts.max()
        
        # 如果最大连续低于天数 >= 5，则视为趋势中断
        if max_consecutive >= 5:
            return True
            
        return False