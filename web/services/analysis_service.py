import numpy as np
from scipy import stats
from datetime import datetime
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
        
        # 获取待分析股票列表（仅需 ID 和 Name 以及 ROE 检查用的 latest_data）
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

        for i, doc in enumerate(all_stocks):
            if self.status and self.status.should_stop:
                self.status.finish("趋势分析已终止")
                return

            code = doc["_id"]
            name = doc.get("name", "Unknown")
            
            # 过滤 8XXXX (人民币柜台)
            if code.startswith("8"):
                continue

            # 基本面支撑: ROE > 0
            latest = doc.get("latest_data", {})
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
        # 获取后复权数据以保证价格连续性，或者前复权
        df = ak.stock_hk_daily(symbol=code, adjust="qfq")
        
        bull_label = None  
        trend_data = {}    

        if df is not None and not df.empty:
            # 预处理成交额
            if 'close' in df.columns and 'volume' in df.columns:
                df['amount_est'] = df['close'].astype(float) * df['volume'].astype(float)
            else:
                df['amount_est'] = 0

            # 倒序循环：5年 -> 1年
            for year in [5, 4, 3, 2, 1]:
                required_days = year * days_per_year
                if len(df) < required_days * 0.8: continue
                
                df_subset = df.iloc[-required_days:].copy()
                
                # 成交额过滤
                avg_turnover = df_subset['amount_est'].mean()
                if avg_turnover < min_turnover: continue 

                y_data = df_subset['close'].astype(float).values
                if np.any(y_data <= 0): continue
                    
                x_data = np.arange(len(y_data))
                log_y_data = np.log(y_data)
                
                slope, intercept, r_value, p_value, std_err = stats.linregress(x_data, log_y_data)
                r_squared = r_value ** 2
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