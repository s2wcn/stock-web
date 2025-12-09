import akshare as ak
import pandas as pd
import time
import random
import math
from datetime import datetime
from database import stock_collection
# 引入状态管理 (确保您的项目中已有 crawler_state.py)
from crawler_state import status

# === 1. 定义需要清洗为数字的基础字段 (原有的所有字段) ===
NUMERIC_FIELDS = [
    "基本每股收益(元)", "每股净资产(元)", "法定股本(股)", "每手股", 
    "每股股息TTM(港元)", "派息比率(%)", "已发行股本(股)", "已发行股本-H股(股)", 
    "每股经营现金流(元)", "股息率TTM(%)", "总市值(港元)", "港股市值(港元)", 
    "营业总收入", "营业总收入滚动环比增长(%)", "销售净利率(%)", "净利润", 
    "净利润滚动环比增长(%)", "股东权益回报率(%)", "市盈率", "PEG", "市净率", 
    "总资产回报率(%)"
]

def get_hk_codes_from_sina():
    print("📡 连接接口获取全市场清单...")
    try:
        df = ak.stock_hk_spot()
        if df is None or df.empty: return {}
        codes = df['代码'].astype(str).tolist()
        names = df['中文名称'].tolist()
        return dict(zip(codes, names))
    except Exception as e:
        print(f"❌ 获取列表失败: {e}")
        return {}

def fetch_and_save_single_stock(code, name):
    try:
        # 1. 抓取数据
        df = ak.stock_hk_financial_indicator_em(symbol=code)
        if df is None or df.empty: return

        # 2. 寻找日期列
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
        df = df.sort_values(by=date_col)

        # 读取现有数据
        existing_doc = stock_collection.find_one({"_id": code})
        history_map = {item["date"]: item for item in existing_doc.get("history", [])} if existing_doc else {}

        latest_record = {}
        
        for _, row in df.iterrows():
            row_date = row[date_col]
            raw_data = row.to_dict()
            new_data = {}
            
            # === 基础数据清洗 (保留所有原字段) ===
            for k, v in raw_data.items():
                if pd.isna(v): continue
                # 尝试将数字型的字符串(如 "1,000")转为 float
                if k in NUMERIC_FIELDS:
                    try:
                        new_data[k] = float(str(v).replace(',', ''))
                    except:
                        new_data[k] = v
                else:
                    new_data[k] = v
            
            new_data["date"] = row_date

            # === 辅助函数：安全获取浮点数 ===
            def get_v(keys):
                for k in keys:
                    if k in new_data and isinstance(new_data[k], (int, float)):
                        return new_data[k]
                return None

            # 获取计算所需的基础变量
            pe = get_v(['市盈率', 'PE'])
            eps = get_v(['基本每股收益(元)', '基本每股收益'])
            bvps = get_v(['每股净资产(元)', '每股净资产'])
            growth = get_v(['净利润滚动环比增长(%)', '净利润环比增长'])
            dividend_yield = get_v(['股息率TTM(%)', '股息率'])
            ocf_ps = get_v(['每股经营现金流(元)', '每股经营现金流'])
            roe = get_v(['股东权益回报率(%)', 'ROE'])
            roa = get_v(['总资产回报率(%)', 'ROA'])
            net_margin = get_v(['销售净利率(%)', '销售净利率'])

            # === 新增公式计算 ===

            # 0. PEG (原有)
            if "PEG" not in new_data and pe is not None and growth is not None:
                if growth != 0:
                    new_data['PEG'] = round(pe / growth, 4)

            # 1. PEGY Ratio
            if pe is not None and growth is not None and dividend_yield is not None:
                total_return = growth + dividend_yield
                if total_return > 0:
                    new_data['PEGY'] = round(pe / total_return, 4)

            # 2. 彼得林奇估值
            if growth is not None and dividend_yield is not None:
                new_data['彼得林奇估值'] = round(growth + dividend_yield, 2)

            # 3. 净现比
            if ocf_ps is not None and eps is not None and eps != 0:
                new_data['净现比'] = round(ocf_ps / eps, 2)

            # 4. 市现率 (P/CF)
            if pe is not None and eps is not None and ocf_ps is not None and ocf_ps != 0:
                price = pe * eps
                new_data['市现率'] = round(price / ocf_ps, 2)

            # 5. 财务杠杆
            if roe is not None and roa is not None and roa != 0:
                new_data['财务杠杆'] = round(roe / roa, 2)

            # 6. 总资产周转率
            if roa is not None and net_margin is not None and net_margin != 0:
                new_data['总资产周转率'] = round(roa / net_margin, 2)

            # 7. 格雷厄姆数
            if eps is not None and bvps is not None:
                val = 22.5 * eps * bvps
                if val > 0:
                    new_data['格雷厄姆数'] = round(math.sqrt(val), 2)

            # 更新数据
            if row_date in history_map:
                history_map[row_date].update(new_data)
            else:
                history_map[row_date] = new_data
            
            latest_record = history_map[row_date]

        sorted_history = sorted(history_map.values(), key=lambda x: x["date"])

        doc = {
            "_id": code,
            "name": name,
            "updated_at": datetime.now(),
            "latest_data": latest_record,
            "history": sorted_history
        }

        stock_collection.replace_one({"_id": code}, doc, upsert=True)

    except Exception as e:
        print(f"⚠️ 处理 {code} 异常: {e}")

def run_crawler_task():
    print(f"[{datetime.now()}] 🚀 开始 MongoDB 采集任务...")
    
    code_map = get_hk_codes_from_sina()
    if not code_map: 
        status.finish()
        return

    # 全量抓取
    all_codes = list(code_map.items())
    
    total = len(all_codes)
    print(f"📊 本次任务将抓取 {total} 只股票...")
    
    status.start(total)

    for i, (code, name) in enumerate(all_codes):
        status.update(i + 1, message=f"正在处理: {name}")
        print(f"⏳ ({i+1}/{total}) 正在处理: {name}")
        fetch_and_save_single_stock(code, name)
        time.sleep(random.uniform(0.5, 1.5))
    
    status.finish()
    print(f"[{datetime.now()}] 🎉 采集完成！")

if __name__ == "__main__":
    run_crawler_task()