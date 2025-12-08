import akshare as ak
import pandas as pd
import time
import random
from datetime import datetime
from database import stock_collection

# 定义需要重点采集和清洗的数字型字段列表
NUMERIC_FIELDS = [
    "基本每股收益(元)", "每股净资产(元)", "法定股本(股)", "每手股", 
    "每股股息TTM(港元)", "派息比率(%)", "已发行股本(股)", "已发行股本-H股(股)", 
    "每股经营现金流(元)", "股息率TTM(%)", "总市值(港元)", "港股市值(港元)", 
    "营业总收入", "营业总收入滚动环比增长(%)", "销售净利率(%)", "净利润", 
    "净利润滚动环比增长(%)", "股东权益回报率(%)", "市盈率", "PEG", "市净率", 
    "总资产回报率(%)"
]

def get_hk_codes_from_sina():
    """获取所有港股代码"""
    print("📡 连接新浪接口获取全市场清单...")
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

        # 2. 动态寻找日期列
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

        # 统一转为字符串日期
        df[date_col] = pd.to_datetime(df[date_col]).dt.strftime("%Y-%m-%d")
        df = df.sort_values(by=date_col)

        # === 读取现有文档 ===
        existing_doc = stock_collection.find_one({"_id": code})
        if existing_doc:
            history_list = existing_doc.get("history", [])
            history_map = {item["date"]: item for item in history_list}
        else:
            history_map = {}

        latest_record = {}
        
        for _, row in df.iterrows():
            row_date = row[date_col]
            
            # 转字典
            raw_data = row.to_dict()
            new_data = {}
            
            # === 数据清洗核心逻辑 ===
            for k, v in raw_data.items():
                if pd.isna(v): continue
                
                # 如果字段在我们需要采集的数字列表中，尝试转换
                if k in NUMERIC_FIELDS:
                    try:
                        # 去掉逗号并转float
                        val_str = str(v).replace(',', '')
                        new_data[k] = float(val_str)
                    except:
                        # 转换失败则保留原值
                        new_data[k] = v
                else:
                    new_data[k] = v
            
            new_data["date"] = row_date

            # 补充计算逻辑：如果接口没返回 PEG，尝试手动计算
            # (AkShare 很多时候不直接返回 PEG，或者字段名不一致，这里保留兜底逻辑)
            if "PEG" not in new_data:
                try:
                    pe = new_data.get("市盈率", new_data.get("PE"))
                    growth = new_data.get("净利润滚动环比增长(%)") # 使用新采集的字段
                    
                    if pe and growth:
                        if growth != 0:
                            new_data['PEG'] = round(pe / growth, 4)
                except:
                    pass

            # 更新或新增
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
    print(f"[{datetime.now()}] 🚀 开始 MongoDB 采集任务 (测试模式: 前10个)...")
    code_map = get_hk_codes_from_sina()
    if not code_map: return

    # 注意：生产环境请去掉 [:10]
    all_codes = list(code_map.items())
    total = len(all_codes)
    print(f"📊 本次任务将抓取 {total} 只股票...")

    for i, (code, name) in enumerate(all_codes):
        print(f"⏳ ({i+1}/{total}) 正在处理: {name}")
        fetch_and_save_single_stock(code, name)
        time.sleep(random.uniform(10, 20))
    
    print(f"[{datetime.now()}] 🎉 采集完成！")

if __name__ == "__main__":
    run_crawler_task()