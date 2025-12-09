import akshare as ak
import pandas as pd
import time
import random
import math
from datetime import datetime
from database import stock_collection
# 引入状态管理
from crawler_state import status

# === 1. 定义需要清洗为数字的基础字段 ===
NUMERIC_FIELDS = [
    "基本每股收益(元)", "每股净资产(元)", "法定股本(股)", "每手股", 
    "每股股息TTM(港元)", "派息比率(%)", "已发行股本(股)", "已发行股本-H股(股)", 
    "每股经营现金流(元)", "股息率TTM(%)", "总市值(港元)", "港股市值(港元)", 
    "营业总收入", "营业总收入滚动环比增长(%)", "销售净利率(%)", "净利润", 
    "净利润滚动环比增长(%)", "股东权益回报率(%)", "市盈率", "PEG", "市净率", 
    "总资产回报率(%)",
    # --- 新增字段 ---
    "基本每股收益同比增长率", "营业收入同比增长率", "营业利润率同比增长率"
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
        # === 1. 主数据：财务指标 ===
        df = ak.stock_hk_financial_indicator_em(symbol=code)
        if df is None or df.empty: return

        # 标准化主数据的日期列
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
        df.rename(columns={date_col: 'date'}, inplace=True)

        # === 2. 新增：获取成长性数据 (Time-Series) ===
        try:
            df_growth = ak.stock_hk_growth_comparison_em(symbol=code)
            if df_growth is not None and not df_growth.empty:
                g_date_col = next((c for c in ['日期', 'date', 'Date', '年度'] if c in df_growth.columns), None)
                if g_date_col:
                    df_growth[g_date_col] = pd.to_datetime(df_growth[g_date_col]).dt.strftime("%Y-%m-%d")
                    df_growth.rename(columns={g_date_col: 'date'}, inplace=True)
                    
                    target_growth_cols = ["基本每股收益同比增长率", "营业收入同比增长率", "营业利润率同比增长率"]
                    existing_cols = [c for c in target_growth_cols if c in df_growth.columns]
                    
                    if existing_cols:
                        df = pd.merge(df, df_growth[['date'] + existing_cols], on='date', how='left', suffixes=('', '_dup'))
                        drop_cols = [c for c in df.columns if c.endswith('_dup')]
                        if drop_cols:
                            df.drop(columns=drop_cols, inplace=True)
        except Exception as e:
            pass

        # === 3. 新增：获取静态信息 (行业 & 简介) ===
        industry_val = ""
        intro_val = ""

        try:
            df_profile = ak.stock_hk_company_profile_em(symbol=code)
            if df_profile is not None and not df_profile.empty:
                if "所属行业" in df_profile.columns:
                    industry_val = str(df_profile["所属行业"].iloc[0])
        except Exception:
            pass

        try:
            df_info = ak.stock_individual_basic_info_hk_xq(symbol=code)
            if df_info is not None and not df_info.empty:
                if "comintr" in df_info.columns:
                    intro_val = str(df_info["comintr"].iloc[0])
        except Exception:
            pass

        # === 4. 数据处理与存储 ===
        df = df.sort_values(by='date')

        existing_doc = stock_collection.find_one({"_id": code})
        history_map = {item["date"]: item for item in existing_doc.get("history", [])} if existing_doc else {}

        latest_record = {}
        
        for _, row in df.iterrows():
            row_date = row['date']
            raw_data = row.to_dict()
            new_data = {}
            
            for k, v in raw_data.items():
                if pd.isna(v): continue
                if k in NUMERIC_FIELDS:
                    try:
                        # 核心逻辑：保持 AkShare 返回的原始数值
                        # 如果 AkShare 返回 15.5 (代表 15.5%)，这里存储为 15.5
                        # 这保证了后续 PEG 计算 (PE/Growth) 是 PE/15.5，符合通常的 PEG 定义
                        new_data[k] = float(str(v).replace(',', ''))
                    except:
                        new_data[k] = v
                else:
                    new_data[k] = v
            
            if industry_val: new_data['所属行业'] = industry_val
            if intro_val: new_data['企业简介'] = intro_val
            
            new_data["date"] = row_date

            # === 计算衍生指标 ===
            def get_v(keys):
                for k in keys:
                    if k in new_data and isinstance(new_data[k], (int, float)):
                        return new_data[k]
                return None

            pe = get_v(['市盈率', 'PE'])
            eps = get_v(['基本每股收益(元)', '基本每股收益'])
            bvps = get_v(['每股净资产(元)', '每股净资产'])
            growth = get_v(['净利润滚动环比增长(%)', '净利润环比增长'])
            dividend_yield = get_v(['股息率TTM(%)', '股息率'])
            ocf_ps = get_v(['每股经营现金流(元)', '每股经营现金流'])
            roe = get_v(['股东权益回报率(%)', 'ROE'])
            roa = get_v(['总资产回报率(%)', 'ROA'])
            net_margin = get_v(['销售净利率(%)', '销售净利率'])

            # PEG: PE / Growth
            # 假设 PE=20, Growth=10 (即 10%) -> 20/10 = 2.0
            if "PEG" not in new_data and pe is not None and growth is not None:
                if growth != 0:
                    new_data['PEG'] = round(pe / growth, 4)

            # PEGY: PE / (Growth + Yield)
            # 假设 Yield=5 (即 5%) -> 20 / (10 + 5) = 1.33
            if pe is not None and growth is not None and dividend_yield is not None:
                total_return = growth + dividend_yield
                if total_return > 0:
                    new_data['PEGY'] = round(pe / total_return, 4)

            # 彼得林奇估值: Growth + Yield -> 15 (15%)
            if growth is not None and dividend_yield is not None:
                new_data['彼得林奇估值'] = round(growth + dividend_yield, 2)

            if ocf_ps is not None and eps is not None and eps != 0:
                new_data['净现比'] = round(ocf_ps / eps, 2)

            if pe is not None and eps is not None and ocf_ps is not None and ocf_ps != 0:
                price = pe * eps
                new_data['市现率'] = round(price / ocf_ps, 2)

            if roe is not None and roa is not None and roa != 0:
                new_data['财务杠杆'] = round(roe / roa, 2)

            if roa is not None and net_margin is not None and net_margin != 0:
                new_data['总资产周转率'] = round(roa / net_margin, 2)

            if eps is not None and bvps is not None:
                val = 22.5 * eps * bvps
                if val > 0:
                    new_data['格雷厄姆数'] = round(math.sqrt(val), 2)

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
            "history": sorted_history,
            "industry": industry_val,
            "intro": intro_val
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

    all_codes = list(code_map.items())
    total = len(all_codes)
    print(f"📊 本次任务将抓取 {total} 只股票...")
    
    status.start(total)

    for i, (code, name) in enumerate(all_codes):
        status.update(i + 1, message=f"正在处理: {name}")
        fetch_and_save_single_stock(code, name)
        time.sleep(random.uniform(1.0, 2.0))
    
    status.finish()
    print(f"[{datetime.now()}] 🎉 采集完成！")

if __name__ == "__main__":
    run_crawler_task()