import akshare as ak
import pandas as pd
import time
import random
import math
from datetime import datetime
from database import stock_collection
from crawler_state import status

# === 1. 定义需要清洗为数字的基础字段 ===
NUMERIC_FIELDS = [
    "基本每股收益(元)", "每股净资产(元)", "法定股本(股)", "每手股", 
    "每股股息TTM(港元)", "派息比率(%)", "已发行股本(股)", "已发行股本-H股(股)", 
    "每股经营现金流(元)", "股息率TTM(%)", "总市值(港元)", "港股市值(港元)", 
    "营业总收入", "营业总收入滚动环比增长(%)", "销售净利率(%)", "净利润", 
    "净利润滚动环比增长(%)", "股东权益回报率(%)", "市盈率", "PEG", "市净率", 
    "总资产回报率(%)",
    "基本每股收益同比增长率", "营业收入同比增长率", "营业利润率同比增长率"
]

def get_ggt_codes():
    print("📡 正在获取港股通成分股名单...")
    try:
        df = ak.stock_hk_ggt_components_em()
        if df is not None and not df.empty:
            codes = df['代码'].astype(str).tolist()
            print(f"✅ 获取到 {len(codes)} 只港股通股票")
            return set(codes)
    except Exception as e:
        print(f"❌ 获取港股通名单失败: {e}")
    return set()

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

def fetch_and_save_single_stock(code, name, is_ggt=False):
    try:
        # === 1. 主数据：财务指标 ===
        df = ak.stock_hk_financial_indicator_em(symbol=code)
        if df is None or df.empty: return

        # 标准化日期列
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

        # === 2. 获取成长性数据 (快照) ===
        growth_data = {}
        try:
            df_growth = ak.stock_hk_growth_comparison_em(symbol=code)
            if df_growth is not None and not df_growth.empty:
                row_growth = df_growth.iloc[0]
                target_keys = ["基本每股收益同比增长率", "营业收入同比增长率", "营业利润率同比增长率"]
                for key in target_keys:
                    if key in df_growth.columns:
                        val = row_growth[key]
                        if pd.notna(val) and val != "":
                            try:
                                growth_data[key] = float(val)
                            except:
                                growth_data[key] = val
        except Exception:
            pass

        # === 3. 获取静态信息 ===
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
                if "item" in df_info.columns and "value" in df_info.columns:
                    mask = df_info['item'] == 'comintr'
                    if not mask.empty and mask.any():
                        intro_val = str(df_info.loc[mask, 'value'].iloc[0])
                elif "comintr" in df_info.columns:
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
                        new_data[k] = float(str(v).replace(',', ''))
                    except:
                        new_data[k] = v
                else:
                    new_data[k] = v
            
            if industry_val: new_data['所属行业'] = industry_val
            if intro_val: new_data['企业简介'] = intro_val
            
            new_data["date"] = row_date

            # === 计算衍生指标 (核心修复区域) ===
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

            # 1. PEG: 必须 PE > 0。亏损股不谈 PEG。
            if "PEG" not in new_data and pe is not None and pe > 0 and growth is not None:
                if growth != 0:
                    new_data['PEG'] = round(pe / growth, 4)

            # 2. PEGY: 必须 PE > 0。
            if pe is not None and pe > 0 and growth is not None and dividend_yield is not None:
                total_return = growth + dividend_yield
                if total_return > 0:
                    new_data['PEGY'] = round(pe / total_return, 4)

            # 3. 彼得林奇估值 (增长+股息)，不受 PE 正负影响，保留
            if growth is not None and dividend_yield is not None:
                new_data['彼得林奇估值'] = round(growth + dividend_yield, 2)

            # 4. 净现比: 必须 EPS > 0。防止 EPS<0 且 OCF<0 导致结果为正的“双亏误导”。
            if ocf_ps is not None and eps is not None and eps > 0:
                new_data['净现比'] = round(ocf_ps / eps, 2)

            # 5. 市现率: 必须 PE > 0 且 EPS > 0。
            # 因为这里是用 PE*EPS 反推股价，如果双负，算出来的股价是正的，逻辑完全错误。
            if pe is not None and pe > 0 and eps is not None and eps > 0 and ocf_ps is not None and ocf_ps != 0:
                price = pe * eps
                new_data['市现率'] = round(price / ocf_ps, 2)

            # 6. 财务杠杆
            if roe is not None and roa is not None and roa != 0:
                new_data['财务杠杆'] = round(roe / roa, 2)

            # 7. 总资产周转率
            if roa is not None and net_margin is not None and net_margin != 0:
                new_data['总资产周转率'] = round(roa / net_margin, 2)

            # 8. 格雷厄姆数 (根号下必须为正，已隐含在val>0中)
            if eps is not None and bvps is not None:
                val = 22.5 * eps * bvps
                if val > 0:
                    new_data['格雷厄姆数'] = round(math.sqrt(val), 2)

            if row_date in history_map:
                history_map[row_date].update(new_data)
            else:
                history_map[row_date] = new_data
            
            latest_record = history_map[row_date]

        if growth_data and latest_record:
            latest_record.update(growth_data)
            if latest_record["date"] in history_map:
                history_map[latest_record["date"]].update(growth_data)

        sorted_history = sorted(history_map.values(), key=lambda x: x["date"])

        doc = {
            "_id": code,
            "name": name,
            "updated_at": datetime.now(),
            "latest_data": latest_record,
            "history": sorted_history,
            "industry": industry_val,
            "intro": intro_val,
            "is_ggt": is_ggt
        }

        stock_collection.replace_one({"_id": code}, doc, upsert=True)

    except Exception as e:
        print(f"⚠️ 处理 {code} 异常: {e}")

def run_crawler_task():
    print(f"[{datetime.now()}] 🚀 开始 MongoDB 采集任务...")
    
    code_map = get_hk_codes_from_sina()
    if not code_map: 
        status.finish("初始化失败")
        return

    ggt_codes = get_ggt_codes()

    all_codes = list(code_map.items())
    total = len(all_codes)
    print(f"📊 本次任务将抓取 {total} 只股票...")
    
    status.start(total)

    for i, (code, name) in enumerate(all_codes):
        if status.should_stop:
            print("🛑 接到停止指令，爬虫任务已终止。")
            status.finish("任务已由用户终止")
            return

        status.update(i + 1, message=f"正在处理: {name}")
        
        is_ggt_stock = code in ggt_codes
        fetch_and_save_single_stock(code, name, is_ggt=is_ggt_stock)
        
        if status.should_stop: break
        
        time.sleep(random.uniform(1.0, 2.0))
    
    if status.should_stop:
        status.finish("任务已由用户终止")
    else:
        status.finish("采集完成")
    
    print(f"[{datetime.now()}] 🎉 采集任务结束")

if __name__ == "__main__":
    run_crawler_task()