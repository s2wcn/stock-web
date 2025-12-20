import akshare as ak
import pandas as pd
import time
import random
import math
from datetime import datetime, timedelta
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
    "基本每股收益同比增长率", "营业收入同比增长率", "营业利润率同比增长率",
    # 行情字段
    "昨收", "昨涨跌幅", "昨成交量", "昨换手率", "近一周涨跌幅", "近一月涨跌幅"
]

def check_critical_error(e):
    """
    检查是否为严重连接错误（IP被封/连接中断）
    """
    err_str = str(e)
    if "Remote end closed connection" in err_str or "Connection aborted" in err_str or "RemoteDisconnected" in err_str:
        print(f"🛑 严重错误检测: {err_str}")
        status.message = "❌ 警告：IP可能被封或连接中断，任务强制终止！"
        status.should_stop = True 
        return True
    return False

def is_derivative(name):
    if not name: return False
    keywords = ['购', '沽', '牛', '熊', '界内']
    for kw in keywords:
        if kw in name:
            return True
    return False

def get_ggt_codes():
    print("📡 正在获取港股通成分股名单...")
    try:
        df = ak.stock_hk_ggt_components_em()
        if df is not None and not df.empty:
            codes = df['代码'].astype(str).tolist()
            print(f"✅ 获取到 {len(codes)} 只港股通股票")
            return set(codes)
    except Exception as e:
        print(f"⚠️ 接口获取港股通名单失败: {e} (已忽略错误，尝试加载历史数据...)")
    
    print("⚠️ 尝试从数据库加载【历史港股通数据】...")
    try:
        cursor = stock_collection.find({"is_ggt": True}, {"_id": 1})
        codes = [doc["_id"] for doc in cursor]
        if codes:
            print(f"✅ 成功加载 {len(codes)} 只历史港股通股票")
            return set(codes)
        else:
            print("⚠️ 数据库中无历史港股通记录")
    except Exception as db_e:
        print(f"❌ 读取数据库失败: {db_e}")

    return None 

def get_hk_codes_from_sina():
    print("📡 连接接口获取全市场清单...")
    try:
        df = ak.stock_hk_spot()
        if df is None or df.empty: return {}
        codes = df['代码'].astype(str).tolist()
        names = df['中文名称'].tolist()
        return dict(zip(codes, names))
    except Exception as e:
        check_critical_error(e)
        print(f"❌ 获取列表失败: {e}")
        return {}

def get_market_performance(code, h_share_capital=None):
    if status.should_stop: return {} 

    performance = {}
    try:
        time.sleep(random.uniform(0.5, 1.0))
        df = ak.stock_hk_daily(symbol=code, adjust="")
        
        if df is None or df.empty:
            return performance

        df = df.sort_values(by="date")
        if len(df) > 45:
            df = df.iloc[-45:]

        latest_row = df.iloc[-1]
        close_val = float(latest_row["close"])
        open_val = float(latest_row["open"])
        volume_val = float(latest_row["volume"])
        
        performance["昨收"] = close_val
        performance["昨成交量"] = volume_val
        
        turnover_rate = 0.0
        if h_share_capital and h_share_capital > 0:
            try:
                turnover_rate = (volume_val / h_share_capital) * 100
            except:
                turnover_rate = 0.0
        
        performance["昨换手率"] = round(turnover_rate, 2)

        if len(df) >= 2:
            prev_close = float(df.iloc[-2]["close"])
            if prev_close > 0:
                pct = (close_val - prev_close) / prev_close * 100
                performance["昨涨跌幅"] = round(pct, 2)
            else:
                performance["昨涨跌幅"] = 0.0
        else:
            if open_val > 0:
                pct = (close_val - open_val) / open_val * 100
                performance["昨涨跌幅"] = round(pct, 2)
            else:
                performance["昨涨跌幅"] = 0.0
        
        total_rows = len(df)
        if total_rows >= 6:
            prev_week_close = float(df.iloc[-6]["close"])
            if prev_week_close > 0:
                pct = (close_val - prev_week_close) / prev_week_close * 100
                performance["近一周涨跌幅"] = round(pct, 2)
        
        if total_rows >= 21:
            prev_month_close = float(df.iloc[-21]["close"])
            if prev_month_close > 0:
                pct = (close_val - prev_month_close) / prev_month_close * 100
                performance["近一月涨跌幅"] = round(pct, 2)
                
    except Exception as e:
        if check_critical_error(e):
            return {}
        pass
        
    return performance

def fetch_and_save_single_stock(code, name, is_ggt=None):
    if status.should_stop: return 
    if is_derivative(name): return

    try:
        # === 1. 主数据 ===
        try:
            time.sleep(random.uniform(0.5, 1.0))
            df = ak.stock_hk_financial_indicator_em(symbol=code)
        except Exception as e:
            if check_critical_error(e): return 
            print(f"⚠️ 获取财务数据失败 {code}: {e}")
            return

        if df is None or df.empty: return
        time.sleep(random.uniform(0.5, 1.0))

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
        df = df.sort_values(by='date')

        h_share_capital = 0.0
        try:
            if not df.empty:
                last_row = df.iloc[-1]
                if "已发行股本-H股(股)" in last_row:
                    val = last_row["已发行股本-H股(股)"]
                    if pd.notna(val):
                        h_share_capital = float(str(val).replace(',', ''))
        except:
            h_share_capital = 0.0

        # === 2. 获取成长性数据 ===
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
        except Exception as e:
            if check_critical_error(e): return
            pass
        
        time.sleep(random.uniform(0.5, 1.0))

        # === 3. 获取静态信息 ===
        industry_val = ""
        intro_val = ""

        try:
            df_profile = ak.stock_hk_company_profile_em(symbol=code)
            if df_profile is not None and not df_profile.empty:
                if "所属行业" in df_profile.columns:
                    industry_val = str(df_profile["所属行业"].iloc[0])
        except Exception as e:
            if check_critical_error(e): return
            pass
        
        time.sleep(random.uniform(0.5, 1.0))

        try:
            df_info = ak.stock_individual_basic_info_hk_xq(symbol=code)
            if df_info is not None and not df_info.empty:
                mask = df_info['item'] == 'comintr'
                if not mask.empty and mask.any():
                    intro_val = str(df_info.loc[mask, 'value'].iloc[0])
        except Exception as e:
            if check_critical_error(e): return
            pass

        time.sleep(random.uniform(0.5, 1.0))

        # === 4. 获取行情数据 (传入股本进行计算) ===
        market_data = get_market_performance(code, h_share_capital=h_share_capital)
        if status.should_stop: return 

        # === 5. 数据处理与存储 ===
        existing_doc = stock_collection.find_one({"_id": code})
        history_map = {item["date"]: item for item in existing_doc.get("history", [])} if existing_doc else {}

        final_is_ggt = False
        if is_ggt is not None:
            final_is_ggt = is_ggt
        elif existing_doc:
            final_is_ggt = existing_doc.get("is_ggt", False)

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

            if "PEG" not in new_data and pe is not None and pe > 0 and growth is not None:
                if growth != 0:
                    new_data['PEG'] = round(pe / growth, 4)

            if pe is not None and pe > 0 and growth is not None and dividend_yield is not None:
                total_return = growth + dividend_yield
                if total_return > 0:
                    new_data['PEGY'] = round(pe / total_return, 4)

            if growth is not None and eps is not None:
                fair_price = eps * (8.5 + 2 * growth)
                if fair_price > 0:
                    new_data['合理股价'] = round(fair_price, 2)

            if ocf_ps is not None and eps is not None and eps > 0:
                new_data['净现比'] = round(ocf_ps / eps, 2)

            if pe is not None and pe > 0 and eps is not None and eps > 0 and ocf_ps is not None and ocf_ps != 0:
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

        if latest_record:
            if growth_data:
                latest_record.update(growth_data)
            if market_data:
                latest_record.update(market_data)
            if latest_record["date"] in history_map:
                history_map[latest_record["date"]].update(latest_record)

        sorted_history = sorted(history_map.values(), key=lambda x: x["date"])

        doc = {
            "_id": code,
            "name": name,
            "updated_at": datetime.now(),
            "latest_data": latest_record,
            "history": sorted_history,
            "industry": industry_val,
            "intro": intro_val,
            "is_ggt": final_is_ggt
        }

        stock_collection.replace_one({"_id": code}, doc, upsert=True)

    except Exception as e:
        if check_critical_error(e): return
        print(f"⚠️ 处理 {code} 异常: {e}")

def run_crawler_task():
    print(f"[{datetime.now()}] 🚀 开始 MongoDB 采集任务 (HK)...")
    
    # [新增] 清理所有以 8 开头的股票 (人民币结算)
    print("🧹 正在清理 8XXXX (人民币柜台) 重复数据...")
    del_result = stock_collection.delete_many({"_id": {"$regex": "^8"}})
    print(f"✅ 已删除 {del_result.deleted_count} 条重复数据")

    code_map = get_hk_codes_from_sina()
    if status.should_stop: 
        status.finish(status.message)
        return
    if not code_map: 
        status.finish("初始化失败：无法获取股票清单")
        return

    ggt_codes = get_ggt_codes()
    if ggt_codes is not None:
        print(f"⚡️ 获取到最新名单，正在批量刷新全库港股通状态...")
        try:
            ggt_list = list(ggt_codes)
            stock_collection.update_many(
                {"_id": {"$in": ggt_list}}, 
                {"$set": {"is_ggt": True}}
            )
            stock_collection.update_many(
                {"_id": {"$nin": ggt_list}}, 
                {"$set": {"is_ggt": False}}
            )
            print("✅ 全库港股通状态刷新完毕")
        except Exception as e:
            print(f"❌ 批量刷新状态出错: {e}")

    # [修改] 过滤代码列表，排除 8 开头的股票
    all_codes = [
        (code, name) for code, name in code_map.items() 
        if not code.startswith("8")
    ]
    
    total = len(all_codes)
    print(f"📊 本次任务将抓取 {total} 只股票 (已过滤 8XXXX)...")
    
    status.start(total)

    for i, (code, name) in enumerate(all_codes):
        if status.should_stop:
            print("🛑 接到停止指令，爬虫任务终止。")
            status.finish(status.message if status.message.startswith("❌") else "任务已由用户终止")
            return

        status.update(i + 1, message=f"正在处理: {name}")
        
        if ggt_codes is None:
            is_ggt_stock = None
        else:
            is_ggt_stock = code in ggt_codes

        fetch_and_save_single_stock(code, name, is_ggt=is_ggt_stock)
        
        if status.should_stop: 
            break
        
        time.sleep(random.uniform(1.5, 2.5))
    
    if status.should_stop:
        final_msg = status.message if status.message.startswith("❌") else "任务已由用户终止"
        status.finish(final_msg)
    else:
        status.finish("采集完成")
    
    print(f"[{datetime.now()}] 🎉 采集任务结束")

if __name__ == "__main__":
    run_crawler_task()