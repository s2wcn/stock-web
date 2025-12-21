import akshare as ak
import pandas as pd
import numpy as np
from scipy import stats
from datetime import datetime, timedelta
import warnings

# 忽略 pandas 的一些警告
warnings.filterwarnings('ignore')

# === 配置参数 ===
MIN_R_SQUARED = 0.80       
MIN_ANNUAL_RETURN = 10.0   
MAX_ANNUAL_RETURN = 60.0   
MIN_TURNOVER = 5_000_000   

def check_ma250_interruption(df_subset):
    """
    检查是否存在连续 5 个交易日低于 MA250 (年线) 的情况。
    并定位最长一次破位的起始日期。
    """
    valid_ma = df_subset.dropna(subset=['ma250'])
    if valid_ma.empty:
        return True, "区间内无有效的 MA250 数据 (上市时间太短)"

    is_below = valid_ma['close'] < valid_ma['ma250']
    
    # 1. 对连续区域进行分组
    groups = is_below.ne(is_below.shift()).cumsum()
    
    # 2. 计算每组的长度
    consecutive_counts = is_below.groupby(groups).sum()
    max_consecutive = consecutive_counts.max()
    
    if max_consecutive >= 5:
        # 3. 找到那个最大的组的 ID
        worst_group_id = consecutive_counts.idxmax()
        
        # 4. 根据 ID 反查原始数据的日期
        worst_period_rows = valid_ma[groups == worst_group_id]
        
        if not worst_period_rows.empty:
            start_date = worst_period_rows['date'].iloc[0].strftime('%Y-%m-%d')
            return True, f"股价从 {start_date} 开始曾连续 {max_consecutive} 个交易日低于 MA250 (趋势中断)"
        
        return True, f"股价曾连续 {max_consecutive} 个交易日低于 MA250 (趋势中断)"
        
    return False, "趋势保持良好 (在年线之上运行)"

def analyze_stock_levels(code, check_date_str):
    print(f"\n{'='*70}")
    print(f"🕵️‍♂️ 长牛评级逐级诊断 (MA250 年线版): {code} @ {check_date_str}")
    print(f"{'='*70}")

    # === 1. 获取数据 ===
    print("📡 拉取 QFQ 历史数据...")
    try:
        df = ak.stock_hk_daily(symbol=code, adjust="qfq")
    except Exception as e:
        print(f"❌ 获取数据失败: {e}")
        return

    if df is None or df.empty:
        print("❌ 数据为空")
        return

    if 'date' in df.columns:
        df['date'] = pd.to_datetime(df['date'])
    else:
        print("❌ 数据缺少 date 列")
        return

    # === 2. 截取数据 ===
    check_date = pd.to_datetime(check_date_str)
    df = df[df['date'] <= check_date].copy()
    
    if df.empty:
        print(f"❌ 在 {check_date_str} 之前没有数据")
        return

    latest_record = df.iloc[-1]
    print(f"📅 分析基准日: {latest_record['date'].strftime('%Y-%m-%d')} | 收盘价: {latest_record['close']}")

    # === 3. 预计算全局指标 (MA50, MA250) ===
    df['ma50'] = df['close'].rolling(window=50).mean()
    # [修改] 使用 250 日均线
    df['ma250'] = df['close'].rolling(window=250).mean()
    
    if 'close' in df.columns and 'volume' in df.columns:
        df['amount_est'] = df['close'].astype(float) * df['volume'].astype(float)
    else:
        df['amount_est'] = 0

    # === 4. 第一关: 趋势熔断检查 (一票否决) ===
    print(f"\n{'='*20} 🛑 熔断检查 🛑 {'='*20}")
    # [修改] 需要至少 270 天 (250天MA + 20天比较)
    if len(df) > 270:
        curr = df.iloc[-1]
        prev_20 = df.iloc[-20]
        
        # [修改] 比较 MA250
        is_dead_cross = curr['ma50'] < curr['ma250']
        is_ma_falling = curr['ma250'] < prev_20['ma250'] 
        
        if is_dead_cross and is_ma_falling:
            print(f"❌ [熔断触发] 当前呈空头排列 (MA50 < MA250) 且年线趋势向下。")
            print(f"   MA50: {curr['ma50']:.3f}, MA250: {curr['ma250']:.3f}")
            print("🚫 结论: 趋势已坏，直接评定为【不符合】，终止脚本。")
            return
        else:
            print("✅ [熔断未触发] 均线形态尚可，继续分析...")
    else:
        print("⚠️ 数据不足 270 天，跳过熔断检查。")

    # === 5. 循环降级检查 (5年 -> 1年) ===
    print(f"\n{'='*20} 📉 开始逐级回测 📉 {'='*20}")
    
    for year in [5, 4, 3, 2, 1]:
        print(f"\n🔍 正在尝试匹配 [长牛 {year} 年] 标准...")
        
        target_start_date = latest_record['date'] - pd.DateOffset(years=year)
        
        mask = df['date'] >= target_start_date
        df_subset = df[mask].copy()
        
        fail_reason = None

        if df_subset.empty:
            fail_reason = "区间内无数据"
        else:
            actual_start_date = df_subset['date'].iloc[0]
            days_diff = (actual_start_date - target_start_date).days
            if days_diff > 30:
                fail_reason = f"数据缺失 (缺失开头 {days_diff} 天)"
            else:
                avg_turnover = df_subset['amount_est'].mean()
                if avg_turnover < MIN_TURNOVER:
                    fail_reason = f"流动性不足 (日均 {avg_turnover/10000:.1f}万 < {MIN_TURNOVER/10000:.0f}万)"
                else:
                    # [修改] 检查 MA250 连续破位
                    is_interrupted, msg = check_ma250_interruption(df_subset)
                    if is_interrupted:
                        fail_reason = msg
                    else:
                        # 线性回归 (日历年模式)
                        y_data = df_subset['close'].astype(float).values
                        
                        if len(y_data) < 20:
                            fail_reason = "有效交易日太少"
                        else:
                            start_ts = df_subset['date'].iloc[0]
                            x_data = (df_subset['date'] - start_ts).dt.days.values / 365.25
                            
                            log_y_data = np.log(y_data)
                            
                            slope, intercept, r_value, p_value, std_err = stats.linregress(x_data, log_y_data)
                            r_squared = r_value ** 2
                            annualized_return = (np.exp(slope) - 1) * 100
                            
                            print(f"   📊 数据: R²={r_squared:.4f} | 年化={annualized_return:.1f}% | 斜率={slope:.5f}")

                            if r_squared < MIN_R_SQUARED:
                                fail_reason = f"拟合度 R² 低于 0.8 ({r_squared:.4f})"
                            elif slope <= 0:
                                fail_reason = "趋势向下 (斜率为负)"
                            elif not (MIN_ANNUAL_RETURN <= annualized_return <= MAX_ANNUAL_RETURN):
                                if annualized_return < MIN_ANNUAL_RETURN:
                                    fail_reason = f"涨幅太慢 (年化 {annualized_return:.1f}% < {MIN_ANNUAL_RETURN}%)"
                                else:
                                    fail_reason = f"涨幅过快/妖股 (年化 {annualized_return:.1f}% > {MAX_ANNUAL_RETURN}%)"

        if fail_reason:
            print(f"   ❌ 失败: {fail_reason}")
            print(f"   👉 降级，继续尝试 [长牛 {year-1} 年]...")
            continue 
        else:
            print(f"\n🎉 匹配成功！")
            print(f"✅ 该股票在 {check_date_str} 符合 【长牛 {year} 年】 标准！")
            return

    print(f"\n🚫 遗憾！该股票在 {check_date_str} 连 [长牛1年] 都不符合。")

if __name__ == "__main__":
    try:
        input_code = input("请输入港股代码 (例如 00005): ").strip()
        input_date = input("请输入检测日期 (格式 YYYY-MM-DD): ").strip()
        
        if not input_date:
            input_date = datetime.now().strftime("%Y-%m-%d")
            
        analyze_stock_levels(input_code, input_date)
        
    except KeyboardInterrupt:
        print("\n已取消")