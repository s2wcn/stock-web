# 文件路径: web/bull_test.py
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
MAX_ANNUAL_RETURN = 200.0   
MIN_TURNOVER = 5_000_000   

# === 工具函数：计算 KAMA ===
def calculate_kama(series, period=10, fast_end=2, slow_end=30):
    change = series.diff(period).abs()
    volatility = series.diff().abs().rolling(window=period).sum()
    er = change / volatility.replace(0, 0.0000001)
    fast_sc = 2 / (fast_end + 1)
    slow_sc = 2 / (slow_end + 1)
    sc = (er * (fast_sc - slow_sc) + slow_sc) ** 2
    
    kama_values = np.zeros_like(series.values)
    kama_values[:] = np.nan
    
    if len(series) > period:
        kama_values[period-1] = series.iloc[period-1]
        values = series.values
        sc_values = sc.values
        current_kama = kama_values[period-1]
        for i in range(period, len(series)):
            if np.isnan(sc_values[i]):
                current_kama = values[i] 
            else:
                current_kama = current_kama + sc_values[i] * (values[i] - current_kama)
            kama_values[i] = current_kama
    return pd.Series(kama_values, index=series.index)

def check_ma250_interruption(df_subset):
    """
    检查是否存在连续 5 个交易日低于 MA250 (年线) 的情况。
    返回: (是否破位, 描述文本)
    """
    valid_ma = df_subset.dropna(subset=['ma250'])
    if valid_ma.empty:
        return True, "无有效年线数据"

    is_below = valid_ma['close'] < valid_ma['ma250']
    
    # 1. 识别连续区间
    groups = is_below.ne(is_below.shift()).cumsum()
    consecutive_counts = is_below.groupby(groups).sum()
    max_consecutive = consecutive_counts.max()
    
    if max_consecutive >= 5:
        # 2. 找到最长那一次破位的 Group ID
        worst_group_id = consecutive_counts.idxmax()
        # 3. 反查该组的数据，获取第一天
        worst_rows = valid_ma[groups == worst_group_id]
        start_date = worst_rows['date'].iloc[0].strftime("%Y-%m-%d")
        
        return True, f"从 {start_date} 开始，曾连续 {max_consecutive} 天低于年线"
        
    return False, "趋势完好 (始终在年线之上)"

def check_kama_status_in_period(df_subset):
    """
    全周期扫描 KAMA 状态 (含2天确认机制)
    返回: (是否通过, 简短状态, 详情日期, 累计破位天数)
    """
    if df_subset.empty: return False, "无数据", None, 0
    
    k_fast = df_subset['kama_fast']
    k_slow = df_subset['kama_slow']
    dates = df_subset['date']
    
    mask_valid = pd.notna(k_fast) & pd.notna(k_slow)
    if not mask_valid.any():
        return False, "KAMA 数据不足", None, 0
        
    kf_valid = k_fast[mask_valid]
    ks_valid = k_slow[mask_valid]
    dates_valid = dates[mask_valid]
    
    # 1. 原始死叉
    raw_dead_mask = kf_valid < ks_valid
    
    # 2. 确认死叉 (连续2天)
    prev_dead_mask = raw_dead_mask.shift(1).fillna(False)
    confirmed_dead_mask = raw_dead_mask & prev_dead_mask

    # === 统计累计破位天数 ===
    total_broken_days = confirmed_dead_mask.sum()

    # === 情况 A: 全程无确认死叉 ===
    if total_broken_days == 0:
        if raw_dead_mask.any():
            return True, "趋势良好 (仅有短暂假摔)", None, 0
        return True, "全程多头排列 (超稳)", None, 0

    # === 情况 B: 存在确认死叉 (检测失败) ===
    # 寻找最后一次破位结束或当前的日期，用于提示
    # 这里我们找“最近一次处于确认死叉”的日期
    last_idx = np.where(confirmed_dead_mask)[0][-1]
    last_date = dates_valid.iloc[last_idx].strftime("%Y-%m-%d")
    
    is_current_broken = confirmed_dead_mask.iloc[-1]
    
    if is_current_broken:
        return False, "当前处于死叉中", last_date, total_broken_days
    else:
        return False, "周期内曾发生破位", last_date, total_broken_days

def analyze_stock_levels(code, check_date_str):
    print(f"\n{'='*70}")
    print(f"🕵️‍♂️ 长牛评级深度体检 (详情增强版): {code} @ {check_date_str}")
    print(f"{'='*70}")

    # === 1. 获取数据 ===
    print("📡 拉取 QFQ 历史数据...")
    try:
        df = ak.stock_hk_daily(symbol=code, adjust="qfq")
    except Exception as e:
        print(f"❌ 获取数据失败: {e}")
        return

    if df is None or df.empty or 'date' not in df.columns:
        print("❌ 数据为空或格式错误")
        return
    
    df['date'] = pd.to_datetime(df['date'])
    check_date = pd.to_datetime(check_date_str)
    df = df[df['date'] <= check_date].copy()
    
    if df.empty:
        print(f"❌ 无历史数据")
        return

    latest_record = df.iloc[-1]
    
    # === 2. 预计算全局指标 ===
    df['ma50'] = df['close'].rolling(window=50).mean()
    df['ma250'] = df['close'].rolling(window=250).mean()
    df['kama_fast'] = calculate_kama(df['close'], 10, 2, 30)
    df['kama_slow'] = calculate_kama(df['close'], 30, 5, 50)
    
    if 'volume' in df.columns:
        df['amount_est'] = df['close'].astype(float) * df['volume'].astype(float)
    else:
        df['amount_est'] = 0

    # === 3. 逐级全指标遍历 ===
    print(f"\n{'='*20} 📉 开始长牛全指标扫描 📉 {'='*20}")
    
    for year in [5, 4, 3, 2, 1]:
        print(f"\n🔍 [检测长牛 {year} 年标准] ------------------------")
        
        # 3.1 数据切片
        try:
            target_start = latest_record['date'] - pd.DateOffset(years=year)
        except:
            target_start = latest_record['date'] - timedelta(days=365*year)
            
        mask = df['date'] >= target_start
        df_subset = df[mask].copy()

        # 核心逻辑变量
        this_year_passed = True
        
        # --- 检查 1: 数据完整性 ---
        if df_subset.empty:
            print(f"   ❌ 数据: 无数据")
            this_year_passed = False
            continue 
            
        days_diff = (df_subset['date'].iloc[0] - target_start).days
        if days_diff > 30:
            print(f"   ❌ 数据: 缺失开头 {days_diff} 天")
            this_year_passed = False
        else:
            print(f"   ✅ 数据: 完整度 OK")

        # --- 检查 2: 流动性 ---
        avg_turnover = df_subset['amount_est'].mean()
        turnover_ok = avg_turnover >= MIN_TURNOVER
        icon = "✅" if turnover_ok else "❌"
        print(f"   {icon} 流动性: 日均 {avg_turnover/10000:.1f}万 (阈值: {MIN_TURNOVER/10000:.0f}万)")
        if not turnover_ok: this_year_passed = False

        # --- 检查 3: 年线支撑 (MA250) ---
        is_broken, msg = check_ma250_interruption(df_subset)
        icon = "❌" if is_broken else "✅"
        print(f"   {icon} 年线支撑: {msg}")
        if is_broken: this_year_passed = False

        # --- 检查 4: KAMA 趋势完整性 ---
        kama_ok, kama_msg, date_info, broken_days = check_kama_status_in_period(df_subset)
        icon = "✅" if kama_ok else "❌"
        
        print(f"   {icon} KAMA趋势: {kama_msg}")
        if not kama_ok:
            print(f"      👉 累计确认破位: {broken_days} 个交易日")
            if date_info:
                print(f"      👉 最近/当前状态日期: {date_info}")
            this_year_passed = False

        # --- 检查 5: 回归分析 (R², 斜率, 年化) ---
        y_data = df_subset['close'].astype(float).values
        if len(y_data) < 20:
             print("   ❌ 统计: 有效交易日太少")
             this_year_passed = False
        else:
            start_ts = df_subset['date'].iloc[0]
            x_data = (df_subset['date'] - start_ts).dt.days.values / 365.25
            log_y_data = np.log(y_data)
            slope, intercept, r_value, _, _ = stats.linregress(x_data, log_y_data)
            
            r_squared = r_value ** 2
            annual_ret = (np.exp(slope) - 1) * 100
            bull_score = annual_ret * r_squared

            # R² 判定
            r2_ok = r_squared >= MIN_R_SQUARED
            icon = "✅" if r2_ok else "❌"
            print(f"   {icon} 拟合度(R²): {r_squared:.4f} (阈值: {MIN_R_SQUARED})")
            if not r2_ok: this_year_passed = False

            # 趋势方向判定
            trend_ok = slope > 0
            icon = "✅" if trend_ok else "❌"
            if not trend_ok:
                print(f"   {icon} 趋势方向: 向下 (斜率<0)")
                this_year_passed = False
            
            # 收益率判定
            ret_ok = MIN_ANNUAL_RETURN <= annual_ret <= MAX_ANNUAL_RETURN
            icon = "✅" if ret_ok else "❌"
            print(f"   {icon} 年化收益: {annual_ret:.1f}% (阈值: {MIN_ANNUAL_RETURN}-{MAX_ANNUAL_RETURN}%)")
            if not ret_ok: this_year_passed = False
            
            print(f"   📊 综合得分: {bull_score:.1f}")

        # === 最终判定 ===
        if this_year_passed:
            print(f"\n🎉 恭喜！匹配成功：【长牛 {year} 年】")
            return 
        else:
            print(f"   👉 结果: 不通过，降级继续...")

    print(f"\n🚫 遗憾！该股票在 {check_date_str} 不符合任何长牛标准。")

if __name__ == "__main__":
    try:
        input_code = input("请输入港股代码 (例如 00005): ").strip()
        input_date = input("请输入检测日期 (格式 YYYY-MM-DD，直接回车为今天): ").strip()
        if not input_date: input_date = datetime.now().strftime("%Y-%m-%d")
        analyze_stock_levels(input_code, input_date)
    except KeyboardInterrupt:
        print("\n已取消")