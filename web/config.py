# 文件路径: web/config.py
from typing import List, Dict, Any, Tuple

# === 1. 系统与爬虫配置 (System Config) ===
class SystemConfig:
    # 爬虫并发线程数 (建议: CPU核数 * 2 ~ 5)
    CRAWLER_MAX_WORKERS: int = 5
    
    # 爬虫请求间隔 (秒)，防止请求太快被封IP
    CRAWLER_REQUEST_DELAY: float = 0.3

    # === [新增] API 请求配置 (修复报错的关键) ===
    API_MAX_RETRIES: int = 5       # 接口最大重试次数
    API_TIMEOUT: int = 60          # 单次请求超时时间(秒)
    
    # [修改] 历史数据获取范围
    # 解释: 为了确保 2018 年开始的数据能算出 MA250 (年线)，
    # 我们需要多抓取一年的数据作为“计算缓冲期 (Warm-up Period)”。
    HISTORY_START_DATE: str = "20170101"
    HISTORY_END_DATE: str = "22220101"
    
    # 日志文件配置
    LOG_MAX_BYTES: int = 10 * 1024 * 1024  # 单个日志最大 10MB
    LOG_BACKUP_COUNT: int = 5              # 保留最近 5 个日志文件

# === 2. 策略与分析配置 (Strategy Config) ===
class StrategyConfig:
    """量化策略核心参数，决定长牛判定和买卖信号"""
    
    # --- 长牛判定标准 ---
    MIN_R_SQUARED: float = 0.80        # 最小拟合度 (R²)，值越接近1表示股价走势越平稳向上
    MIN_ANNUAL_RETURN: float = 10.0    # 最小年化收益率 (%)
    MAX_ANNUAL_RETURN: float = 150.0   # 最大年化收益率 (%)
    MIN_TURNOVER: float = 50_000_000   # 最小日均成交额 (港币)
    MIN_MARKET_CAP: float = 10_000_000_000 # 最小总市值 (100亿港币)
    
    # --- 趋势完整性检查 ---
    TREND_MA_SHORT: int = 50           # 短期趋势均线 (用于熔断检查)
    TREND_MA_LONG: int = 250           # 长期趋势年线
    TREND_BREAK_CHECK_DAYS: int = 270  # 如果上市超过多少天，才开启年线熔断检查
    MIN_REGRESSION_SAMPLES: int = 20   # 线性回归最少需要的样本数 (天)

    # --- 网格交易策略回测 ---
    STRAT_COMMISSION: float = 0.002    # 交易佣金费率 (千分之二)
    # [修改] 建议改为 100,000 (十万) 以符合港股一手交易门槛
    STRAT_INITIAL_CAPITAL: float = 100000.0 
    
    MA_SHORT_WINDOW: int = 5           # 卖出信号参考均线 (MA5)
    MA_LONG_WINDOW: int = 60           # 买入信号参考均线 (MA60)
    MIN_STRAT_TRADES: int = 3          # 策略有效最少交易次数 (少于3次不算数)

    # 策略优化时的网格搜索范围 (起始, 结束, 步长)
    STRAT_BUY_RANGE: Tuple[float, float, float] = (-0.1, 0.101, 0.002) # 买入偏离度
    STRAT_SELL_RANGE: Tuple[float, float, float] = (0.00, 0.151, 0.002) # 卖出偏离度

# === 3. 价值估值配置 (Valuation Config) ===
class ValuationConfig:
    """格雷厄姆估值模型系数"""
    GRAHAM_CONST: float = 22.5 
    FAIR_PRICE_BASE: float = 8.5
    FAIR_PRICE_GROWTH_MULTIPLIER: float = 2.0

# === [新增] 钉钉通知配置 (DingTalk Config) ===
class DingTalkConfig:
    """钉钉群机器人配置"""
    # 是否启用通知
    ENABLED: bool = True
    
    # 钉钉机器人的 Webhook URL (请在钉钉群设置->智能群助手->添加机器人获取)
    WEBHOOK_URL: str = "https://oapi.dingtalk.com/robot/send?access_token=7ae5c654a13a267376fe807aff67b5442ef8eecc3574109b3614c6202a3e6201"
    
    # 安全设置中的加签密钥 (Secret)
    SECRET: str = "SECa45d3f8b763804d9a3d5c1e3775b0ca0f57b99eb168fe28394784511f84a20c1"
    
    # “接近”阈值的判定缓冲比例 (0.1 表示 10%)
    # 如果买入阈值是 -5%，当偏离度达到 -4.5% 时就会提示“接近买点”
    APPROACH_BUFFER: float = 0.2 

# === 4. 字段清洗配置 (Field Config) ===
NUMERIC_FIELDS: List[str] = [
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

# === 5. 前端表格列配置 (UI Config) ===
COLUMN_CONFIG: List[Dict[str, Any]] = [
    {"key": "所属行业", "label": "行业", "desc": "公司所属行业板块", "tip": "按东财/GICS分类标准划分", "no_sort": True, "no_chart": True},
    {"key": "bull_label", "label": "长牛评级", "desc": "长牛分级筛选", "tip": "基于5年走势算法筛选。<br>需满足：<br>1. R²>0.8<br>2. 年化10%-60%<br>3. <b>日均成交 > 500万</b><br>4. <b>ROE > 0</b>", "no_chart": True},
    {"key": "trend_analysis.r_squared", "label": "趋势R²", "desc": "对应周期的拟合度", "tip": "股价走势越接近直线，该值越接近1。<br><b>>0.8</b> 表示极度平稳。", "no_chart": True},
    
    {
        "key": "ma_strategy.benchmark_return", 
        "label": "基准回报%", 
        "desc": "同期持股不动回报", 
        "tip": "在策略回测的相同周期内（如5年），<b>买入并持有</b>不动的累计总收益率。<br>用于对比策略是否跑赢了股价本身。", 
        "suffix": "%", 
        "no_chart": True
    },
    {
        "key": "ma_strategy.total_return", 
        "label": "策略回报%", 
        "desc": "MA乖离率策略回测总回报", 
        "tip": "基于长牛周期进行的网格交易策略回测结果。", 
        "suffix": "%", 
        "no_chart": True 
    },
    {
        "key": "ma_strategy.win_rate", 
        "label": "胜率%", 
        "desc": "策略交易胜率", 
        "tip": "盈利交易次数 / 总交易次数", 
        "suffix": "%",
        "no_chart": True
    },
    {
        "key": "ma_strategy.buy_bias", 
        "label": "买入阈值", 
        "desc": "最佳买入偏离度", 
        "tip": "当(现价-MA60)/MA60低于此值时买入", 
        "suffix": "%",
        "no_chart": True
    },
    {
        "key": "ma_strategy.sell_bias", 
        "label": "卖出阈值", 
        "desc": "最佳卖出偏离度", 
        "tip": "当(现价-MA5)/MA5高于此值时卖出", 
        "suffix": "%",
        "no_chart": True
    },

    # === [修改点] 保持之前的修改：将“昨收”改为“最新” ===
    {"key": "昨收", "label": "最新", "desc": "最新价格", "tip": "最近一个交易日（或今日）的最新收盘价", "no_chart": False},
    {"key": "昨涨跌幅", "label": "涨跌%", "desc": "日涨跌幅", "tip": "最新交易日的涨跌百分比", "suffix": "%"},
    {"key": "昨成交量", "label": "成交量", "desc": "日成交量(股)", "tip": "最新交易日的成交股数", },
    {"key": "昨换手率", "label": "换手%", "desc": "交易活跃度", "tip": "最新成交量 ÷ 流通股本", "suffix": "%"},
    
    {"key": "近一周涨跌幅", "label": "周涨跌%", "desc": "短期动量", "tip": "当前价格相比5个交易日前的涨跌幅", "suffix": "%"},
    {"key": "近一月涨跌幅", "label": "月涨跌%", "desc": "中期动量", "tip": "当前价格相比20个交易日前的涨跌幅", "suffix": "%"},
    {"key": "市盈率", "label": "市盈率(PE)", "desc": "回本年限", "tip": "股价 ÷ 每股收益"},
    {"key": "PEG", "label": "PEG", "desc": "成长估值比", "tip": "PE ÷ (净利增长率 × 100)"},
    {"key": "PEGY", "label": "PEGY", "desc": "股息修正PEG", "tip": "PE ÷ (净利增长率 + 股息率)"},
    {"key": "合理股价", "label": "合理股价", "desc": "格雷厄姆估值", "tip": "EPS × (8.5 + 2 × 盈利增长率)"},
    {"key": "格雷厄姆数", "label": "格雷厄姆数", "desc": "价值上限", "tip": "√(22.5 × EPS × 每股净资产)"},
    {"key": "净现比", "label": "净现比", "desc": "盈利含金量", "tip": "每股经营现金流 ÷ EPS"},
    {"key": "市现率", "label": "市现率", "desc": "现金流估值", "tip": "股价 ÷ 每股经营现金流"},
    {"key": "财务杠杆", "label": "财务杠杆", "desc": "权益乘数", "tip": "总资产 ÷ 股东权益"},
    {"key": "总资产周转率", "label": "周转率", "desc": "营运能力", "tip": "营业收入 ÷ 总资产"},
    {"key": "基本每股收益同比增长率", "label": "EPS同比%", "desc": "盈利增速", "tip": "衡量归属股东利润的增长速度", "suffix": "%"},
    {"key": "营业收入同比增长率", "label": "营收同比%", "desc": "规模增速", "tip": "衡量业务规模的扩张速度", "suffix": "%"},
    {"key": "营业利润率同比增长率", "label": "利润率同比%", "desc": "获利能力变动", "tip": "反映产品竞争力的变化趋势", "suffix": "%"},
    {"key": "基本每股收益(元)", "label": "EPS(元)", "desc": "每股所获利润", "tip": ""},
    {"key": "每股净资产(元)", "label": "BPS(元)", "desc": "每股归属权益", "tip": ""},
    {"key": "每股经营现金流(元)", "label": "每股现金流", "desc": "每股进账现金", "tip": ""},
    {"key": "市净率", "label": "市净率(PB)", "desc": "净资产溢价", "tip": "股价 ÷ 每股净资产"},
    {"key": "股息率TTM(%)", "label": "股息率%", "desc": "分红回报率", "tip": "过去12个月分红总额 ÷ 市值", "suffix": "%"},
    {"key": "每股股息TTM(港元)", "label": "每股股息", "desc": "每股分到的钱", "tip": ""},
    {"key": "派息比率(%)", "label": "派息比%", "desc": "分红慷慨度", "tip": "总分红 ÷ 总净利润", "suffix": "%"},
    {"key": "营业总收入", "label": "营收", "desc": "总生意额", "tip": ""},
    {"key": "营业总收入滚动环比增长(%)", "label": "营收环比%", "desc": "营收短期趋势", "tip": "", "suffix": "%"},
    {"key": "净利润", "label": "净利润", "desc": "最终落袋利润", "tip": ""},
    {"key": "净利润滚动环比增长(%)", "label": "净利环比%", "desc": "净利短期趋势", "tip": "", "suffix": "%"},
    {"key": "销售净利率(%)", "label": "净利率%", "desc": "产品暴利程度", "tip": "净利润 ÷ 营收", "suffix": "%"},
    {"key": "股东权益回报率(%)", "label": "ROE%", "desc": "净资产收益率", "tip": "衡量管理层用股东的钱生钱的能力", "suffix": "%"},
    {"key": "总资产回报率(%)", "label": "ROA%", "desc": "总资产收益率", "tip": "衡量所有资产(含负债)的综合利用效率", "suffix": "%"},
    {"key": "总市值(港元)", "label": "总市值", "desc": "", "tip": ""},
    {"key": "港股市值(港元)", "label": "港股市值", "desc": "", "tip": ""},
    {"key": "法定股本(股)", "label": "法定股本", "desc": "", "tip": "", "no_sort": True, "no_chart": True},
    {"key": "已发行股本(股)", "label": "发行股本", "desc": "", "tip": "", "no_sort": True, "no_chart": True},
    {"key": "已发行股本-H股(股)", "label": "H股股本", "desc": "", "tip": "", "no_sort": True, "no_chart": True},
    {"key": "每手股", "label": "每手股", "desc": "", "tip": "", "no_sort": True, "no_chart": True},
]