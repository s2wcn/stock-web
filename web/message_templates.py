# 文件路径: web/message_templates.py
from datetime import datetime
from typing import List, Tuple

class DingTalkTemplates:
    """
    钉钉消息内容生成器 (Template Engine)
    
    职责:
    负责将业务数据（如股票列表、错误信息）转换为格式化的 Markdown 文本。
    将“展示逻辑”与“业务逻辑”分离，便于统一管理文案风格。
    """

    @staticmethod
    def strategy_signal_report(
        buy_signals: List[str], 
        sell_signals: List[str], 
        approach_buy: List[str], 
        approach_sell: List[str]
    ) -> Tuple[str, str]:
        """
        生成【策略信号报告】的 Markdown 内容
        
        Args:
            buy_signals: 触发买入的股票列表 (Markdown 格式字符串)
            sell_signals: 触发卖出的股票列表
            approach_buy: 接近买点（观察区）的股票列表
            approach_sell: 接近卖点（观察区）的股票列表
            
        Returns:
            (title, markdown_text) 元组，用于直接传给 DingTalkService 发送
        """
        title = "📢 港股长牛策略信号"
        cur_time = datetime.now().strftime('%m-%d %H:%M')
        
        # === 组装 Markdown 内容 ===
        content = []
        
        # 1. 标题头
        content.append(f"## {title} ({cur_time})")
        content.append("---") # 分割线
        
        # 2. 🟢 强力买入区域
        if buy_signals:
            content.append("\n### 🟢 触发买入")
            # 使用引用样式 (>) 让列表更突出
            content.extend([f"> {s}" for s in buy_signals])

        # 3. 🔴 强力卖出区域
        if sell_signals:
            content.append("\n### 🔴 触发卖出")
            content.extend([f"> {s}" for s in sell_signals])
            
        # 4. 📉 接近买点 (观察区)
        if approach_buy:
            content.append("\n#### 📉 接近买点 (观察)")
            content.extend([f"- {s}" for s in approach_buy]) # 使用普通列表

        # 5. 📈 接近卖点 (观察区)
        if approach_sell:
            content.append("\n#### 📈 接近卖点 (观察)")
            content.extend([f"- {s}" for s in approach_sell])
            
        # 6. 底部签名
        content.append("\n---")
        # 提示: 这里的 IP 需要根据实际服务器地址修改，或者读取配置
        content.append(f"###### 🤖 自动生成于 {cur_time}")

        return title, "\n".join(content)

    @staticmethod
    def task_exception_report(error_msg: str) -> Tuple[str, str]:
        """
        生成【任务异常报警】的 Markdown 内容
        
        Args:
            error_msg: 捕获到的异常堆栈或错误信息字符串
            
        Returns:
            (title, markdown_text)
        """
        title = "🚨 任务执行异常"
        cur_time = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        
        text = (
            f"### ❌ {title}\n\n"
            f"**发生时间**: {cur_time}\n\n"
            f"**错误详情**:\n"
            f"> {error_msg}\n\n"
            f"---\n"
            f"⚠️ 请及时登录服务器检查 `logs/system.log` 以获取完整堆栈信息。"
        )
        return title, text