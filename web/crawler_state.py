# 文件路径: web/crawler_state.py
from datetime import datetime
from typing import Optional

class CrawlerStatus:
    """
    用于跨模块共享爬虫任务的状态。
    相当于一个全局的公告栏，爬虫线程写状态，API 线程读状态。
    """
    def __init__(self):
        self.is_running: bool = False
        self.should_stop: bool = False  # 停止信号标志位
        self.current: int = 0
        self.total: int = 0
        self.message: str = "空闲"
        self.last_finished_time: Optional[datetime] = None

    def start(self, total: int):
        """重置状态，开始新任务"""
        self.is_running = True
        self.should_stop = False  # 每次开始前重置信号
        self.total = total
        self.current = 0
        self.message = "正在初始化..."

    def request_stop(self):
        """发出停止请求 (软中断)"""
        self.should_stop = True
        self.message = "正在停止..."

    def update(self, current: int, message: str = ""):
        """更新进度"""
        self.current = current
        if message:
            self.message = message

    def finish(self, end_msg: str = "任务完成"):
        """标记任务结束"""
        self.is_running = False
        self.should_stop = False
        self.message = end_msg
        self.last_finished_time = datetime.now()

# 初始化全局实例
status = CrawlerStatus()