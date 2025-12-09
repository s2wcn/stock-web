from datetime import datetime

class CrawlerStatus:
    def __init__(self):
        self.is_running = False
        self.current = 0
        self.total = 0
        self.message = "空闲"
        self.last_finished_time = None  # 新增：记录最后完成时间

    def start(self, total):
        self.is_running = True
        self.total = total
        self.current = 0
        self.message = "正在初始化..."

    def update(self, current, message=""):
        self.current = current
        if message:
            self.message = message

    def finish(self):
        self.is_running = False
        self.message = "任务完成"
        self.last_finished_time = datetime.now()  # 新增：任务结束时记录当前时间

# 初始化全局实例
status = CrawlerStatus()