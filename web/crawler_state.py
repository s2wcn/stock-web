from datetime import datetime

class CrawlerStatus:
    def __init__(self):
        self.is_running = False
        self.should_stop = False  # [新增] 停止信号标志位
        self.current = 0
        self.total = 0
        self.message = "空闲"
        self.last_finished_time = None

    def start(self, total):
        self.is_running = True
        self.should_stop = False  # [新增] 每次开始前重置信号
        self.total = total
        self.current = 0
        self.message = "正在初始化..."

    def request_stop(self):
        """ [新增] 发出停止请求 """
        self.should_stop = True
        self.message = "正在停止..."

    def update(self, current, message=""):
        self.current = current
        if message:
            self.message = message

    def finish(self, end_msg="任务完成"):
        """ [修改] 支持自定义结束语 """
        self.is_running = False
        self.should_stop = False
        self.message = end_msg
        self.last_finished_time = datetime.now()

# 初始化全局实例
status = CrawlerStatus()