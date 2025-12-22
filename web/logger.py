# 文件路径: web/logger.py
import logging
import os
from logging.handlers import RotatingFileHandler

# 确保日志目录存在
LOG_DIR = "logs"
if not os.path.exists(LOG_DIR):
    os.makedirs(LOG_DIR)

def get_logger(name, filename="app.log"):
    logger = logging.getLogger(name)
    logger.setLevel(logging.INFO)
    
    # 避免重复添加 Handler
    if not logger.handlers:
        # 1. 文件处理器 (按大小轮转，最大 10MB，保留 5 个备份)
        file_handler = RotatingFileHandler(
            os.path.join(LOG_DIR, filename), 
            maxBytes=10*1024*1024, 
            backupCount=5, 
            encoding='utf-8'
        )
        file_formatter = logging.Formatter(
            '%(asctime)s - [%(levelname)s] - %(name)s - %(message)s'
        )
        file_handler.setFormatter(file_formatter)
        logger.addHandler(file_handler)

        # 2. 控制台处理器 (保留控制台输出，但有了格式)
        console_handler = logging.StreamHandler()
        console_formatter = logging.Formatter(
            '%(asctime)s - [%(levelname)s] - %(message)s'
        )
        console_handler.setFormatter(console_formatter)
        logger.addHandler(console_handler)
        
    return logger

# 预定义各个模块的 Logger
crawl_logger = get_logger("crawler", "crawler.log")
analysis_logger = get_logger("analysis", "analysis.log")
sys_logger = get_logger("system", "system.log")