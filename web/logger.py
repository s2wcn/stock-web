# 文件路径: web/logger.py
import logging
import os
from logging.handlers import RotatingFileHandler
from logging import Logger
from config import SystemConfig # 引入配置

# 确保日志目录存在
LOG_DIR = "logs"
if not os.path.exists(LOG_DIR):
    os.makedirs(LOG_DIR)

def get_logger(name: str, filename: str = "app.log") -> Logger:
    """
    配置并获取一个 Logger 实例。
    使用 SystemConfig 中的配置来决定日志轮转策略。
    """
    logger = logging.getLogger(name)
    logger.setLevel(logging.INFO)
    
    if not logger.handlers:
        # 1. 文件处理器 (按配置轮转)
        file_path = os.path.join(LOG_DIR, filename)
        file_handler = RotatingFileHandler(
            file_path, 
            maxBytes=SystemConfig.LOG_MAX_BYTES, # 使用配置
            backupCount=SystemConfig.LOG_BACKUP_COUNT, # 使用配置
            encoding='utf-8'
        )
        file_formatter = logging.Formatter(
            '%(asctime)s - [%(levelname)s] - %(name)s - %(message)s'
        )
        file_handler.setFormatter(file_formatter)
        logger.addHandler(file_handler)

        # 2. 控制台处理器
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