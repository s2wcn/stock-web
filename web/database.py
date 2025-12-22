# 文件路径: web/database.py
import os
from pymongo import MongoClient, ASCENDING, DESCENDING

# === 配置区域 ===
MONGO_HOST = os.getenv("MONGO_HOST", "192.168.1.252")
MONGO_PORT = int(os.getenv("MONGO_PORT", 27017))
MONGO_USER = os.getenv("MONGO_USER", "")
MONGO_PASS = os.getenv("MONGO_PASS", "")
DB_NAME = os.getenv("MONGO_DB_NAME", "stock_system")

# 构建连接 URI
if MONGO_USER:
    MONGO_URI = f"mongodb://{MONGO_USER}:{MONGO_PASS}@{MONGO_HOST}:{MONGO_PORT}/"
else:
    MONGO_URI = f"mongodb://{MONGO_HOST}:{MONGO_PORT}/"

client = None
db = None
stock_collection = None
config_collection = None
template_collection = None

def init_db():
    global client, db, stock_collection, config_collection, template_collection
    try:
        # connect=False: 避免在 import 时立即连接，防止多进程 fork 时死锁
        client = MongoClient(MONGO_URI, serverSelectionTimeoutMS=5000, connect=False)
        
        db = client[DB_NAME]
        stock_collection = db["stocks"]
        config_collection = db["system_config"] 
        template_collection = db["filter_templates"]

        print(f"✅ MongoDB 配置就绪: {MONGO_HOST}:{MONGO_PORT} / {DB_NAME}")

        # === 索引优化 ===
        # 使用 background=True 在后台创建索引，避免阻塞服务启动
        print("🛠️ 正在后台检查索引...")
        
        stock_collection.create_index([("name", ASCENDING)], background=True)
        stock_collection.create_index([("is_ggt", ASCENDING)], background=True)
        stock_collection.create_index([("bull_label", ASCENDING)], background=True)
        
        # 针对筛选和排序的高频字段
        index_fields = [
            "latest_data.昨收", 
            "latest_data.市盈率", 
            "latest_data.PEG", 
            "latest_data.股息率TTM(%)",
            "latest_data.股东权益回报率(%)",
            "latest_data.所属行业", # 新增
            "trend_analysis.r_squared" # 新增
        ]
        for field in index_fields:
            stock_collection.create_index([(field, ASCENDING)], background=True)
            
    except Exception as e:
        print(f"❌ MongoDB 初始化配置失败: {e}")

# 初始化
init_db()