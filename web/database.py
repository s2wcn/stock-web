import os
from pymongo import MongoClient

# === 配置区域 ===
# 优先从环境变量获取，否则使用默认值 (方便本地开发)
MONGO_HOST = os.getenv("MONGO_HOST", "192.168.1.252")
MONGO_PORT = int(os.getenv("MONGO_PORT", 27017))
MONGO_USER = os.getenv("MONGO_USER", "")
MONGO_PASS = os.getenv("MONGO_PASS", "")
DB_NAME = os.getenv("MONGO_DB_NAME", "stock_system")

# 构建连接 URI
if MONGO_USER and MONGO_PASS:
    MONGO_URI = f"mongodb://{MONGO_USER}:{MONGO_PASS}@{MONGO_HOST}:{MONGO_PORT}/"
else:
    MONGO_URI = f"mongodb://{MONGO_HOST}:{MONGO_PORT}/"

# === 连接池 ===
try:
    # 设置超时时间，避免连接不上时一直卡住
    client = MongoClient(MONGO_URI, serverSelectionTimeoutMS=5000)
    # 简单的连接测试
    client.server_info()
    print(f"✅ 已连接 MongoDB: {MONGO_HOST}:{MONGO_PORT} / {DB_NAME}")
except Exception as e:
    print(f"❌ MongoDB 连接失败: {e}")
    # 在生产环境中，这里可能需要抛出异常终止启动
    # raise e

db = client[DB_NAME]
stock_collection = db["stocks"]
config_collection = db["system_config"] 
template_collection = db["filter_templates"]