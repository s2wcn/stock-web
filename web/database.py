# 文件路径: web/database.py
import os
from pymongo import MongoClient, ASCENDING, DESCENDING

# === 配置区域 ===
# 优先从环境变量获取，否则使用默认值 (方便本地开发)
MONGO_HOST = os.getenv("MONGO_HOST", "192.168.1.252")
MONGO_PORT = int(os.getenv("MONGO_PORT", 27017))
MONGO_USER = os.getenv("MONGO_USER", "")
MONGO_PASS = os.getenv("MONGO_PASS", "")
DB_NAME = os.getenv("MONGO_DB_NAME", "stock_system")

# 构建连接 URI
# [修复] 只要有用户名，就应该尝试构建带认证的 URI，防止密码为空字符串时的逻辑错误
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
        # 设置超时时间，避免连接不上时一直卡住
        client = MongoClient(MONGO_URI, serverSelectionTimeoutMS=5000)
        # 简单的连接测试
        client.server_info()
        print(f"✅ 已连接 MongoDB: {MONGO_HOST}:{MONGO_PORT} / {DB_NAME}")
        
        db = client[DB_NAME]
        stock_collection = db["stocks"]
        config_collection = db["system_config"] 
        template_collection = db["filter_templates"]

        # === 索引优化 (新增) ===
        print("🛠️ 正在检查并创建数据库索引...")
        # 基础查询索引
        stock_collection.create_index([("name", ASCENDING)])
        stock_collection.create_index([("is_ggt", ASCENDING)])
        stock_collection.create_index([("bull_label", ASCENDING)])
        
        # 排序和筛选常用字段索引 (Latest Data)
        index_fields = [
            "latest_data.昨收", 
            "latest_data.市盈率", 
            "latest_data.PEG", 
            "latest_data.股息率TTM(%)",
            "latest_data.股东权益回报率(%)"
        ]
        for field in index_fields:
            stock_collection.create_index([(field, ASCENDING)])
            
        print("✅ 数据库索引维护完成")

    except Exception as e:
        print(f"❌ MongoDB 连接失败: {e}")
        # 在生产环境中，这里可能需要抛出异常终止启动
        # raise e

# 初始化
init_db()