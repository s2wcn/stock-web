from pymongo import MongoClient

# === 配置区域 ===
# 默认本地连接，如果你有密码请改为: mongodb://user:pass@localhost:27017/
MONGO_URI = "mongodb://192.168.1.252:27017/"
DB_NAME = "stock_system"

# === 连接池 ===
client = MongoClient(MONGO_URI)
db = client[DB_NAME]
stock_collection = db["stocks"]
config_collection = db["system_config"] 
template_collection = db["filter_templates"] # [新增] 筛选模版集合

print(f"✅ 已连接 MongoDB: {DB_NAME}")