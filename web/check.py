import os
from pymongo import MongoClient
import pprint  # 用于漂亮打印

# === 配置 (参考 database.py) ===
# 如果你的配置在环境变量中，请确保在这里设置或直接修改默认值
MONGO_HOST = os.getenv("MONGO_HOST", "192.168.1.252")
MONGO_PORT = int(os.getenv("MONGO_PORT", 27017))
DB_NAME = os.getenv("MONGO_DB_NAME", "stock_system")

# 连接数据库
client = MongoClient(host=MONGO_HOST, port=MONGO_PORT)
db = client[DB_NAME]
collection = db["stocks"]

def check_stock(code):
    # 查询 ID 为 code 的文档，只返回 name 和 latest_data
    doc = collection.find_one(
        {"_id": code},
        {"latest_data": 1, "name": 1, "bull_label": 1} # 也可以加上 'bull_label' 查看评级
    )
    
    if doc:
        print(f"=== {code} {doc.get('name')} 最新数据 ===")
        pprint.pprint(doc.get("latest_data"))
        if "bull_label" in doc:
            print(f"\n长牛评级: {doc['bull_label']}")
    else:
        print(f"未找到代码为 {code} 的数据。")

if __name__ == "__main__":
    check_stock("00005")