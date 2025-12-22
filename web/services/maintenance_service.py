# 文件路径: web/services/maintenance_service.py
import math
from typing import List, Dict, Any, Optional
from pymongo import UpdateOne
from pymongo.collection import Collection
from config import NUMERIC_FIELDS, ValuationConfig # 引入配置
from logger import sys_logger as logger

class MaintenanceService:
    def __init__(self, collection: Collection, status_tracker: Any):
        self.collection = collection
        self.status = status_tracker

    def run_recalculate_task(self):
        logger.info("🔄 Service: 开始执行离线补全指标与类型修复...")
        
        total = self.collection.count_documents({})
        self.status.start(total)
        self.status.message = "正在扫描数据库..."

        cursor = self.collection.find({})
        batch_ops: List[UpdateOne] = []
        BATCH_SIZE = 50 
        processed_count = 0

        for doc in cursor:
            if self.status.should_stop:
                self.status.finish("补全任务已终止")
                return

            code = doc["_id"]
            if str(code).startswith("8"): 
                self.collection.delete_one({"_id": code})
                continue

            processed_count += 1
            if processed_count % 10 == 0:
                self.status.update(processed_count, message=f"正在计算: {doc.get('name')}")

            history = doc.get("history", [])
            if not history: continue
            
            updated_history = []
            latest_record = {}

            for item in history:
                # 修复数据类型
                for k, v in item.items():
                    if k in NUMERIC_FIELDS and isinstance(v, str):
                        try: item[k] = float(v.replace(',', ''))
                        except: pass 

                def get_f(keys):
                    for k in keys:
                        val = item.get(k)
                        if val is not None:
                            try: return float(str(val).replace(',', ''))
                            except: pass
                    return None

                # 获取基础数据
                pe = get_f(['市盈率', 'PE'])
                eps = get_f(['基本每股收益(元)', '基本每股收益'])
                bvps = get_f(['每股净资产(元)', '每股净资产'])
                growth = get_f(['净利润滚动环比增长(%)', '净利润环比增长'])
                div_yield = get_f(['股息率TTM(%)', '股息率'])
                ocf_ps = get_f(['每股经营现金流(元)', '每股经营现金流'])
                roe = get_f(['股东权益回报率(%)', 'ROE'])
                roa = get_f(['总资产回报率(%)', 'ROA'])
                net_margin = get_f(['销售净利率(%)', '销售净利率'])

                # 重新计算
                if pe and pe > 0 and growth and growth != 0:
                    item['PEG'] = round(pe / growth, 4)

                if pe and pe > 0 and growth is not None and div_yield is not None:
                    tr = growth + div_yield
                    if tr > 0: item['PEGY'] = round(pe / tr, 4)
                
                # 使用 ValuationConfig 计算合理股价
                if eps is not None and growth is not None:
                    # 公式: EPS * (8.5 + 2 * g)
                    multiplier = ValuationConfig.FAIR_PRICE_BASE + ValuationConfig.FAIR_PRICE_GROWTH_MULTIPLIER * growth
                    fair_price = eps * multiplier
                    if fair_price > 0:
                        item['合理股价'] = round(fair_price, 2)
                
                if ocf_ps is not None and eps and eps > 0:
                    item['净现比'] = round(ocf_ps / eps, 2)
                
                if pe and pe > 0 and eps and eps > 0 and ocf_ps and ocf_ps != 0:
                    price = pe * eps
                    item['市现率'] = round(price / ocf_ps, 2)

                if roe is not None and roa and roa != 0:
                    item['财务杠杆'] = round(roe / roa, 2)

                if roa is not None and net_margin and net_margin != 0:
                    item['总资产周转率'] = round(roa / net_margin, 2)

                # 使用 ValuationConfig 计算格雷厄姆数
                if eps is not None and bvps is not None:
                    # 公式: Sqrt(22.5 * EPS * BVPS)
                    val = ValuationConfig.GRAHAM_CONST * eps * bvps
                    if val > 0:
                        item['格雷厄姆数'] = round(math.sqrt(val), 2)
                
                updated_history.append(item)
                latest_record = item

            op = UpdateOne(
                {"_id": code},
                {"$set": {"history": updated_history, "latest_data": latest_record}}
            )
            batch_ops.append(op)

            if len(batch_ops) >= BATCH_SIZE:
                try: self.collection.bulk_write(batch_ops, ordered=False)
                except Exception: pass
                batch_ops = []

        if batch_ops:
            try: self.collection.bulk_write(batch_ops, ordered=False)
            except Exception: pass

        self.status.finish("全库清洗重算完成")