# 文件路径: web/services/maintenance_service.py
import math
from pymongo import UpdateOne
from database import stock_collection
from crawler_state import status
from config import NUMERIC_FIELDS
from logger import sys_logger as logger # [新增] 引入日志

class MaintenanceService:
    def __init__(self, collection, status_tracker):
        self.collection = collection
        self.status = status_tracker

    def run_recalculate_task(self):
        """
        执行离线补全指标与类型修复任务
        优化点:
        1. 使用 count_documents 获取总数，而非加载 list
        2. 使用 cursor 迭代器，避免一次性加载全库导致内存溢出
        3. 使用 bulk_write 批量提交修改，大幅降低数据库 IO 耗时
        """
        logger.info("🔄 Service: 开始执行离线补全指标与类型修复...")
        
        # 1. 获取总数用于进度条，但不加载具体数据
        total = self.collection.count_documents({})
        self.status.start(total)
        self.status.message = "正在扫描数据库..."

        # 2. 使用游标流式读取
        cursor = self.collection.find({})
        
        batch_ops = []
        BATCH_SIZE = 50  # 每 50 条提交一次数据库
        
        processed_count = 0

        for doc in cursor:
            if self.status.should_stop:
                self.status.finish("补全任务已终止")
                return

            code = doc["_id"]
            
            # 过滤 8XXXX (人民币柜台)
            if code.startswith("8"):
                self.collection.delete_one({"_id": code})
                continue

            name = doc.get("name", "Unknown")
            processed_count += 1
            
            # 仅更新进度文字，不频繁刷新整个状态以免阻塞
            if processed_count % 10 == 0:
                self.status.update(processed_count, message=f"正在计算: {name}")

            history = doc.get("history", [])
            if not history: 
                continue
            
            updated_history = []
            latest_record = {}
            has_changes = False # 标记是否真的需要更新，减少无效写入

            for item in history:
                # [修复] 强制类型转换
                for k, v in item.items():
                    if k in NUMERIC_FIELDS and isinstance(v, str):
                        try:
                            item[k] = float(v.replace(',', ''))
                            has_changes = True
                        except:
                            pass 

                # 辅助函数：安全获取浮点数
                def get_f(keys):
                    for k in keys:
                        val = item.get(k)
                        if val is not None:
                            try:
                                return float(str(val).replace(',', ''))
                            except:
                                pass
                    return None

                # 获取基础指标
                pe = get_f(['市盈率', 'PE'])
                eps = get_f(['基本每股收益(元)', '基本每股收益'])
                bvps = get_f(['每股净资产(元)', '每股净资产'])
                growth = get_f(['净利润滚动环比增长(%)', '净利润环比增长'])
                div_yield = get_f(['股息率TTM(%)', '股息率'])
                ocf_ps = get_f(['每股经营现金流(元)', '每股经营现金流'])
                roe = get_f(['股东权益回报率(%)', 'ROE'])
                roa = get_f(['总资产回报率(%)', 'ROA'])
                net_margin = get_f(['销售净利率(%)', '销售净利率'])

                # 清除旧的衍生指标以便重算
                derived_keys = [
                    'PEG', 'PEGY', '彼得林奇估值', '净现比', '市现率', 
                    '财务杠杆', '总资产周转率', '格雷厄姆数', '合理股价'
                ]
                for key in derived_keys:
                    if key in item:
                        item.pop(key, None)
                        has_changes = True

                # === 计算逻辑 (保持原样) ===
                if pe and pe > 0 and growth and growth != 0:
                    item['PEG'] = round(pe / growth, 4)

                if pe and pe > 0 and growth is not None and div_yield is not None:
                    total_return = growth + div_yield
                    if total_return > 0:
                        item['PEGY'] = round(pe / total_return, 4)
                
                if eps is not None and growth is not None:
                    fair_price = eps * (8.5 + 2 * growth)
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

                if eps is not None and bvps is not None:
                    val = 22.5 * eps * bvps
                    if val > 0:
                        item['格雷厄姆数'] = round(math.sqrt(val), 2)
                
                updated_history.append(item)
                latest_record = item

            # 构建批量更新操作
            # 只有当数据是新计算的，或者我们确认覆盖时才添加
            op = UpdateOne(
                {"_id": code},
                {"$set": {"history": updated_history, "latest_data": latest_record}}
            )
            batch_ops.append(op)

            # 达到 Batch Size 提交一次
            if len(batch_ops) >= BATCH_SIZE:
                try:
                    self.collection.bulk_write(batch_ops, ordered=False)
                except Exception as e:
                    logger.warning(f"⚠️ 批量写入部分失败: {e}")
                batch_ops = []

        # 提交剩余的
        if batch_ops:
            try:
                self.collection.bulk_write(batch_ops, ordered=False)
            except Exception as e:
                logger.error(f"⚠️ 最后批量写入失败: {e}")

        self.status.finish("全库清洗重算完成")