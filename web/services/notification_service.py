# 文件路径: web/services/notification_service.py
import requests
import time
import hmac
import hashlib
import base64
import urllib.parse
from config import DingTalkConfig
from logger import sys_logger as logger

class DingTalkService:
    """钉钉群机器人通知服务"""
    
    @staticmethod
    def _generate_sign() -> tuple:
        """生成钉钉加签签名"""
        timestamp = str(round(time.time() * 1000))
        secret = DingTalkConfig.SECRET
        secret_enc = secret.encode('utf-8')
        string_to_sign = '{}\n{}'.format(timestamp, secret)
        string_to_sign_enc = string_to_sign.encode('utf-8')
        hmac_code = hmac.new(secret_enc, string_to_sign_enc, digestmod=hashlib.sha256).digest()
        sign = urllib.parse.quote_plus(base64.b64encode(hmac_code))
        return timestamp, sign

    @classmethod
    def send_markdown(cls, title: str, text: str):
        """
        发送 Markdown 格式消息
        
        Args:
            title: 消息标题 (首屏会透出)
            text: Markdown 内容
        """
        if not DingTalkConfig.ENABLED:
            logger.info("🔕 钉钉通知已禁用，跳过发送")
            return
            
        if "YOUR_ACCESS_TOKEN" in DingTalkConfig.WEBHOOK_URL:
            logger.warning("⚠️ 钉钉 Webhook 未配置，无法发送通知")
            return

        try:
            timestamp, sign = cls._generate_sign()
            # 拼接带签名的 URL
            url = f"{DingTalkConfig.WEBHOOK_URL}&timestamp={timestamp}&sign={sign}"
            
            payload = {
                "msgtype": "markdown",
                "markdown": {
                    "title": title,
                    "text": text
                }
            }
            
            resp = requests.post(url, json=payload, timeout=5)
            if resp.status_code == 200:
                res_data = resp.json()
                if res_data.get("errcode") == 0:
                    logger.info("✅ 钉钉通知发送成功")
                else:
                    logger.error(f"❌ 钉钉发送失败: {res_data}")
            else:
                logger.error(f"❌ 钉钉HTTP错误: {resp.status_code}")
                
        except Exception as e:
            logger.error(f"❌ 发送钉钉通知异常: {e}")