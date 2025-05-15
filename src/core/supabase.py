"""
Supabase 整合模塊

處理與 Supabase 相關的初始化和操作。
"""

import os
import logging
from supabase import create_client, Client
from .config import settings

# 設定日誌
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
)
logger = logging.getLogger("supabase")

# 創建 Supabase 客戶端
supabase: Client = create_client(
    settings.SUPABASE_URL,
    settings.SUPABASE_KEY
) 