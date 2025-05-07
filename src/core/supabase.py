"""
Supabase 整合模塊

處理與 Supabase 相關的初始化和操作，包括檢查遷移狀態。
"""

import os
import subprocess
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

def check_supabase_migrations():
    """
    檢查 Supabase 遷移狀態
    
    這個函數使用 Supabase CLI 檢查是否有未應用的遷移。
    如果 CLI 沒有安裝或配置不正確，則會返回警告信息。
    
    Returns:
        tuple: (成功與否, 信息)
    """
    try:
        # 檢查 Supabase CLI 是否已安裝
        result = subprocess.run(
            ["supabase", "--version"], 
            capture_output=True, 
            text=True
        )
        
        if result.returncode != 0:
            logger.warning("Supabase CLI 未安裝或不可用")
            return False, "Supabase CLI 未安裝或不可用"
        
        # 獲取遷移狀態
        result = subprocess.run(
            ["supabase", "migration", "list"], 
            capture_output=True, 
            text=True
        )
        
        if result.returncode != 0:
            logger.warning(f"無法獲取遷移狀態: {result.stderr}")
            return False, f"無法獲取遷移狀態: {result.stderr}"
        
        # 分析輸出以檢查是否有未應用的遷移
        output = result.stdout
        if "Pending migrations" in output:
            logger.warning("有未應用的遷移")
            return False, "有未應用的遷移。請運行 'supabase db push' 應用它們。"
        
        logger.info("所有遷移已應用")
        return True, "所有遷移已應用"
    
    except Exception as e:
        logger.error(f"檢查遷移時出錯: {e}")
        return False, f"檢查遷移時出錯: {e}"

def get_supabase_migrations():
    """
    從 storage.migrations 表中獲取已應用的遷移
    
    這個方法不依賴於 Supabase CLI，而是直接從資料庫查詢。
    
    Returns:
        list: 已應用遷移的列表
    """
    try:
        result = supabase.table("storage.migrations").select("*").execute()
        return result.data
    except Exception as e:
        logger.error(f"獲取遷移記錄失敗: {e}")
        return []
