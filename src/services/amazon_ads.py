import httpx
import json
import uuid
from datetime import datetime
from typing import Dict, Any, List, Optional, Tuple
import os
import logging

from ..core.config import settings
from ..core.security import encrypt_token, decrypt_token
from ..models.connections import AmazonAdsConnection
from supabase import create_client, Client

# 設定日誌
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# 嘗試獲取 Supabase 配置，支持不同的環境變量名稱
supabase_url = settings.SUPABASE_URL or os.getenv("NEXT_PUBLIC_SUPABASE_URL")
supabase_key = settings.SUPABASE_KEY or os.getenv("NEXT_PUBLIC_SUPABASE_ANON_KEY")

# 日誌輸出當前環境變量
logger.info(f"環境變量: SUPABASE_URL={supabase_url}, SUPABASE_KEY={'已設置' if supabase_key else '未設置'}")
logger.info(f"其他環境變量: AMAZON_ADS_CLIENT_ID={settings.AMAZON_ADS_CLIENT_ID}, FRONTEND_URL={settings.FRONTEND_URL}")

if not supabase_url or not supabase_key:
    logger.warning("警告: Supabase URL 或密鑰未設置。請檢查環境變量。")
    logger.warning(f"當前設置: URL={supabase_url}, KEY={'已設置' if supabase_key else '未設置'}")
    # 設置為空字符串，避免創建客戶端時出錯
    supabase_url = supabase_url or ""
    supabase_key = supabase_key or ""

# 創建 Supabase 客戶端
try:
    supabase: Client = create_client(supabase_url, supabase_key)
    logger.info("Supabase 客戶端創建成功")
except Exception as e:
    logger.error(f"創建 Supabase 客戶端失敗: {str(e)}")
    # 創建一個空的客戶端，避免代碼中的引用錯誤
    supabase = None

# 初始化 Supabase 表
def init_supabase_tables():
    """初始化必要的 Supabase 表"""
    if not supabase:
        logger.error("無法初始化 Supabase 表：客戶端不可用")
        return False
    
    try:
        # 檢查並創建 amazon_ads_states 表
        logger.info("正在檢查 amazon_ads_states 表...")
        # 僅供判斷表是否存在，不執行實際操作
        try:
            supabase.table('amazon_ads_states').select('id').limit(1).execute()
            logger.info("amazon_ads_states 表已存在")
        except Exception as e:
            logger.info(f"創建 amazon_ads_states 表...")
            # 在實際情況下，應該通過 Supabase 界面或遷移腳本創建表
            # 這裡僅作示例，實際上 SDK 不支持 CREATE TABLE 操作
            logger.warning("需要手動在 Supabase 界面創建 amazon_ads_states 表")
        
        # 檢查並創建 amazon_ads_connections 表
        logger.info("正在檢查 amazon_ads_connections 表...")
        try:
            supabase.table('amazon_ads_connections').select('id').limit(1).execute()
            logger.info("amazon_ads_connections 表已存在")
        except Exception as e:
            logger.info(f"創建 amazon_ads_connections 表...")
            logger.warning("需要手動在 Supabase 界面創建 amazon_ads_connections 表")
        
        return True
    except Exception as e:
        logger.error(f"初始化 Supabase 表時出錯: {str(e)}")
        return False

# 嘗試初始化表
init_result = init_supabase_tables()
logger.info(f"Supabase 表初始化結果: {'成功' if init_result else '失敗'}")

class AmazonAdsService:
    """Amazon Ads API 服務"""
    
    def __init__(self):
        self.client_id = settings.AMAZON_ADS_CLIENT_ID
        self.client_secret = settings.AMAZON_ADS_CLIENT_SECRET
        self.redirect_uri = settings.AMAZON_ADS_REDIRECT_URI
        
        # Amazon Ads API 相關端點
        self.auth_host = "https://www.amazon.com/ap/oa"
        self.token_host = "https://api.amazon.com/auth/o2/token"
        self.api_host = "https://advertising-api.amazon.com"
        
        logger.info(f"AmazonAdsService 初始化完成，使用重定向 URL: {self.redirect_uri}")
    
    def generate_auth_url(self, user_id: str) -> Tuple[str, str]:
        """
        生成授權 URL
        
        Args:
            user_id: 用戶 ID
        
        Returns:
            Tuple[str, str]: (授權 URL, 狀態碼)
        """
        # 生成狀態參數用於防止 CSRF 攻擊
        state = str(uuid.uuid4())
        
        # 如果 Supabase 客戶端不可用，僅返回 URL，不保存狀態
        if supabase:
            # 儲存狀態到 Supabase
            try:
                supabase.table('amazon_ads_states').insert({
                    'state': state,
                    'user_id': user_id,
                    'created_at': datetime.now().isoformat()
                }).execute()
                logger.info(f"已保存授權狀態: {state} 用於用戶 {user_id}")
            except Exception as e:
                logger.error(f"保存授權狀態時出錯: {str(e)}")
        else:
            logger.warning("無法保存授權狀態：Supabase 客戶端不可用")
        
        # 構建授權 URL
        auth_url = (
            f"{self.auth_host}"
            f"?client_id={self.client_id}"
            f"&scope=advertising::campaign_management"
            f"&response_type=code"
            f"&redirect_uri={self.redirect_uri}"
            f"&state={state}"
        )
        
        logger.info(f"生成授權 URL: {auth_url}")
        return auth_url, state
    
    async def exchange_authorization_code(self, code: str) -> Dict[str, Any]:
        """
        交換授權碼獲取訪問令牌
        
        Args:
            code: 授權碼
        
        Returns:
            Dict[str, Any]: 包含訪問令牌和刷新令牌的響應
        """
        logger.info(f"正在交換授權碼: {code[:5]}...")
        
        payload = {
            "grant_type": "authorization_code",
            "code": code,
            "redirect_uri": self.redirect_uri,
            "client_id": self.client_id,
            "client_secret": self.client_secret
        }
        
        try:
            async with httpx.AsyncClient() as client:
                response = await client.post(self.token_host, data=payload)
                response.raise_for_status()
                result = response.json()
                logger.info(f"成功獲取訪問令牌")
                return result
        except Exception as e:
            logger.error(f"交換授權碼時出錯: {str(e)}")
            raise
    
    async def refresh_access_token(self, refresh_token: str) -> Dict[str, Any]:
        """
        使用刷新令牌獲取新的訪問令牌
        
        Args:
            refresh_token: 刷新令牌
        
        Returns:
            Dict[str, Any]: 包含新訪問令牌的響應
        """
        logger.info("正在刷新訪問令牌...")
        
        payload = {
            "grant_type": "refresh_token",
            "refresh_token": refresh_token,
            "client_id": self.client_id,
            "client_secret": self.client_secret
        }
        
        try:
            async with httpx.AsyncClient() as client:
                response = await client.post(self.token_host, data=payload)
                response.raise_for_status()
                result = response.json()
                logger.info("成功刷新訪問令牌")
                return result
        except Exception as e:
            logger.error(f"刷新訪問令牌時出錯: {str(e)}")
            raise
    
    async def get_profiles(self, access_token: str) -> List[Dict[str, Any]]:
        """
        獲取 Amazon Ads 配置檔案列表
        
        Args:
            access_token: 訪問令牌
        
        Returns:
            List[Dict[str, Any]]: 配置檔案列表
        """
        logger.info("正在獲取 Amazon Ads 配置檔案...")
        
        headers = {
            "Authorization": f"Bearer {access_token}",
            "Amazon-Advertising-API-ClientId": self.client_id
        }
        
        try:
            async with httpx.AsyncClient() as client:
                response = await client.get(f"{self.api_host}/v2/profiles", headers=headers)
                response.raise_for_status()
                profiles = response.json()
                logger.info(f"成功獲取 {len(profiles)} 個配置檔案")
                return profiles
        except Exception as e:
            logger.error(f"獲取配置檔案時出錯: {str(e)}")
            raise
    
    async def save_connection(self, user_id: str, profile: Dict[str, Any], refresh_token: str) -> AmazonAdsConnection:
        """
        保存連接信息到數據庫
        
        Args:
            user_id: 用戶 ID
            profile: 配置檔案信息
            refresh_token: 刷新令牌
        
        Returns:
            AmazonAdsConnection: 創建的連接
        """
        logger.info(f"正在保存連接: 用戶={user_id}, 配置檔案={profile.get('profileId', '')}")
        
        # 加密刷新令牌
        try:
            encrypted_token = encrypt_token(refresh_token)
        except Exception as e:
            logger.error(f"加密令牌時出錯: {str(e)}")
            encrypted_token = refresh_token  # 發生錯誤時使用原始令牌
        
        # 創建連接對象
        connection = AmazonAdsConnection(
            user_id=user_id,
            profile_id=str(profile.get("profileId", "")),
            country_code=profile.get("countryCode", ""),
            currency_code=profile.get("currencyCode", ""),
            marketplace_id=profile.get("accountInfo", {}).get("marketplaceStringId", ""),
            account_name=profile.get("accountInfo", {}).get("name", ""),
            account_type=profile.get("accountInfo", {}).get("type", ""),
            refresh_token=encrypted_token
        )
        
        # 保存到 Supabase
        if supabase:
            try:
                result = supabase.table('amazon_ads_connections').insert(
                    connection.to_dict()
                ).execute()
                logger.info(f"連接保存成功: ID={connection.profile_id}")
            except Exception as e:
                logger.error(f"保存連接時出錯: {str(e)}")
        else:
            logger.warning("無法保存連接：Supabase 客戶端不可用")
        
        # 返回創建的連接
        return connection
    
    async def get_user_connections(self, user_id: str) -> List[AmazonAdsConnection]:
        """
        獲取用戶的 Amazon Ads 連接
        
        Args:
            user_id: 用戶 ID
        
        Returns:
            List[AmazonAdsConnection]: 連接列表
        """
        logger.info(f"正在獲取用戶連接: user_id={user_id}")
        
        if not supabase:
            logger.warning("無法獲取連接：Supabase 客戶端不可用")
            return []
        
        try:
            result = supabase.table('amazon_ads_connections').select('*').eq('user_id', user_id).execute()
            
            # 轉換為連接對象
            connections = []
            for item in result.data:
                connections.append(AmazonAdsConnection.from_dict(item))
            
            logger.info(f"成功獲取 {len(connections)} 個連接")
            return connections
        except Exception as e:
            logger.error(f"獲取用戶連接時出錯: {str(e)}")
            return []
    
    async def get_connection_by_profile_id(self, profile_id: str) -> Optional[AmazonAdsConnection]:
        """
        通過配置檔案 ID 獲取連接
        
        Args:
            profile_id: 配置檔案 ID
        
        Returns:
            Optional[AmazonAdsConnection]: 找到的連接或 None
        """
        logger.info(f"正在通過配置檔案 ID 獲取連接: profile_id={profile_id}")
        
        if not supabase:
            logger.warning("無法獲取連接：Supabase 客戶端不可用")
            return None
        
        try:
            result = supabase.table('amazon_ads_connections').select('*').eq('profile_id', profile_id).execute()
            
            if not result.data:
                logger.warning(f"未找到配置檔案 ID 為 {profile_id} 的連接")
                return None
            
            logger.info(f"成功獲取連接: ID={profile_id}")
            return AmazonAdsConnection.from_dict(result.data[0])
        except Exception as e:
            logger.error(f"通過配置檔案 ID 獲取連接時出錯: {str(e)}")
            return None
    
    async def delete_connection(self, profile_id: str) -> bool:
        """
        刪除連接
        
        Args:
            profile_id: 配置檔案 ID
        
        Returns:
            bool: 刪除成功返回 True
        """
        logger.info(f"正在刪除連接: profile_id={profile_id}")
        
        if not supabase:
            logger.warning("無法刪除連接：Supabase 客戶端不可用")
            return False
        
        try:
            result = supabase.table('amazon_ads_connections').delete().eq('profile_id', profile_id).execute()
            
            # 檢查是否刪除成功
            success = len(result.data) > 0
            logger.info(f"連接刪除{'成功' if success else '失敗'}: ID={profile_id}")
            return success
        except Exception as e:
            logger.error(f"刪除連接時出錯: {str(e)}")
            return False
    
    def validate_state(self, state: str) -> Optional[str]:
        """
        驗證狀態參數並返回關聯的用戶 ID
        
        Args:
            state: 狀態參數
        
        Returns:
            Optional[str]: 用戶 ID 或 None
        """
        logger.info(f"正在驗證狀態參數: state={state}")
        
        if not supabase:
            logger.warning("無法驗證狀態：Supabase 客戶端不可用")
            # 開發模式下，允許所有狀態
            return "dev_user_id"
        
        try:
            # 從 Supabase 獲取狀態記錄
            result = supabase.table('amazon_ads_states').select('*').eq('state', state).execute()
            
            if not result.data:
                logger.warning(f"未找到狀態: {state}")
                return None
            
            # 刪除使用過的狀態記錄
            supabase.table('amazon_ads_states').delete().eq('state', state).execute()
            
            # 返回用戶 ID
            user_id = result.data[0].get('user_id')
            logger.info(f"狀態驗證成功: 用戶={user_id}")
            return user_id
        except Exception as e:
            logger.error(f"驗證狀態時出錯: {str(e)}")
            return None

# 創建服務實例
amazon_ads_service = AmazonAdsService()
