import httpx
import json
import uuid
from datetime import datetime, timedelta
from typing import Dict, Any, List, Optional, Tuple, Union
import os
import logging
import traceback
import base64
import gzip
import io
import json
import asyncio

from ..core.config import settings
from ..core.security import encrypt_token, decrypt_token
from ..models.connections import AmazonAdsConnection
from supabase import create_client, Client
from contextlib import asynccontextmanager

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

# 添加清理過期狀態記錄的函數
def cleanup_expired_states(expiration_minutes: int = 30):
    """
    清理過期的 state 記錄
    
    Args:
        expiration_minutes: 過期時間（分鐘）
    """
    if not supabase:
        logger.warning("無法清理過期狀態：Supabase 客戶端不可用")
        return
    
    try:
        # 計算過期時間
        expiration_time = datetime.now() - timedelta(minutes=expiration_minutes)
        expiration_iso = expiration_time.isoformat()
        
        logger.info(f"正在清理 {expiration_iso} 之前的過期狀態記錄")
        
        # 刪除過期記錄
        # Supabase JS 支持 lt (less than) 運算符，但 Python 客戶端可能有所不同
        # 這裡使用過濾器功能刪除早於指定時間的記錄
        result = supabase.table('amazon_ads_states').select('*').lt('created_at', expiration_iso).execute()
        expired_states = result.data
        
        if expired_states:
            for state in expired_states:
                supabase.table('amazon_ads_states').delete().eq('id', state.get('id')).execute()
            
            logger.info(f"已清理 {len(expired_states)} 條過期狀態記錄")
        else:
            logger.info("沒有找到過期的狀態記錄")
    except Exception as e:
        logger.error(f"清理過期狀態記錄時出錯: {str(e)}")
            
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
            
            # 首次啟動時清理過期狀態
            cleanup_expired_states()
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
        
        # 超時設置（分鐘）
        self.state_expiration_minutes = 30
        
        logger.info(f"AmazonAdsService 初始化完成，使用重定向 URL: {self.redirect_uri}")
    
    @asynccontextmanager
    async def httpx_client(self):
        """
        創建一個 HTTPX 異步客戶端的上下文管理器
        
        Returns:
            AsyncContextManager: 異步客戶端上下文
        """
        async with httpx.AsyncClient(timeout=30.0) as client:
            yield client
    
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
        
        # 嘗試清理過期的狀態記錄
        try:
            cleanup_expired_states(self.state_expiration_minutes)
        except Exception as e:
            logger.warning(f"清理過期狀態記錄時出錯: {str(e)}")
        
        # 如果 Supabase 客戶端不可用，僅返回 URL，不保存狀態
        if supabase:
            # 儲存狀態到 Supabase
            try:
                current_time = datetime.now().isoformat()
                insert_result = supabase.table('amazon_ads_states').insert({
                    'state': state,
                    'user_id': user_id,
                    'created_at': current_time
                }).execute()
                
                if insert_result.data:
                    logger.info(f"已保存授權狀態: {state} 用於用戶 {user_id}, 時間 {current_time}")
                else:
                    logger.warning(f"保存授權狀態失敗，未返回數據: {insert_result}")
            except Exception as e:
                logger.error(f"保存授權狀態時出錯: {str(e)}")
                import traceback
                logger.error(f"詳細錯誤: {traceback.format_exc()}")
        else:
            logger.warning("無法保存授權狀態：Supabase 客戶端不可用")
        
        # 構建授權 URL
        auth_url = (
            f"{self.auth_host}"
            f"?client_id={self.client_id}"
            f"&scope=advertising::campaign_management profile"
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
        logger.info(f"正在交換授權碼: {code[:10]}...（已截斷）")
        
        payload = {
            "grant_type": "authorization_code",
            "code": code,
            "redirect_uri": self.redirect_uri,
            "client_id": self.client_id,
            "client_secret": self.client_secret
        }
        
        try:
            logger.info(f"發送請求到 Amazon token 端點: {self.token_host}")
            logger.info(f"使用參數: grant_type={payload['grant_type']}, redirect_uri={payload['redirect_uri']}, client_id={payload['client_id'][:8]}...（已截斷）")
            
            async with httpx.AsyncClient() as client:
                response = await client.post(self.token_host, data=payload)
                response.raise_for_status()
                result = response.json()
                
                # 記錄響應結果（截斷敏感信息）
                logger.info("成功獲取訪問令牌")
                logger.info(f"響應狀態碼: {response.status_code}")
                
                # 記錄返回的token類型和過期時間
                token_type = result.get("token_type", "unknown")
                expires_in = result.get("expires_in", "unknown")
                logger.info(f"Token類型: {token_type}, 過期時間: {expires_in}秒")
                
                # 記錄access_token和refresh_token（已截斷）
                if "access_token" in result:
                    logger.info(f"Access Token: {result['access_token'][:10]}...（已截斷）")
                if "refresh_token" in result:
                    logger.info(f"Refresh Token: {result['refresh_token'][:10]}...（已截斷）")
                    
                return result
        except Exception as e:
            logger.error(f"交換授權碼時出錯: {str(e)}")
            if isinstance(e, httpx.HTTPStatusError):
                logger.error(f"HTTP錯誤狀態碼: {e.response.status_code}")
                logger.error(f"響應內容: {e.response.text}")
            raise
    
    async def refresh_access_token(self, refresh_token: str) -> Dict[str, Any]:
        """
        使用刷新令牌獲取新的訪問令牌
        
        Args:
            refresh_token: 刷新令牌
        
        Returns:
            Dict[str, Any]: 包含新訪問令牌和可能的新刷新令牌的響應
        """
        logger.info("正在刷新訪問令牌...")
        
        # === 調試 refresh_token ===
        logger.info(f"Refresh Token 長度: {len(refresh_token)}")
        logger.info(f"Refresh Token 字符: {refresh_token[:20]}...{refresh_token[-20:]}")
        # === 調試結束 ===
        
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
                
                # === 調試返回的 token ===
                if "access_token" in result:
                    access_token = result["access_token"]
                    logger.info(f"新的 Access Token 長度: {len(access_token)}")
                    logger.info(f"新的 Access Token 字符: {access_token[:20]}...{access_token[-20:]}")
                # === 調試結束 ===
                
                # 檢查是否返回了新的刷新令牌
                if "refresh_token" in result:
                    logger.info("獲取到新的刷新令牌")
                
                return result
        except Exception as e:
            logger.error(f"刷新訪問令牌時出錯: {str(e)}")
            if isinstance(e, httpx.HTTPStatusError):
                logger.error(f"HTTP錯誤狀態碼: {e.response.status_code}")
                logger.error(f"響應內容: {e.response.text}")
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
            "Amazon-Advertising-API-ClientId": self.client_id,
            "Content-Type": "application/json",
            "Accept": "application/json"
        }
        
        try:
            endpoint = f"{self.api_host}/v2/profiles"
            logger.info(f"發送請求到端點: {endpoint}")
            logger.info(f"請求頭部: Client-ID={self.client_id}")
            
            async with httpx.AsyncClient(timeout=30.0) as client:
                response = await client.get(endpoint, headers=headers)
                response.raise_for_status()
                profiles = response.json()
                
                # 記錄響應狀態和獲取的配置檔案數量
                logger.info(f"響應狀態碼: {response.status_code}")
                logger.info(f"成功獲取 {len(profiles)} 個配置檔案")
                
                # 只詳細記錄前幾個配置檔案，避免日誌過長
                max_detail_profiles = 5  # 只記錄前5個的詳細信息
                for i, profile in enumerate(profiles):
                    if i < max_detail_profiles:
                        logger.info(f"配置檔案 #{i+1} 詳細信息:")
                        logger.info(f"  - profileId: {profile.get('profileId', 'N/A')}")
                        logger.info(f"  - countryCode: {profile.get('countryCode', 'N/A')}")
                        
                        # 簡化記錄accountInfo內容
                        account_info = profile.get("accountInfo", {})
                        logger.info(f"  - accountInfo: id={account_info.get('id', 'N/A')}, name={account_info.get('name', 'N/A')}, type={account_info.get('type', 'N/A')}")
                    elif i == max_detail_profiles:
                        logger.info(f"還有 {len(profiles) - max_detail_profiles} 個配置檔案 (省略詳細信息)")
                        break
            
            return profiles
        except Exception as e:
            import traceback
            logger.error(f"獲取配置檔案時出錯: {repr(e)}")
            logger.error(f"詳細錯誤信息: {traceback.format_exc()}")
            if isinstance(e, httpx.HTTPStatusError):
                logger.error(f"HTTP錯誤狀態碼: {e.response.status_code}")
                logger.error(f"響應內容: {e.response.text}")
            elif isinstance(e, httpx.ConnectError):
                logger.error(f"連接錯誤: 無法連接到 {self.api_host}")
            elif isinstance(e, httpx.ReadTimeout):
                logger.error(f"讀取超時: 請求超時")
            raise
    
    async def get_amazon_user_profile(self, access_token: str) -> Dict[str, Any]:
        """
        獲取 Amazon 主帳號用戶資料
        
        Args:
            access_token: 訪問令牌
            
        Returns:
            Dict[str, Any]: 用戶資料，包含 user_id, name, email 等信息
        """
        logger.info("正在獲取 Amazon 主帳號用戶資料...")
        
        try:
            profile_endpoint = "https://api.amazon.com/user/profile"
            headers = {
                "Authorization": f"Bearer {access_token}"
            }
            
            logger.info(f"發送請求到端點: {profile_endpoint}")
            
            async with httpx.AsyncClient() as client:
                response = await client.get(profile_endpoint, headers=headers)
                
                # 檢查響應狀態
                if response.status_code == 200:
                    user_profile = response.json()
                    
                    # 日誌記錄，但隱藏敏感信息
                    safe_profile = {
                        "user_id": user_profile.get("user_id", "N/A"),
                        "name": user_profile.get("name", "N/A"),
                        "email": user_profile.get("email", "")[:3] + "***" if user_profile.get("email") else "N/A",
                        "postal_code": user_profile.get("postal_code", "N/A")
                    }
                    
                    logger.info(f"成功獲取用戶資料: {safe_profile}")
                    return user_profile
                else:
                    logger.error(f"獲取用戶資料失敗，狀態碼: {response.status_code}")
                    logger.error(f"錯誤響應: {response.text}")
                    raise ValueError(f"Failed to get user profile: HTTP {response.status_code}")
                
        except Exception as e:
            import traceback
            logger.error(f"獲取用戶資料時出錯: {repr(e)}")
            logger.error(f"詳細錯誤信息: {traceback.format_exc()}")
            raise
            
    async def save_main_account(self, user_id: str, main_account_info: Dict[str, Any], refresh_token: Optional[str] = None) -> int:
        """
        保存主帳號信息到數據庫
        
        Args:
            user_id: 用戶 ID
            main_account_info: 主帳號信息
            refresh_token: Amazon授權刷新令牌
            
        Returns:
            int: 主帳號記錄的 ID
        """
        logger.info(f"正在保存主帳號信息: 用戶={user_id}")
        
        amazon_user_id = main_account_info.get("user_id", "")
        email = main_account_info.get("email", "")
        name = main_account_info.get("name", "")
        
        # 記錄要保存的主帳號信息
        logger.info(f"主帳號信息詳情:")
        logger.info(f"  - amazon_user_id: {amazon_user_id}")
        logger.info(f"  - name: {name}")
        logger.info(f"  - email: {email}")
        
        if not supabase:
            logger.warning("無法保存主帳號信息：Supabase 客戶端不可用")
            return None
        
        try:
            # 檢查是否已存在相同 amazon_user_id 的記錄
            existing_account = supabase.table('amazon_main_accounts').select('*').eq('amazon_user_id', amazon_user_id).execute()
            
            # 如果提供了 refresh_token，則加密
            encrypted_refresh_token = None
            if refresh_token:
                try:
                    encrypted_refresh_token = encrypt_token(refresh_token)
                    logger.info(f"已加密 refresh_token (加密前長度: {len(refresh_token)}, 加密後長度: {len(encrypted_refresh_token)})")
                except Exception as e:
                    logger.error(f"加密 refresh_token 時出錯: {str(e)}")
                    logger.error(traceback.format_exc())
                    encrypted_refresh_token = None
            
            if existing_account and existing_account.data:
                logger.info(f"找到已存在的主帳號記錄，ID={existing_account.data[0]['id']}")
                
                # 更新現有記錄
                update_data = {
                    'email': email,
                    'name': name,
                    'updated_at': datetime.now().isoformat()
                }
                
                if encrypted_refresh_token:
                    update_data['refresh_token'] = encrypted_refresh_token
                
                supabase.table('amazon_main_accounts').update(update_data).eq('id', existing_account.data[0]['id']).execute()
                
                logger.info(f"已更新主帳號記錄")
                return existing_account.data[0]['id']
                
            else:
                # 創建新記錄
                logger.info(f"創建新的主帳號記錄")
                insert_data = {
                    'user_id': user_id,
                    'amazon_user_id': amazon_user_id,
                    'email': email,
                    'name': name,
                    'created_at': datetime.now().isoformat(),
                    'updated_at': datetime.now().isoformat()
                }
                
                if encrypted_refresh_token:
                    insert_data['refresh_token'] = encrypted_refresh_token
                
                result = supabase.table('amazon_main_accounts').insert(insert_data).execute()
                
                if result and result.data:
                    account_id = result.data[0]['id']
                    logger.info(f"主帳號信息保存成功: ID={account_id}")
                    return account_id
                else:
                    logger.warning("主帳號信息可能未成功保存，無返回數據")
                    return None
                    
        except Exception as e:
            logger.error(f"保存主帳號信息時出錯: {str(e)}")
            logger.error(f"詳細錯誤:")
            logger.error(traceback.format_exc())
            return None
            
    async def save_connection(self, user_id: str, profile: Dict[str, Any], refresh_token: str, main_account_id: Optional[int] = None) -> AmazonAdsConnection:
        """
        保存連接信息到數據庫
        
        Args:
            user_id: 用戶 ID
            profile: 配置檔案信息
            refresh_token: 刷新令牌
            main_account_id: 主帳號 ID
        
        Returns:
            AmazonAdsConnection: 創建的連接
        """
        profile_id = profile.get("profileId", "")
        logger.info(f"保存連接: 用戶={user_id}, 配置檔案ID={profile_id}")
        
        # 精簡日誌輸出，只記錄關鍵信息
        account_info = profile.get("accountInfo", {})
        marketplace_id = account_info.get("marketplaceStringId", "")
        account_name = account_info.get("name", "")
        account_type = account_info.get("type", "")
        account_id = account_info.get("id", "")
        valid_payment = account_info.get("validPaymentMethod", False)
        
        # 提取時區信息
        timezone = profile.get("timezone", "")
        
        # 提取每日預算
        daily_budget = profile.get("dailyBudget")
        if daily_budget is not None:
            # 處理科學計數法格式
            try:
                if isinstance(daily_budget, str) and "E" in daily_budget.upper():
                    daily_budget = float(daily_budget)
            except Exception as e:
                logger.warning(f"轉換每日預算時出錯: {str(e)}")
        
        # 加密刷新令牌
        try:
            encrypted_token = encrypt_token(refresh_token)
        except Exception as e:
            logger.error(f"加密令牌時出錯: {str(e)}")
            encrypted_token = refresh_token  # 發生錯誤時使用原始令牌
            logger.warning("使用未加密的令牌作為後備")
        
        # 創建連接對象
        connection = AmazonAdsConnection(
            user_id=user_id,
            profile_id=str(profile_id),
            country_code=profile.get("countryCode", ""),
            currency_code=profile.get("currencyCode", ""),
            marketplace_id=marketplace_id,
            account_name=account_name,
            account_type=account_type,
            refresh_token=encrypted_token,
            is_active=False,  # 新連接默認為禁用狀態
            main_account_id=main_account_id,
            timezone=timezone,
            daily_budget=daily_budget,
            account_id=account_id,
            valid_payment=valid_payment
        )
        
        # 保存到 Supabase
        if supabase:
            try:
                result = supabase.table('amazon_ads_connections').insert(
                    connection.to_dict()
                ).execute()
                
                if not result or not result.data:
                    logger.warning(f"連接可能未成功保存，無返回數據")
            except Exception as e:
                logger.error(f"保存連接時出錯: {str(e)}")
                logger.error(f"詳細錯誤: {traceback.format_exc()}")
        else:
            logger.warning("無法保存連接：Supabase 客戶端不可用")
        
        # 返回創建的連接
        return connection
    
    async def bulk_save_connections(self, user_id: str, profiles: List[Dict[str, Any]], refresh_token: str, main_account_id: Optional[int] = None) -> int:
        """
        批量保存多個連接信息到數據庫，先檢查已存在的記錄，只保存新的配置檔案
        
        Args:
            user_id: 用戶 ID
            profiles: 配置檔案信息列表
            refresh_token: 刷新令牌
            main_account_id: 主帳號 ID
        
        Returns:
            int: 成功保存的連接數量
        """
        if not profiles:
            logger.warning("沒有配置檔案需要保存")
            return 0
            
        if not supabase:
            logger.warning("無法批量保存連接：Supabase 客戶端不可用")
            # 如果 Supabase 不可用，逐個保存
            saved_count = 0
            for profile in profiles:
                await self.save_connection(user_id, profile, refresh_token, main_account_id)
                saved_count += 1
            return saved_count
        
        # 收集所有 profile ID 用於後續比較
        profile_ids = [str(profile.get("profileId", "")) for profile in profiles]
        logger.info(f"待處理的配置檔案數量：{len(profile_ids)}")
        
        # 查詢用戶現有的連接記錄
        try:
            logger.info(f"檢查用戶 {user_id} 已有的連接記錄")
            existing_result = supabase.table('amazon_ads_connections') \
                .select('profile_id') \
                .eq('user_id', user_id) \
                .execute()
            
            # 獲取已存在的 profile ID 集合
            existing_profile_ids = set()
            if existing_result and existing_result.data:
                existing_profile_ids = {item['profile_id'] for item in existing_result.data}
                logger.info(f"用戶已有 {len(existing_profile_ids)} 個連接記錄")
            
            # 過濾出需要新增的配置檔案
            new_profile_ids = [pid for pid in profile_ids if pid not in existing_profile_ids]
            logger.info(f"需要新增的配置檔案數量：{len(new_profile_ids)}")
            
            # 如果沒有新配置檔案需要保存，直接返回
            if not new_profile_ids:
                logger.info("沒有新的配置檔案需要保存")
                return 0
                
            # 過濾出需要新增的配置檔案對象
            new_profiles = [p for p in profiles if str(p.get("profileId", "")) in new_profile_ids]
        except Exception as e:
            logger.error(f"檢查現有連接記錄時出錯: {str(e)}")
            # 如果檢查失敗，按原計劃處理所有配置檔案
            new_profiles = profiles
            logger.warning("無法檢查現有記錄，將處理所有配置檔案")
        
        logger.info(f"批量保存 {len(new_profiles)} 個新配置檔案")
        
        # 加密刷新令牌 (所有連接共用同一個)
        try:
            encrypted_token = encrypt_token(refresh_token)
        except Exception as e:
            logger.error(f"加密令牌時出錯: {str(e)}")
            encrypted_token = refresh_token  # 發生錯誤時使用原始令牌
            logger.warning("使用未加密的令牌作為後備")
        
        # 準備批量插入數據
        connections_data = []
        current_time = datetime.now().isoformat()
        
        for profile in new_profiles:
            profile_id = profile.get("profileId", "")
            account_info = profile.get("accountInfo", {})
            marketplace_id = account_info.get("marketplaceStringId", "")
            account_name = account_info.get("name", "")
            account_type = account_info.get("type", "")
            account_id = account_info.get("id", "")
            valid_payment = account_info.get("validPaymentMethod", False)
            
            # 提取時區信息
            timezone = profile.get("timezone", "")
            
            # 提取每日預算
            daily_budget = profile.get("dailyBudget")
            if daily_budget is not None:
                # 處理科學計數法格式
                try:
                    if isinstance(daily_budget, str) and "E" in daily_budget.upper():
                        daily_budget = float(daily_budget)
                except Exception as e:
                    logger.warning(f"轉換每日預算時出錯: {str(e)}")
            
            # 創建連接數據
            connection_dict = {
                'user_id': user_id,
                'profile_id': str(profile_id),
                'country_code': profile.get("countryCode", ""),
                'currency_code': profile.get("currencyCode", ""),
                'marketplace_id': marketplace_id,
                'account_name': account_name,
                'account_type': account_type,
                'refresh_token': encrypted_token,
                'is_active': False,  # 新連接默認為禁用狀態
                'main_account_id': main_account_id,
                'timezone': timezone,  # 添加時區
                'daily_budget': daily_budget,  # 添加每日預算
                'account_id': account_id,  # 添加賬號ID
                'valid_payment': valid_payment,  # 添加支付方式有效性
                'created_at': current_time,
                'updated_at': current_time
            }
            
            connections_data.append(connection_dict)
        
        # 如果沒有需要保存的連接數據，直接返回
        if not connections_data:
            logger.info("沒有新的連接數據需要保存")
            return 0
        
        # 使用批量插入 (每批最多50個記錄，避免請求過大)
        batch_size = 50
        total_saved = 0
        
        for i in range(0, len(connections_data), batch_size):
            batch = connections_data[i:min(i+batch_size, len(connections_data))]
            
            try:
                logger.info(f"保存批次 {i//batch_size + 1}/{(len(connections_data)-1)//batch_size + 1}，共 {len(batch)} 個連接")
                result = supabase.table('amazon_ads_connections').insert(batch).execute()
                
                if result and result.data:
                    batch_saved = len(result.data)
                    total_saved += batch_saved
                    logger.info(f"批次保存成功: {batch_saved}/{len(batch)} 個連接")
                else:
                    logger.warning(f"批次保存可能未成功，無返回數據")
            except Exception as e:
                logger.error(f"批次保存時出錯: {str(e)}")
                import traceback
                logger.error(f"詳細錯誤: {traceback.format_exc()}")
                
                # 如果批量保存失敗，嘗試逐個保存這一批次
                logger.warning(f"嘗試逐個保存批次中的連接")
                for conn_data in batch:
                    try:
                        # 單個保存
                        result = supabase.table('amazon_ads_connections').insert(conn_data).execute()
                        
                        if result and result.data:
                            total_saved += 1
                    except Exception as inner_e:
                        logger.error(f"單個保存連接時出錯: {str(inner_e)}")
        
        logger.info(f"批量保存完成，共保存 {total_saved}/{len(connections_data)} 個新連接")
        return total_saved
    
    async def get_all_connections(self) -> List[AmazonAdsConnection]:
        """
        獲取所有 Amazon Ads 連接，使用外鍵關聯一次性獲取主帳號信息
        
        Returns:
            List[AmazonAdsConnection]: 連接列表
        """
        logger.info("正在獲取所有 Amazon Ads 連接")
        
        if not supabase:
            logger.warning("無法獲取連接：Supabase 客戶端不可用")
            return []
        
        try:
            # 使用外鍵關聯語法一次性獲取所有數據，避免N+1查詢問題
            result = supabase.table('amazon_ads_connections').select("""
                *,
                amazon_main_accounts!main_account_id (
                    id,
                    name,
                    email
                )
            """).execute()
            
            # 處理結果
            connections = []
            for item in result.data:
                conn_data = dict(item)
                # 從嵌套數據中提取主帳號信息
                main_account = conn_data.pop('amazon_main_accounts', None)
                if main_account:
                    conn_data['main_account_name'] = main_account.get('name')
                    conn_data['main_account_email'] = main_account.get('email')
                
                # 創建連接對象
                connection = AmazonAdsConnection.from_dict(conn_data)
                connections.append(connection)
            
            logger.info(f"成功獲取 {len(connections)} 個連接（使用外鍵關聯查詢）")
            return connections
        except Exception as e:
            logger.error(f"獲取所有連接時出錯: {str(e)}")
            logger.error(f"詳細錯誤: {traceback.format_exc()}")
            return []
            
    async def get_user_connections(self, user_id: str) -> List[AmazonAdsConnection]:
        """
        獲取用戶的 Amazon Ads 連接，使用外鍵關聯一次性獲取主帳號信息
        
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
            # 使用外鍵關聯語法一次性獲取所有數據，避免N+1查詢問題
            result = supabase.table('amazon_ads_connections').select("""
                *,
                amazon_main_accounts!main_account_id (
                    id,
                    name,
                    email
                )
            """).eq('user_id', user_id).execute()
            
            # 處理結果
            connections = []
            for item in result.data:
                conn_data = dict(item)
                # 從嵌套數據中提取主帳號信息
                main_account = conn_data.pop('amazon_main_accounts', None)
                if main_account:
                    conn_data['main_account_name'] = main_account.get('name')
                    conn_data['main_account_email'] = main_account.get('email')
                
                # 創建連接對象
                connection = AmazonAdsConnection.from_dict(conn_data)
                connections.append(connection)
            
            logger.info(f"成功獲取 {len(connections)} 個連接（使用外鍵關聯查詢）")
            return connections
        except Exception as e:
            logger.error(f"獲取用戶連接時出錯: {str(e)}")
            logger.error(f"詳細錯誤: {traceback.format_exc()}")
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
        刪除特定配置檔案的連接
        
        Args:
            profile_id: Amazon Ads 配置檔案 ID
            
        Returns:
            bool: 是否成功刪除
        """
        try:
            # 刪除連接
            result = supabase.table('amazon_ads_connections').delete().eq('profile_id', profile_id).execute()
            
            if len(result.data) > 0:
                return True
            else:
                return False
        except Exception as e:
            logger.error(f"刪除連接時出錯: {e}")
            return False

    async def update_connection_status(self, profile_id: str, is_active: bool) -> bool:
        """
        更新連接狀態（啟用/禁用）
        
        Args:
            profile_id: Amazon Ads 配置檔案 ID
            is_active: 是否啟用連接
            
        Returns:
            bool: 是否成功更新
        """
        try:
            # 更新連接狀態
            result = supabase.table('amazon_ads_connections').update({
                'is_active': is_active,
                'updated_at': datetime.now().isoformat()
            }).eq('profile_id', profile_id).execute()
            
            if len(result.data) > 0:
                return True
            else:
                logger.warning(f"未找到配置檔案 {profile_id} 的連接")
                return False
        except Exception as e:
            logger.error(f"更新連接狀態時出錯: {e}")
            return False

    async def create_report(self, 
                          profile_id: str, 
                          access_token: str, 
                          ad_product: str,
                          start_date: Optional[str] = None, 
                          end_date: Optional[str] = None,
                          user_id: Optional[str] = None,
                          report_name: Optional[str] = None,
                          report_type_id: Optional[str] = None) -> Dict[str, Any]:
        """
        創建Amazon廣告報告請求
        
        Args:
            profile_id: Amazon Ads 配置檔案 ID
            access_token: 訪問令牌
            ad_product: 廣告產品類型 (SPONSORED_PRODUCTS, SPONSORED_BRANDS, SPONSORED_DISPLAY)
            start_date: 報告開始日期 (YYYY-MM-DD)，默認為前7天
            end_date: 報告結束日期 (YYYY-MM-DD)，默認為前1天
            user_id: 用戶ID，用於記錄
            report_name: 報告名稱，默認自動生成
            report_type_id: 報告類型ID，默認根據ad_product選擇
            
        Returns:
            Dict[str, Any]: 報告請求的響應
        """
        logger.info(f"開始創建 {ad_product} 報告，profile_id={profile_id}")
        
        # 確定報告類型ID
        if not report_type_id:
            if ad_product == "SPONSORED_PRODUCTS":
                report_type_id = "spCampaigns"
            elif ad_product == "SPONSORED_BRANDS":
                report_type_id = "sbCampaigns"
            elif ad_product == "SPONSORED_DISPLAY":
                report_type_id = "sdCampaigns"
            else:
                raise ValueError(f"不支援的廣告產品類型: {ad_product}")
        
        # 確定日期範圍
        today = datetime.now()
        if not start_date:
            # 默認前7天
            start_date = (today - timedelta(days=7)).strftime("%Y-%m-%d")
        if not end_date:
            # 默認前1天
            end_date = (today - timedelta(days=1)).strftime("%Y-%m-%d")
            
        # 確定報告名稱
        if not report_name:
            report_name = f"{ad_product} report {start_date} to {end_date} for Profile {profile_id}"
        
        # 構建報告配置
        configuration = {
            "adProduct": ad_product,
            "groupBy": ["campaign"],
            "columns": self._get_report_columns(ad_product),
            "reportTypeId": report_type_id,
            "timeUnit": "DAILY",
            "format": "GZIP_JSON"
        }
        
        # 構建請求體
        request_body = {
            "name": report_name,
            "startDate": start_date,
            "endDate": end_date,
            "configuration": configuration
        }
        
        # 設置請求頭
        headers = {
            "Authorization": f"Bearer {access_token}",
            "Amazon-Advertising-API-ClientId": self.client_id,
            "Amazon-Advertising-API-Scope": profile_id,
            "Content-Type": "application/vnd.createasyncreportrequest.v3+json"
        }
        
        # 調用API創建報告
        endpoint = f"{self.api_host}/reporting/reports"
        
        try:
            async with self.httpx_client() as client:
                response = await client.post(endpoint, headers=headers, json=request_body)
                response.raise_for_status()
                report_data = response.json()
                
                logger.info(f"成功創建報告: report_id={report_data.get('reportId')}, status={report_data.get('status')}")
                
                # 將報告信息保存到數據庫
                if supabase:
                    try:
                        report_record = {
                            "report_id": report_data.get("reportId"),
                            "user_id": user_id,
                            "profile_id": profile_id,
                            "name": report_data.get("name"),
                            "status": report_data.get("status"),
                            "ad_product": ad_product,
                            "report_type_id": report_type_id,
                            "start_date": start_date,
                            "end_date": end_date,
                            "time_unit": configuration.get("timeUnit"),
                            "format": configuration.get("format"),
                            "configuration": configuration,
                            "created_at": datetime.now().isoformat(),
                            "updated_at": datetime.now().isoformat(),
                            "amazon_created_at": report_data.get("createdAt"),
                            "amazon_updated_at": report_data.get("updatedAt"),
                            "url": report_data.get("url"),
                            "url_expires_at": report_data.get("urlExpiresAt"),
                            "file_size": report_data.get("fileSize"),
                            "failure_reason": report_data.get("failureReason")
                        }
                        
                        result = supabase.table('amazon_ads_reports').upsert(
                            report_record,
                            on_conflict='profile_id,ad_product,start_date,end_date,report_type_id'
                        ).execute()
                        logger.info(f"報告信息已保存/更新到數據庫: {report_data.get('reportId')}")
                    except Exception as db_error:
                        logger.error(f"保存報告信息到數據庫時出錯: {str(db_error)}")
                        logger.error(traceback.format_exc())
                
                return report_data
        except Exception as e:
            if isinstance(e, httpx.HTTPStatusError):
                status_code = e.response.status_code
                response_text = e.response.text
                
                if status_code == 400:
                    logger.error(f"Amazon API 請求無效 (400 Bad Request): {response_text}")
                    error_message = f"Amazon API 返回錯誤 (400 Bad Request): {response_text}"
                    raise ValueError(error_message)
                    
                elif status_code == 425:
                    logger.info(f"檢測到重複報告請求 (425): {response_text}")
                    
                    duplicate_report_id = None
                    try:
                        error_response = json.loads(response_text)
                        if "detail" in error_response:
                            detail = error_response["detail"]
                            logger.info(f"重複報告詳情: {detail}")
                            
                            if "duplicate of :" in detail:
                                duplicate_report_id = detail.split("duplicate of :")[1].strip()
                                logger.info(f"重複報告 ID: {duplicate_report_id}")
                    except Exception as extract_error:
                        logger.info(f"無法從響應中提取報告 ID: {str(extract_error)}")
                    
                    error_message = f"報告請求重複"
                    if duplicate_report_id:
                        error_message += f", 重複報告 ID: {duplicate_report_id}"
                    
                    raise ValueError(f"DUPLICATE_REPORT:{duplicate_report_id}:{error_message}")
                    
                else:
                    logger.error(f"Amazon API 請求失敗 ({status_code}): {response_text}")
                    error_message = f"Amazon API 返回錯誤 ({status_code}): {response_text}"
                    raise ValueError(error_message)
            else:
                logger.error(f"創建報告時出錯: {str(e)}")
                raise
    
    def _get_report_columns(self, ad_product: str) -> List[str]:
        """
        獲取指定廣告產品的報告欄位
        
        Args:
            ad_product: 廣告產品類型
            
        Returns:
            List[str]: 欄位列表
        """
        
        match ad_product:
            case "SPONSORED_PRODUCTS":
                return [ 
                    "impressions","clicks","cost","purchases1d","purchases7d","purchases14d","purchases30d",
                    "purchasesSameSku1d","purchasesSameSku7d","purchasesSameSku14d","purchasesSameSku30d",
                    "unitsSoldClicks1d","unitsSoldClicks7d","unitsSoldClicks14d","unitsSoldClicks30d",
                    "sales1d","sales7d","sales14d","sales30d",
                    "attributedSalesSameSku1d","attributedSalesSameSku7d","attributedSalesSameSku14d","attributedSalesSameSku30d",
                    "unitsSoldSameSku1d","unitsSoldSameSku7d","unitsSoldSameSku14d","unitsSoldSameSku30d",
                    "kindleEditionNormalizedPagesRead14d","kindleEditionNormalizedPagesRoyalties14d",
                    "qualifiedBorrows","royaltyQualifiedBorrows","addToList","date",
                    "campaignBiddingStrategy","costPerClick","clickThroughRate","spend",
                    "acosClicks14d","roasClicks14d","retailer",
                    "campaignName","campaignId","campaignStatus","campaignBudgetAmount","campaignBudgetType","campaignRuleBasedBudgetAmount",
                    "campaignApplicableBudgetRuleId","campaignApplicableBudgetRuleName","campaignBudgetCurrencyCode","topOfSearchImpressionShare"
                ]
            case "SPONSORED_BRANDS":
                return [
                    "campaignName","campaignId","campaignStatus","impressions","clicks","cost","date",
                    "brandedSearches","purchases","purchasesPromoted","detailPageViews",
                    "newToBrandPurchasesRate","newToBrandPurchases","newToBrandPurchasesPercentage",
                    "sales","salesPromoted","newToBrandSales","newToBrandSalesPercentage","newToBrandUnitsSold","newToBrandUnitsSoldPercentage",
                    "unitsSold","viewClickThroughRate","video5SecondViewRate","video5SecondViews",
                    "videoCompleteViews","videoFirstQuartileViews","videoMidpointViews","videoThirdQuartileViews",
                    "videoUnmutes","viewableImpressions","viewabilityRate",
                    "brandedSearchesClicks","purchasesClicks","detailPageViewsClicks","newToBrandPurchasesClicks","salesClicks",
                    "newToBrandSalesClicks","newToBrandUnitsSoldClicks","unitsSoldClicks","costType","newToBrandDetailPageViews",
                    "newToBrandDetailPageViewsClicks","newToBrandDetailPageViewRate","newToBrandECPDetailPageView",
                    "addToCart","addToCartClicks","addToCartRate","eCPAddToCart",
                    "kindleEditionNormalizedPagesRead14d","kindleEditionNormalizedPagesRoyalties14d",
                    "qualifiedBorrows","qualifiedBorrowsFromClicks","royaltyQualifiedBorrows","royaltyQualifiedBorrowsFromClicks",
                    "addToList","addToListFromClicks","longTermSales","longTermROAS",
                    "campaignBudgetAmount","campaignBudgetCurrencyCode","campaignBudgetType","topOfSearchImpressionShare","campaignRuleBasedBudgetAmount"
                ]
            case "SPONSORED_DISPLAY":
                return [
                    "date","purchasesClicks","purchasesPromotedClicks","detailPageViewsClicks","newToBrandPurchasesClicks",
                    "salesClicks","salesPromotedClicks","newToBrandSalesClicks","unitsSoldClicks","newToBrandUnitsSoldClicks",
                    "campaignId","campaignName","clicks","cost","campaignBudgetCurrencyCode","impressions","purchases","detailPageViews",
                    "sales","unitsSold","impressionsViews","newToBrandPurchases","newToBrandUnitsSold","brandedSearchesClicks",
                    "brandedSearches","brandedSearchesViews","brandedSearchRate","eCPBrandSearch","videoCompleteViews",
                    "videoFirstQuartileViews","videoMidpointViews","videoThirdQuartileViews","videoUnmutes","viewabilityRate",
                    "viewClickThroughRate","addToCart","addToCartViews","addToCartClicks","addToCartRate","eCPAddToCart",
                    "qualifiedBorrows","qualifiedBorrowsFromClicks","qualifiedBorrowsFromViews","royaltyQualifiedBorrows",
                    "royaltyQualifiedBorrowsFromClicks","royaltyQualifiedBorrowsFromViews","addToList","addToListFromClicks",
                    "addToListFromViews","linkOuts","leadFormOpens","leads","longTermSales","longTermROAS","newToBrandSales",
                    "campaignStatus","campaignBudgetAmount","costType","impressionsFrequencyAverage","cumulativeReach",
                    "newToBrandDetailPageViews","newToBrandDetailPageViewViews","newToBrandDetailPageViewClicks",
                    "newToBrandDetailPageViewRate","newToBrandECPDetailPageView"
                ]
            case _:
                return []
            
    async def get_report_status(self, 
                              profile_id: str, 
                              access_token: str, 
                              report_id: str) -> Dict[str, Any]:
        """
        獲取報告狀態
        
        Args:
            profile_id: Amazon Ads 配置檔案 ID
            access_token: 訪問令牌
            report_id: 報告ID
            
        Returns:
            Dict[str, Any]: 報告狀態信息
        """
        logger.info(f"獲取報告狀態: report_id={report_id}")
        
        # 設置請求頭
        headers = {
            "Authorization": f"Bearer {access_token}",
            "Amazon-Advertising-API-ClientId": self.client_id,
            "Amazon-Advertising-API-Scope": profile_id,
            "Accept": "application/json"
        }
        
        # 調用API獲取報告狀態
        endpoint = f"{self.api_host}/reporting/reports/{report_id}"
        
        try:
            async with self.httpx_client() as client:
                response = await client.get(endpoint, headers=headers)
                response.raise_for_status()
                status_data = response.json()
                
                logger.info(f"報告狀態: report_id={report_id}, status={status_data.get('status')}")
                
                # 更新數據庫中的報告狀態
                if supabase:
                    try:
                        update_data = {
                            "status": status_data.get("status"),
                            "updated_at": datetime.now().isoformat(),
                            "amazon_updated_at": status_data.get("updatedAt"),
                            "url": status_data.get("url"),
                            "url_expires_at": status_data.get("urlExpiresAt"),
                            "file_size": status_data.get("fileSize"),
                            "failure_reason": status_data.get("failureReason")
                        }
                        
                        result = supabase.table('amazon_ads_reports').update(update_data).eq('report_id', report_id).execute()
                        logger.info(f"報告狀態已更新: {report_id}")
                    except Exception as db_error:
                        logger.error(f"更新報告狀態到數據庫時出錯: {str(db_error)}")
                
                return status_data
        except Exception as e:
            logger.error(f"獲取報告狀態時出錯: {str(e)}")
            logger.error(traceback.format_exc())
            raise
            
    async def create_profile_reports(self, 
                                profile_id: str, 
                                ad_product: str,
                                start_date: Optional[str] = None, 
                                end_date: Optional[str] = None) -> Dict[str, Any]:
        """
        為 profile 申請 spCampaign, sbCampaign, sdCampaign report
        
        Args:
            profile_id: Amazon Ads 配置檔案 ID
            ad_product: 廣告產品類型
            start_date: 報告開始日期 (YYYY-MM-DD)，默認為前7天
            end_date: 報告結束日期 (YYYY-MM-DD)，默認為前1天
            
        Returns:
            Dict[str, Any]: 處理結果統計
        """
        logger.info(f"為配置檔案 {profile_id} 創建 {ad_product} 報告")
        
        # 獲取連接詳情
        connection = await self.get_connection_by_profile_id(profile_id)
        
        if not connection:
            logger.warning(f"未找到配置檔案 {profile_id} 的連接")
            return {
                "success": False,
                "message": "Connection not found",
                "created_reports": 0
            }
        
        # 處理結果統計
        result_stats = {
            "success": True,
            "profile_id": profile_id,
            "created_reports": 0
        }
        
        try:
            # 解密刷新令牌
            refresh_token = decrypt_token(connection.refresh_token)
            
            # 刷新訪問令牌
            token_response = await self.refresh_access_token(refresh_token)
            access_token = token_response.get("access_token")
            
            if not access_token:
                logger.error(f"無法獲取訪問令牌: {profile_id}")
                return {
                    "success": False,
                    "message": "Failed to get access token",
                    "created_reports": 0
                }
            
            try:
                # 創建報告請求
                retry_count = 0
                max_retries = 3
                retry_delay = 2  # 秒
                
                while retry_count <= max_retries:
                    try:
                        report_data = await self.create_report(
                            profile_id=profile_id,
                            access_token=access_token,
                            ad_product=ad_product,
                            start_date=start_date,
                            end_date=end_date,
                            user_id=connection.user_id
                        )
                        
                        if report_data and report_data.get("reportId"):
                            result_stats["created_reports"] = 1
                            result_stats["message"] = f"Successfully created {ad_product} report"
                            result_stats["report_id"] = report_data.get("reportId")
                            break  # 成功創建報告，退出重試循環
                        else:
                            result_stats["success"] = False
                            result_stats["message"] = f"Failed to create {ad_product} report"
                            break  # 沒有獲取到報告ID，退出重試循環
                    except ValueError as ve:
                        error_message = str(ve)
                        if "425 Too Early" in error_message and retry_count < max_retries:
                            retry_count += 1
                            logger.warning(f"獲取到 425 Too Early 錯誤，將進行第 {retry_count} 次重試（共 {max_retries} 次）")
                            await asyncio.sleep(retry_delay * retry_count)  # 指數退避策略
                            continue
                        else:
                            result_stats["success"] = False
                            result_stats["message"] = error_message
                            break
                    except Exception as e:
                        logger.error(f"為配置檔案 {profile_id} 創建 {ad_product} 報告時出錯: {str(e)}")
                        result_stats["success"] = False
                        result_stats["message"] = str(e)
                        break
            except Exception as e:
                logger.error(f"為配置檔案 {profile_id} 創建 {ad_product} 報告時出錯: {str(e)}")
                result_stats["success"] = False
                result_stats["message"] = str(e)
            
            return result_stats
            
        except Exception as e:
            logger.error(f"為配置檔案 {profile_id} 創建報告時出錯: {str(e)}")
            logger.error(traceback.format_exc())
            return {
                "success": False,
                "message": str(e),
                "created_reports": 0
            }
            
    async def get_profile_campaigns(self, profile_id: str, ad_type: str) -> List[Dict[str, Any]]:
        """
        獲取配置檔案的所有廣告活動
        
        Args:
            profile_id: Amazon Ads 配置檔案 ID
            ad_type: 廣告類型 (SP, SB, SD)
            
        Returns:
            List[Dict[str, Any]]: 廣告活動列表
        """
        logger.info(f"獲取配置檔案 {profile_id} 的 {ad_type} 廣告活動")
        
        if not supabase:
            logger.error("Supabase 客戶端不可用")
            return []
        
        try:
            # 查詢廣告活動
            result = supabase.table('amazon_ads_campaigns').select('*').eq('profile_id', profile_id).eq('ad_type', ad_type).execute()
            
            if not result.data:
                logger.info(f"未找到配置檔案 {profile_id} 的 {ad_type} 廣告活動")
                return []
            
            return result.data
        except Exception as e:
            logger.error(f"獲取配置檔案 {profile_id} 的廣告活動時出錯: {str(e)}")
            logger.error(traceback.format_exc())
            return []

    async def bulk_refresh_tokens(self, user_id: str) -> Dict[str, Any]:
        """
        批量刷新用戶所有 Amazon Ads 連接的訪問令牌
        
        Args:
            user_id: 用戶 ID
            
        Returns:
            Dict[str, Any]: 包含刷新結果的詳細信息
        """
        logger.info(f"開始為用戶 {user_id} 批量刷新 Amazon Ads 訪問令牌")
        
        if not supabase:
            logger.warning("無法刷新令牌：Supabase 客戶端不可用")
            return {
                "success": False,
                "message": "Database client not available",
                "total": 0,
                "refreshed": 0,
                "failed": 0
            }
        
        # 獲取用戶的所有連接
        connections = await self.get_user_connections(user_id)
        
        if not connections:
            logger.info(f"用戶 {user_id} 沒有可刷新的連接")
            return {
                "success": True,
                "message": "No connections found to refresh",
                "total": 0,
                "refreshed": 0,
                "failed": 0
            }
        
        logger.info(f"找到 {len(connections)} 個連接需要刷新")
        
        # 記錄處理結果
        total = len(connections)
        refreshed = 0
        failed = 0
        failed_details = []
        
        # 為每個連接刷新令牌
        for connection in connections:
            try:
                # 移除對 is_active 的檢查，處理所有連接
                logger.info(f"正在刷新連接: {connection.profile_id} (啟用狀態: {connection.is_active})")
                
                # 解密刷新令牌
                refresh_token = decrypt_token(connection.refresh_token)
                
                # 刷新訪問令牌
                token_response = await self.refresh_access_token(refresh_token)
                
                # 檢查是否返回了新的刷新令牌
                new_refresh_token = token_response.get("refresh_token")
                if new_refresh_token and new_refresh_token != refresh_token:
                    logger.info(f"獲取到新的刷新令牌: {connection.profile_id}")
                    
                    # 加密新的刷新令牌
                    encrypted_token = encrypt_token(new_refresh_token)
                    
                    # 更新資料庫中的刷新令牌
                    result = supabase.table('amazon_ads_connections').update({
                        'refresh_token': encrypted_token,
                        'updated_at': datetime.now().isoformat()
                    }).eq('profile_id', connection.profile_id).execute()
                    
                    if result and len(result.data) > 0:
                        logger.info(f"成功更新刷新令牌: {connection.profile_id}")
                    else:
                        logger.warning(f"更新刷新令牌可能失敗: {connection.profile_id}")
                
                # 記錄成功結果
                refreshed += 1
                
            except Exception as e:
                # 記錄失敗詳情
                logger.error(f"刷新連接 {connection.profile_id} 時出錯: {str(e)}")
                failed += 1
                failed_details.append({
                    "profile_id": connection.profile_id,
                    "error": str(e)
                })
        
        # 返回處理結果
        result = {
            "success": True,
            "message": f"Successfully refreshed {refreshed} out of {total} connections",
            "total": total,
            "refreshed": refreshed,
            "failed": failed,
            "failed_details": failed_details if failed > 0 else None
        }
        
        logger.info(f"批量刷新完成: 總共 {total} 個連接，成功 {refreshed} 個，失敗 {failed} 個")
        return result

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
            # 嘗試獲取 amazon_ads_states 表的所有記錄（僅用於調試）
            debug_result = supabase.table('amazon_ads_states').select('*').limit(10).execute()
            logger.info(f"表中現有狀態記錄（最多10條）: {[s.get('state') for s in debug_result.data if s]}")
            
            # 從 Supabase 獲取狀態記錄
            logger.info(f"查詢狀態: {state}")
            result = supabase.table('amazon_ads_states').select('*').eq('state', state).execute()
            logger.info(f"狀態查詢結果: {result.data}")
            
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
            # 增加更詳細的錯誤信息
            import traceback
            logger.error(f"詳細錯誤: {traceback.format_exc()}")
            return None

# 創建服務實例
amazon_ads_service = AmazonAdsService()
