import httpx
import json
import uuid
from datetime import datetime, timedelta
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
            
    async def save_main_account(self, user_id: str, main_account_info: Dict[str, Any]) -> int:
        """
        保存主帳號信息到數據庫
        
        Args:
            user_id: 用戶 ID
            main_account_info: 主帳號信息
            
        Returns:
            int: 主帳號記錄的 ID
        """
        logger.info(f"正在保存主帳號信息: 用戶={user_id}")
        
        amazon_user_id = main_account_info.get("user_id", "")
        email = main_account_info.get("email", "")
        name = main_account_info.get("name", "")
        postal_code = main_account_info.get("postal_code", "")
        
        # 記錄要保存的主帳號信息
        logger.info(f"主帳號信息詳情:")
        logger.info(f"  - amazon_user_id: {amazon_user_id}")
        logger.info(f"  - name: {name}")
        logger.info(f"  - postal_code: {postal_code}")
        
        if not supabase:
            logger.warning("無法保存主帳號信息：Supabase 客戶端不可用")
            return None
        
        try:
            # 檢查是否已存在相同 amazon_user_id 的記錄
            existing_account = supabase.table('amazon_main_accounts').select('*').eq('amazon_user_id', amazon_user_id).execute()
            
            if existing_account and existing_account.data:
                logger.info(f"找到已存在的主帳號記錄，ID={existing_account.data[0]['id']}")
                
                # 更新現有記錄
                supabase.table('amazon_main_accounts').update({
                    'email': email,
                    'name': name,
                    'postal_code': postal_code,
                    'updated_at': datetime.now().isoformat()
                }).eq('id', existing_account.data[0]['id']).execute()
                
                logger.info(f"已更新主帳號記錄")
                return existing_account.data[0]['id']
                
            else:
                # 創建新記錄
                logger.info(f"創建新的主帳號記錄")
                result = supabase.table('amazon_main_accounts').insert({
                    'user_id': user_id,
                    'amazon_user_id': amazon_user_id,
                    'email': email,
                    'name': name,
                    'postal_code': postal_code,
                    'created_at': datetime.now().isoformat(),
                    'updated_at': datetime.now().isoformat()
                }).execute()
                
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
            import traceback
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
        logger.info(f"正在保存連接: 用戶={user_id}, 配置檔案ID={profile_id}")
        
        # 記錄完整的配置檔案信息，幫助診斷
        logger.info(f"配置檔案詳細信息:")
        logger.info(f"  - profileId: {profile_id}")
        logger.info(f"  - countryCode: {profile.get('countryCode', 'N/A')}")
        logger.info(f"  - currencyCode: {profile.get('currencyCode', 'N/A')}")
        
        # 記錄accountInfo內容
        account_info = profile.get("accountInfo", {})
        logger.info(f"AccountInfo詳細信息:")
        for key, value in account_info.items():
            logger.info(f"  - {key}: {value}")
        
        # 特別記錄我們關心的字段
        marketplace_id = account_info.get("marketplaceStringId", "")
        account_name = account_info.get("name", "")
        account_type = account_info.get("type", "")
        logger.info(f"關鍵字段: name={account_name}, type={account_type}, marketplaceId={marketplace_id}")
        
        # 加密刷新令牌
        try:
            logger.info("開始加密刷新令牌")
            encrypted_token = encrypt_token(refresh_token)
            logger.info("刷新令牌加密成功")
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
            main_account_id=main_account_id  # 添加主帳號 ID
        )
        
        # 記錄要保存的連接對象信息
        logger.info(f"準備保存的連接對象:")
        logger.info(f"  - user_id: {connection.user_id}")
        logger.info(f"  - profile_id: {connection.profile_id}")
        logger.info(f"  - account_name: {connection.account_name}")
        logger.info(f"  - is_active: {connection.is_active}")
        logger.info(f"  - main_account_id: {connection.main_account_id}")
        
        # 保存到 Supabase
        if supabase:
            try:
                logger.info(f"開始保存到Supabase, 表: amazon_ads_connections")
                result = supabase.table('amazon_ads_connections').insert(
                    connection.to_dict()
                ).execute()
                
                if result and result.data:
                    logger.info(f"連接保存成功: ID={connection.profile_id}")
                    logger.info(f"Supabase返回數據: {result.data}")
                else:
                    logger.warning(f"連接可能未成功保存，無返回數據")
            except Exception as e:
                logger.error(f"保存連接時出錯: {str(e)}")
                logger.error(f"詳細錯誤:")
                import traceback
                logger.error(traceback.format_exc())
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
            # 修改查詢，連接 amazon_main_accounts 表以獲取主帳號信息
            query = """
            SELECT c.*, 
                   m.id as main_account_id, 
                   m.name as main_account_name, 
                   m.email as main_account_email
            FROM amazon_ads_connections c
            LEFT JOIN amazon_main_accounts m ON c.main_account_id = m.id
            WHERE c.user_id = ?
            """
            
            result = supabase.table('amazon_ads_connections').select('*').eq('user_id', user_id).execute()
            
            # 如果需要獲取主帳號信息，還需要額外查詢
            connections = []
            for item in result.data:
                connection = AmazonAdsConnection.from_dict(item)
                
                # 如果有主帳號ID，尋找對應的主帳號信息
                if connection.main_account_id:
                    try:
                        main_account_result = supabase.table('amazon_main_accounts').select('*').eq('id', connection.main_account_id).execute()
                        if main_account_result.data:
                            main_account = main_account_result.data[0]
                            # 添加主帳號信息到連接對象
                            connection.main_account_name = main_account.get('name')
                            connection.main_account_email = main_account.get('email')
                    except Exception as e:
                        logger.error(f"獲取主帳號信息時出錯: {str(e)}")
                
                connections.append(connection)
            
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
