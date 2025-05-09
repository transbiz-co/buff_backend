from typing import Dict, Any, Optional, List
from datetime import datetime
import json

class AmazonAdsConnection:
    """Amazon Ads 連接數據模型"""
    
    def __init__(
        self,
        id: Optional[str] = None,
        user_id: str = "",
        profile_id: str = "",
        country_code: str = "",
        currency_code: str = "",
        marketplace_id: str = "",
        account_name: str = "",
        account_type: str = "",
        refresh_token: str = "",
        is_active: bool = False,
        main_account_id: Optional[int] = None,
        main_account_name: Optional[str] = None,
        main_account_email: Optional[str] = None,
        created_at: Optional[datetime] = None,
        updated_at: Optional[datetime] = None
    ):
        self.id = id
        self.user_id = user_id
        self.profile_id = profile_id
        self.country_code = country_code
        self.currency_code = currency_code
        self.marketplace_id = marketplace_id
        self.account_name = account_name
        self.account_type = account_type
        self.refresh_token = refresh_token
        self.is_active = is_active
        self.main_account_id = main_account_id
        self.main_account_name = main_account_name
        self.main_account_email = main_account_email
        self.created_at = created_at or datetime.now()
        self.updated_at = updated_at or datetime.now()
    
    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> 'AmazonAdsConnection':
        """從字典創建連接對象"""
        
        # 嘗試將日期時間字符串轉換為datetime對象
        created_at = data.get("created_at")
        if isinstance(created_at, str):
            try:
                created_at = datetime.fromisoformat(created_at.replace('Z', '+00:00'))
            except (ValueError, TypeError):
                created_at = None
        
        updated_at = data.get("updated_at")
        if isinstance(updated_at, str):
            try:
                updated_at = datetime.fromisoformat(updated_at.replace('Z', '+00:00'))
            except (ValueError, TypeError):
                updated_at = None
        
        return cls(
            id=data.get("id"),
            user_id=data.get("user_id", ""),
            profile_id=data.get("profile_id", ""),
            country_code=data.get("country_code", ""),
            currency_code=data.get("currency_code", ""),
            marketplace_id=data.get("marketplace_id", ""),
            account_name=data.get("account_name", ""),
            account_type=data.get("account_type", ""),
            refresh_token=data.get("refresh_token", ""),
            is_active=data.get("is_active", False),
            main_account_id=data.get("main_account_id"),
            main_account_name=data.get("main_account_name"),
            main_account_email=data.get("main_account_email"),
            created_at=created_at,
            updated_at=updated_at
        )
    
    def to_dict(self) -> Dict[str, Any]:
        """
        轉換為字典以存儲到 Supabase
        
        確保 datetime 對象被轉換為 ISO 格式字符串，解決 JSON 序列化問題
        同時排除 id 為 None 的情況，以便使用數據庫的 DEFAULT 值
        """
        result = {
            'user_id': self.user_id,
            'profile_id': self.profile_id,
            'country_code': self.country_code,
            'currency_code': self.currency_code,
            'marketplace_id': self.marketplace_id,
            'account_name': self.account_name,
            'account_type': self.account_type,
            'refresh_token': self.refresh_token,
            'is_active': self.is_active,
            'main_account_id': self.main_account_id,
            'created_at': self.created_at.isoformat() if isinstance(self.created_at, datetime) else self.created_at,
            'updated_at': self.updated_at.isoformat() if isinstance(self.updated_at, datetime) else self.updated_at
        }
        
        # 只有在 id 不為 None 時才添加到結果中
        if self.id is not None:
            result['id'] = self.id
            
        return result

    def __str__(self) -> str:
        """連接對象的字符串表示"""
        return f"AmazonAdsConnection(id={self.id}, profile_id={self.profile_id}, account_name={self.account_name})"
