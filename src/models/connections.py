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
        self.created_at = created_at or datetime.now()
        self.updated_at = updated_at or datetime.now()
    
    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> 'AmazonAdsConnection':
        """從字典創建連接對象"""
        return cls(
            id=data.get('id'),
            user_id=data.get('user_id', ''),
            profile_id=data.get('profile_id', ''),
            country_code=data.get('country_code', ''),
            currency_code=data.get('currency_code', ''),
            marketplace_id=data.get('marketplace_id', ''),
            account_name=data.get('account_name', ''),
            account_type=data.get('account_type', ''),
            refresh_token=data.get('refresh_token', ''),
            created_at=data.get('created_at'),
            updated_at=data.get('updated_at')
        )
    
    def to_dict(self) -> Dict[str, Any]:
        """轉換為字典以存儲到 Supabase"""
        return {
            'id': self.id,
            'user_id': self.user_id,
            'profile_id': self.profile_id,
            'country_code': self.country_code,
            'currency_code': self.currency_code,
            'marketplace_id': self.marketplace_id,
            'account_name': self.account_name,
            'account_type': self.account_type,
            'refresh_token': self.refresh_token,
            'created_at': self.created_at,
            'updated_at': self.updated_at
        }
