from pydantic import BaseModel, Field
from typing import List, Optional, Dict, Any
from datetime import datetime


class AmazonAdsProfile(BaseModel):
    """Amazon Ads 配置檔案結構"""
    profile_id: str = Field(..., description="Amazon Ads 配置檔案 ID")
    country_code: str = Field(..., description="國家代碼，例如 US, JP, UK")
    currency_code: str = Field(..., description="幣別代碼，例如 USD, JPY, GBP")
    timezone: Optional[str] = Field(None, description="時區信息，例如 America/Los_Angeles")
    marketplace_id: str = Field(..., description="Amazon 市場 ID")
    account_name: str = Field(..., description="帳戶名稱")
    account_type: str = Field(..., description="帳戶類型：seller（賣家）、vendor（廠商）或 agency（代理商）")
    amazon_account_name: str = Field(..., description="廣告帳號所屬的主Amazon帳號名稱")
    is_active: bool = Field(False, description="廣告帳號是否啟用，默認為false")
    main_account_id: Optional[int] = Field(None, description="關聯的Amazon主帳號ID")
    main_account_name: Optional[str] = Field(None, description="主帳號姓名")
    main_account_email: Optional[str] = Field(None, description="主帳號電子郵件")

    class Config:
        schema_extra = {
            "example": {
                "profile_id": "111222333",
                "country_code": "US",
                "currency_code": "USD",
                "timezone": "America/Los_Angeles",
                "marketplace_id": "ATVPDKIKX0DER",
                "account_name": "My Amazon Account",
                "account_type": "seller",
                "amazon_account_name": "amazon_user",
                "is_active": False,
                "main_account_id": 1,
                "main_account_name": "John Doe",
                "main_account_email": "john.doe@example.com"
            }
        }


class AmazonAdsConnectionResponse(BaseModel):
    """Amazon Ads 連接響應模型"""
    connected: bool = Field(..., description="是否已連接 Amazon Ads 帳戶")
    profiles: List[AmazonAdsProfile] = Field(default_factory=list, description="已連接的配置檔案列表")

    class Config:
        schema_extra = {
            "example": {
                "connected": True,
                "profiles": [
                    {
                        "profile_id": "111222333",
                        "country_code": "US",
                        "currency_code": "USD",
                        "timezone": "America/Los_Angeles",
                        "marketplace_id": "ATVPDKIKX0DER",
                        "account_name": "My Amazon Account",
                        "account_type": "seller"
                    }
                ]
            }
        }


class AmazonAdsConnectionCreate(BaseModel):
    """創建 Amazon Ads 連接模型"""
    user_id: str = Field(..., description="用戶 ID")
    profile_id: str = Field(..., description="Amazon Ads 配置檔案 ID")
    country_code: str = Field(..., description="國家代碼")
    currency_code: str = Field(..., description="幣別代碼")
    marketplace_id: str = Field(..., description="Amazon 市場 ID")
    account_name: str = Field(..., description="帳戶名稱")
    account_type: str = Field(..., description="帳戶類型")
    refresh_token: str = Field(..., description="刷新令牌，用於獲取新的訪問令牌")


class AmazonAdsConnectionStatus(BaseModel):
    """Amazon Ads 連接狀態"""
    connected: bool = Field(..., description="是否已連接 Amazon Ads 帳戶")
    user_id: Optional[str] = Field(None, description="用戶 ID")
    profiles: List[AmazonAdsProfile] = Field(default_factory=list, description="已連接的配置檔案列表")

    class Config:
        schema_extra = {
            "example": {
                "connected": True,
                "user_id": "123456",
                "profiles": [
                    {
                        "profile_id": "111222333",
                        "country_code": "US",
                        "currency_code": "USD",
                        "timezone": "America/Los_Angeles",
                        "marketplace_id": "ATVPDKIKX0DER",
                        "account_name": "My Amazon Account",
                        "account_type": "seller"
                    }
                ]
            }
        }


class AuthUrlResponse(BaseModel):
    """授權 URL 響應"""
    auth_url: str = Field(..., description="Amazon Ads 授權 URL，用戶需要訪問此 URL 完成授權流程")

    class Config:
        schema_extra = {
            "example": {
                "auth_url": "https://www.amazon.com/ap/oa?client_id=amzn1.application-oa2-client...&scope=advertising::campaign_management profile&response_type=code&redirect_uri=..."
            }
        }


class AccessTokenResponse(BaseModel):
    """訪問令牌響應"""
    access_token: str = Field(..., description="Amazon Ads API 訪問令牌")
    token_type: str = Field("bearer", description="令牌類型，通常為 bearer")
    expires_in: int = Field(3600, description="令牌有效期，單位為秒，通常為 3600 秒（1 小時）")

    class Config:
        schema_extra = {
            "example": {
                "access_token": "Atza|IQEBLjAsAhRmHjNgHpi0U-Dme37rR6CuUpSR...",
                "token_type": "bearer",
                "expires_in": 3600
            }
        }


class AmazonAdsCallback(BaseModel):
    """Amazon Ads 回調請求數據"""
    code: str = Field(..., description="授權碼，用於交換訪問令牌")
    state: str = Field(..., description="狀態參數，用於防止 CSRF 攻擊")
