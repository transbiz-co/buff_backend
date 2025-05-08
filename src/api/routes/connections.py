from fastapi import APIRouter, HTTPException, Depends, Query, Path, Request
from fastapi.responses import RedirectResponse, JSONResponse
from typing import Optional, List
from datetime import datetime

from ...models.schemas.amazon_ads import (
    AmazonAdsConnectionResponse,
    AmazonAdsProfile,
    AccessTokenResponse,
    AuthUrlResponse,
    AmazonAdsConnectionStatus
)
from ...services.amazon_ads import amazon_ads_service, supabase
from ...core.config import settings

router = APIRouter(
    prefix="/connections", 
    tags=["connections"],
    responses={404: {"description": "未找到"}}
)

# 健康檢查 API
@router.get(
    "/health",
    summary="健康檢查",
    description="檢查 API 和 Supabase 數據庫連接狀態",
    responses={
        200: {
            "description": "系統健康狀態",
            "content": {
                "application/json": {
                    "example": {
                        "status": "healthy",
                        "api": {
                            "status": "ok",
                            "timestamp": "2023-07-05T12:34:56.789Z"
                        },
                        "database": {
                            "status": "ok",
                            "connection_success": True,
                            "tables": ["amazon_ads_states", "amazon_ads_connections"]
                        }
                    }
                }
            }
        },
        500: {"description": "系統不健康"}
    }
)
async def health_check():
    """
    健康檢查 API
    
    返回:
        API 和 Supabase 數據庫連接狀態
    """
    import logging
    logger = logging.getLogger(__name__)
    
    health_status = {
        "api": {
            "status": "ok",
            "timestamp": datetime.now().isoformat()
        },
        "database": {
            "status": "unknown",
            "connection_success": False,
            "tables": []
        }
    }
    
    # 檢查 Supabase 連接
    if not supabase:
        health_status["database"]["status"] = "error"
        health_status["database"]["message"] = "Supabase client not initialized"
        health_status["status"] = "unhealthy"
        return JSONResponse(status_code=500, content=health_status)
    
    try:
        # 嘗試檢查表格
        tables_to_check = ["amazon_ads_states", "amazon_ads_connections"]
        available_tables = []
        
        for table in tables_to_check:
            try:
                result = supabase.table(table).select('id').limit(1).execute()
                available_tables.append(table)
            except Exception as e:
                logger.error(f"檢查表格 {table} 時出錯: {str(e)}")
        
        if len(available_tables) == len(tables_to_check):
            health_status["database"]["status"] = "ok"
        else:
            health_status["database"]["status"] = "partial"
            
        health_status["database"]["connection_success"] = True
        health_status["database"]["tables"] = available_tables
        health_status["status"] = "healthy"
        
        return health_status
    except Exception as e:
        logger.error(f"健康檢查時出錯: {str(e)}")
        health_status["database"]["status"] = "error"
        health_status["database"]["message"] = str(e)
        health_status["status"] = "unhealthy"
        return JSONResponse(status_code=500, content=health_status)

# 獲取授權 URL
@router.get(
    "/amazon-ads/authorize", 
    response_model=AuthUrlResponse,
    summary="獲取 Amazon Ads 授權 URL",
    description="""
    生成 Amazon Ads 授權 URL，用戶需要訪問該 URL 完成授權流程。
    授權成功後，用戶將被重定向回應用的回調 URL。
    """
)
async def authorize_amazon_ads(
    user_id: str = Query(..., description="用戶 ID")
):
    """
    生成 Amazon Ads 授權 URL
    
    參數:
        user_id: 用戶 ID
        
    返回:
        JSON 對象，包含授權 URL
    """
    auth_url, _ = amazon_ads_service.generate_auth_url(user_id)
    return {"auth_url": auth_url}

# 處理授權回調
@router.get(
    "/amazon-ads/callback",
    summary="Amazon Ads 授權回調處理",
    description="""
    處理 Amazon Ads 授權流程的回調。
    當用戶授權應用訪問其 Amazon Ads 帳戶後，Amazon 會將用戶重定向到此端點。
    此端點接收授權碼，交換訪問令牌，獲取用戶配置檔案，並將用戶重定向回前端。
    """
)
async def amazon_ads_callback(
    code: str = Query(..., description="Amazon 授權碼"),
    state: str = Query(..., description="狀態參數，用於防止 CSRF 攻擊")
):
    """
    處理 Amazon Ads 授權回調
    
    參數:
        code: Amazon 授權碼
        state: 狀態參數，用於防止 CSRF 攻擊
        
    返回:
        重定向到前端，帶有成功或錯誤狀態
    """
    # 驗證狀態參數
    user_id = amazon_ads_service.validate_state(state)
    if not user_id:
        raise HTTPException(status_code=400, detail="Invalid state parameter")
    
    try:
        # 交換授權碼獲取訪問令牌
        token_response = await amazon_ads_service.exchange_authorization_code(code)
        access_token = token_response.get("access_token")
        refresh_token = token_response.get("refresh_token")
        
        if not access_token or not refresh_token:
            raise HTTPException(status_code=400, detail="Failed to get access token")
        
        # 獲取配置檔案
        profiles = await amazon_ads_service.get_profiles(access_token)
        
        if not profiles:
            raise HTTPException(status_code=400, detail="No Amazon Ads profiles found")
        
        # 保存連接信息
        for profile in profiles:
            await amazon_ads_service.save_connection(user_id, profile, refresh_token)
        
        # 重定向回前端
        frontend_url = f"{settings.FRONTEND_URL}/connections?status=success"
        return RedirectResponse(url=frontend_url)
    
    except Exception as e:
        # 處理錯誤
        frontend_url = f"{settings.FRONTEND_URL}/connections?status=error&message={str(e)}"
        return RedirectResponse(url=frontend_url)

# 獲取連接狀態
@router.get(
    "/amazon-ads/status", 
    response_model=AmazonAdsConnectionStatus,
    summary="獲取 Amazon Ads 連接狀態",
    description="獲取用戶的 Amazon Ads 連接狀態，包括是否已連接以及已連接的配置檔案列表。",
    responses={
        200: {
            "description": "連接狀態和配置檔案列表",
            "content": {
                "application/json": {
                    "example": {
                        "connected": True,
                        "user_id": "123456",
                        "profiles": [
                            {
                                "profile_id": "111222333",
                                "country_code": "US",
                                "currency_code": "USD",
                                "marketplace_id": "ATVPDKIKX0DER",
                                "account_name": "My Amazon Account",
                                "account_type": "seller",
                                "timezone": "America/Los_Angeles"
                            }
                        ]
                    }
                }
            }
        }
    }
)
async def get_connection_status(
    user_id: str = Query(..., description="用戶 ID")
):
    """
    獲取用戶的 Amazon Ads 連接狀態
    
    參數:
        user_id: 用戶 ID
        
    返回:
        連接狀態和配置檔案列表
    """
    connections = await amazon_ads_service.get_user_connections(user_id)
    
    if not connections:
        return {"connected": False, "user_id": user_id, "profiles": []}
    
    # 構建配置檔案列表
    profiles = []
    for conn in connections:
        profiles.append(AmazonAdsProfile(
            profile_id=conn.profile_id,
            country_code=conn.country_code,
            currency_code=conn.currency_code,
            marketplace_id=conn.marketplace_id,
            account_name=conn.account_name,
            account_type=conn.account_type,
            timezone=""  # 可選字段
        ))
    
    return {
        "connected": True,
        "user_id": user_id,
        "profiles": profiles
    }

# 刷新訪問令牌
@router.post(
    "/amazon-ads/refresh-token", 
    response_model=AccessTokenResponse,
    summary="刷新 Amazon Ads 訪問令牌",
    description="""
    使用保存的刷新令牌獲取新的訪問令牌。
    Amazon Ads 訪問令牌有效期為 60 分鐘，需要定期刷新。
    """,
    responses={
        200: {
            "description": "新的訪問令牌",
            "content": {
                "application/json": {
                    "example": {
                        "access_token": "Atza|IQEBLjAsAhRmHjNgHpi0U-Dme37rR6CuUpSR...",
                        "token_type": "bearer",
                        "expires_in": 3600
                    }
                }
            }
        },
        404: {"description": "找不到連接"},
        400: {"description": "刷新令牌失敗"}
    }
)
async def refresh_token(
    profile_id: str = Query(..., description="Amazon Ads 配置檔案 ID")
):
    """
    刷新 Amazon Ads 訪問令牌
    
    參數:
        profile_id: Amazon Ads 配置檔案 ID
        
    返回:
        新的訪問令牌
    """
    # 獲取連接
    connection = await amazon_ads_service.get_connection_by_profile_id(profile_id)
    
    if not connection:
        raise HTTPException(status_code=404, detail="Connection not found")
    
    # 解密刷新令牌
    from ...core.security import decrypt_token
    refresh_token = decrypt_token(connection.refresh_token)
    
    try:
        # 刷新訪問令牌
        token_response = await amazon_ads_service.refresh_access_token(refresh_token)
        
        # 返回新的訪問令牌
        return {
            "access_token": token_response.get("access_token"),
            "token_type": token_response.get("token_type", "bearer"),
            "expires_in": token_response.get("expires_in", 3600)
        }
    except Exception as e:
        raise HTTPException(status_code=400, detail=f"Failed to refresh token: {str(e)}")

# 刪除連接
@router.delete(
    "/amazon-ads/{profile_id}",
    summary="刪除 Amazon Ads 連接",
    description="刪除指定配置檔案 ID 的 Amazon Ads 連接。",
    responses={
        200: {
            "description": "連接刪除成功",
            "content": {
                "application/json": {
                    "example": {
                        "status": "success",
                        "message": "Connection deleted successfully"
                    }
                }
            }
        },
        404: {"description": "找不到連接"}
    }
)
async def delete_connection(
    profile_id: str = Path(..., description="要刪除的 Amazon Ads 配置檔案 ID")
):
    """
    刪除 Amazon Ads 連接
    
    參數:
        profile_id: 要刪除的 Amazon Ads 配置檔案 ID
        
    返回:
        刪除操作的結果
    """
    result = await amazon_ads_service.delete_connection(profile_id)
    
    if not result:
        raise HTTPException(status_code=404, detail="Connection not found")
    
    return {"status": "success", "message": "Connection deleted successfully"}
