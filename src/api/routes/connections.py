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
    import logging
    import traceback
    logger = logging.getLogger(__name__)
    
    # 詳細記錄收到的授權碼
    logger.info(f"===== Amazon Callback 收到的授權碼 =====")
    logger.info(f"授權碼: {code[:10]}...（已截斷）")
    logger.info(f"狀態參數: {state}")
    
    # 驗證狀態參數
    user_id = amazon_ads_service.validate_state(state)
    if not user_id:
        logger.error(f"狀態參數驗證失敗: {state}")
        # 返回錯誤到前端
        error_msg = "Invalid state parameter"
        frontend_url = f"{settings.FRONTEND_URL}/connections?status=error&message={error_msg}"
        return RedirectResponse(url=frontend_url)
    
    try:
        # 交換授權碼獲取訪問令牌
        logger.info(f"開始交換授權碼獲取訪問令牌")
        token_response = await amazon_ads_service.exchange_authorization_code(code)
        access_token = token_response.get("access_token")
        refresh_token = token_response.get("refresh_token")
        
        # 記錄token信息（截斷顯示）
        if access_token:
            logger.info(f"成功獲取access_token: {access_token[:10]}...（已截斷）")
        else:
            logger.error("未能獲取access_token")
            raise ValueError("Failed to get access token from Amazon Ads API")
            
        if refresh_token:
            logger.info(f"成功獲取refresh_token: {refresh_token[:10]}...（已截斷）")
        else:
            logger.error("未能獲取refresh_token")
            raise ValueError("Failed to get refresh token from Amazon Ads API")
        
        # 獲取授權主帳號資訊
        logger.info(f"開始獲取Amazon主帳號資訊")
        try:
            main_account_info = await amazon_ads_service.get_amazon_user_profile(access_token)
            logger.info(f"成功獲取主帳號資訊: name={main_account_info.get('name', 'N/A')}, email={main_account_info.get('email', 'N/A')}")
        except Exception as e:
            logger.error(f"獲取主帳號資訊時發生異常: {repr(e)}")
            logger.error(f"異常詳情: {traceback.format_exc()}")
            # 繼續處理，主帳號資訊獲取失敗不影響主流程
            main_account_info = None
        
        # 獲取配置檔案
        logger.info(f"開始獲取Amazon Ads配置檔案")
        try:
            profiles = await amazon_ads_service.get_profiles(access_token)
        except Exception as e:
            logger.error(f"獲取配置檔案時發生異常: {repr(e)}")
            logger.error(f"異常詳情: {traceback.format_exc()}")
            raise ValueError(f"Failed to get Amazon Ads profiles: {str(e) or 'Unknown error'}")
        
        # 記錄獲取到的配置檔案數量及關鍵信息
        if profiles:
            logger.info(f"成功獲取{len(profiles)}個配置檔案")
            
            # 設置處理上限，避免一次處理太多配置檔案
            MAX_PROFILES_TO_PROCESS = 120
            if len(profiles) > MAX_PROFILES_TO_PROCESS:
                logger.warning(f"配置檔案數量 ({len(profiles)}) 超過處理上限 ({MAX_PROFILES_TO_PROCESS})，將只處理前 {MAX_PROFILES_TO_PROCESS} 個")
                profiles = profiles[:MAX_PROFILES_TO_PROCESS]
            
            # 記錄前幾個配置檔案的基本信息
            max_log_profiles = 3
            for i, profile in enumerate(profiles):
                if i < max_log_profiles:
                    logger.info(f"配置檔案 #{i+1}:")
                    logger.info(f"  - profileId: {profile.get('profileId', 'N/A')}")
                    logger.info(f"  - countryCode: {profile.get('countryCode', 'N/A')}")
                    logger.info(f"  - accountInfo.name: {profile.get('accountInfo', {}).get('name', 'N/A')}")
                elif i == max_log_profiles:
                    logger.info(f"還有 {len(profiles) - max_log_profiles} 個配置檔案，省略日誌...")
                    break
        else:
            logger.warning("未獲取到任何配置檔案")
            raise ValueError("No Amazon Ads profiles found")
        
        # 保存主帳號資訊
        main_account_id = None
        if main_account_info:
            logger.info(f"開始保存主帳號資訊，用戶ID: {user_id}")
            main_account_id = await amazon_ads_service.save_main_account(user_id, main_account_info)
            logger.info(f"主帳號資訊保存完成，ID: {main_account_id}")
        
        # 保存連接信息
        logger.info(f"開始保存連接信息，用戶ID: {user_id}")
        
        # 使用批量處理來優化性能
        batch_size = 10  # 每批處理10個配置檔案
        total_saved = 0
        
        for i in range(0, len(profiles), batch_size):
            batch = profiles[i:min(i+batch_size, len(profiles))]
            logger.info(f"處理批次 {i//batch_size + 1}/{(len(profiles)-1)//batch_size + 1}，共 {len(batch)} 個配置檔案")
            
            for profile in batch:
                await amazon_ads_service.save_connection(user_id, profile, refresh_token, main_account_id)
                total_saved += 1
            
            logger.info(f"已保存 {total_saved}/{len(profiles)} 個配置檔案")
        
        logger.info(f"連接保存完成，共保存 {total_saved} 個配置檔案")
        
        # 重定向回前端
        frontend_url = f"{settings.FRONTEND_URL}/connections?status=success"
        logger.info(f"授權流程完成，重定向到: {frontend_url}")
        return RedirectResponse(url=frontend_url)
    
    except Exception as e:
        # 處理錯誤
        error_detail = str(e) if str(e) else "Unknown error occurred"
        logger.error(f"授權處理過程中發生錯誤: {repr(e)}")
        logger.error(f"錯誤詳情: {traceback.format_exc()}")
        frontend_url = f"{settings.FRONTEND_URL}/connections?status=error&message={error_detail}"
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
                                "amazon_account_name": "amazon_user",
                                "is_active": False,
                                "timezone": "America/Los_Angeles",
                                "main_account_id": 1,
                                "main_account_name": "John Doe",
                                "main_account_email": "john.doe@example.com"
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
            amazon_account_name=conn.amazon_account_name,
            is_active=conn.is_active,
            timezone="",  # 可選字段
            main_account_id=conn.main_account_id,
            main_account_name=conn.main_account_name,
            main_account_email=conn.main_account_email
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
