from fastapi import APIRouter, HTTPException, Depends, Query, Path, Request
from typing import Dict, Any, List, Optional
import logging
import traceback
from datetime import datetime
import asyncio

from ...services.amazon_ads import amazon_ads_service, supabase
from ...core.security import decrypt_token
from ...models.connections import AmazonAdsConnection

# 設定全局日誌
logger = logging.getLogger(__name__)

router = APIRouter(
    prefix="/metadatas", 
    tags=["amazon-ads-metadatas"],
    responses={404: {"description": "未找到"}}
)

# 同步用戶的所有廣告活動
@router.post(
    "/campaigns",
    summary="同步 Amazon Ads Campaigns Metadata",
    description="""
    同步指定用戶的所有 Amazon Ads Campaigns Metadata
    
    此 API 會獲取用戶所有 Amazon profiles，並取得每個 profile 的 SP、SB、SD 三種 campaign metadata，
    並將結果存儲到資料庫中。此操作可能需要較長時間，取決於用戶的廣告活動數量。
    """,
    responses={
        200: {
            "description": "同步操作結果",
            "content": {
                "application/json": {
                    "example": {
                        "success": True,
                        "total_profiles": 5,
                        "processed_profiles": 5,
                        "total_campaigns": 120,
                        "campaigns_by_type": {
                            "SP": 80,
                            "SB": 30,
                            "SD": 10
                        },
                        "message": "Successfully synced all campaigns"
                    }
                }
            }
        },
        400: {"description": "同步失敗"},
        404: {"description": "未找到用戶或連接檔案"}
    }
)
async def sync_campaigns(
    user_id: str = Query(..., description="用戶 ID")
):
    """
    同步用戶的所有 Amazon Ads 廣告活動資料
    
    參數:
        user_id: 用戶 ID
        
    返回:
        同步操作結果，包括同步的廣告活動數量和類型統計
    """
    logger.info(f"開始同步用戶 {user_id} 的廣告活動")
    
    # 獲取用戶的所有連接檔案
    connections = await amazon_ads_service.get_user_connections(user_id)
    
    # 檢查是否找到連接檔案
    if not connections:
        logger.warning(f"未找到用戶 {user_id} 的連接檔案")
        raise HTTPException(status_code=404, detail="No Amazon Ads connections found for this user")
    
    logger.info(f"找到 {len(connections)} 個連接檔案")
    
    # 處理結果統計
    result_stats = {
        "success": True,
        "total_profiles": len(connections),
        "processed_profiles": 0,
        "total_campaigns": 0,
        "campaigns_by_type": {
            "SP": 0,
            "SB": 0,
            "SD": 0
        },
        "failed_profiles": []
    }
    
    # 對每個連接檔案同步廣告活動
    for connection in connections:
        try:
            profile_id = connection.profile_id
            logger.info(f"處理連接檔案: {profile_id} ({connection.account_name})")
            
            # 解密刷新令牌
            refresh_token = decrypt_token(connection.refresh_token)
            
            # 刷新訪問令牌
            try:
                token_response = await amazon_ads_service.refresh_access_token(refresh_token)
                access_token = token_response.get("access_token")
                
                if not access_token:
                    logger.error(f"無法獲取訪問令牌，跳過連接檔案 {profile_id}")
                    result_stats["failed_profiles"].append({
                        "profile_id": profile_id,
                        "error": "Failed to get access token"
                    })
                    continue
                    
                logger.info(f"成功獲取訪問令牌: {access_token[:10]}...（已截斷）")
            except Exception as e:
                logger.error(f"刷新訪問令牌失敗: {str(e)}")
                result_stats["failed_profiles"].append({
                    "profile_id": profile_id,
                    "error": f"Failed to refresh token: {str(e)}"
                })
                continue
            
            # 同步三種廣告活動類型
            profile_campaigns = {}
            
            # 1. 同步 Sponsored Products (SP) 廣告活動
            try:
                sp_campaigns = await sync_sp_campaigns(profile_id, access_token)
                profile_campaigns["SP"] = sp_campaigns
                result_stats["campaigns_by_type"]["SP"] += len(sp_campaigns)
                result_stats["total_campaigns"] += len(sp_campaigns)
                logger.info(f"已同步 {len(sp_campaigns)} 個 SP 廣告活動")
            except Exception as e:
                logger.error(f"同步 SP 廣告活動時出錯: {str(e)}")
                logger.error(traceback.format_exc())
            
            # 2. 同步 Sponsored Brands (SB) 廣告活動
            try:
                sb_campaigns = await sync_sb_campaigns(profile_id, access_token)
                profile_campaigns["SB"] = sb_campaigns
                result_stats["campaigns_by_type"]["SB"] += len(sb_campaigns)
                result_stats["total_campaigns"] += len(sb_campaigns)
                logger.info(f"已同步 {len(sb_campaigns)} 個 SB 廣告活動")
            except Exception as e:
                logger.error(f"同步 SB 廣告活動時出錯: {str(e)}")
                logger.error(traceback.format_exc())
            
            # 3. 同步 Sponsored Display (SD) 廣告活動
            try:
                sd_campaigns = await sync_sd_campaigns(profile_id, access_token)
                profile_campaigns["SD"] = sd_campaigns
                result_stats["campaigns_by_type"]["SD"] += len(sd_campaigns)
                result_stats["total_campaigns"] += len(sd_campaigns)
                logger.info(f"已同步 {len(sd_campaigns)} 個 SD 廣告活動")
            except Exception as e:
                logger.error(f"同步 SD 廣告活動時出錯: {str(e)}")
                logger.error(traceback.format_exc())
            
            # 更新處理成功的連接檔案計數
            result_stats["processed_profiles"] += 1
            
            # 更新最後同步時間
            try:
                if supabase:
                    supabase.table('amazon_ads_connections').update({
                        'updated_at': datetime.now().isoformat()
                    }).eq('profile_id', profile_id).execute()
            except Exception as e:
                logger.error(f"更新連接檔案同步時間時出錯: {str(e)}")
                
        except Exception as e:
            logger.error(f"處理連接檔案 {connection.profile_id} 時出錯: {str(e)}")
            logger.error(traceback.format_exc())
            result_stats["failed_profiles"].append({
                "profile_id": connection.profile_id,
                "error": str(e)
            })
    
    # 更新結果信息
    if result_stats["total_campaigns"] > 0:
        result_stats["message"] = f"Successfully synced {result_stats['total_campaigns']} campaigns from {result_stats['processed_profiles']} of {result_stats['total_profiles']} profiles"
    else:
        result_stats["success"] = False
        result_stats["message"] = "No campaigns were synced"
    
    # 如果有失敗的連接檔案，記錄在結果中
    if result_stats["failed_profiles"]:
        logger.warning(f"有 {len(result_stats['failed_profiles'])} 個連接檔案同步失敗")
    
    return result_stats

# 同步 Sponsored Products (SP) 廣告活動
async def sync_sp_campaigns(profile_id: str, access_token: str) -> List[Dict[str, Any]]:
    """
    同步特定連接檔案的 Sponsored Products 廣告活動
    
    參數:
        profile_id: Amazon Ads 連接檔案 ID
        access_token: 訪問令牌
        
    返回:
        List[Dict[str, Any]]: 同步的廣告活動列表
    """
    logger.info(f"開始同步 SP 廣告活動: profile_id={profile_id}")
    
    # 調用 SP API 獲取廣告活動列表
    endpoint = "https://advertising-api.amazon.com/sp/campaigns/list"
    headers = {
        "Authorization": f"Bearer {access_token}",
        "Amazon-Advertising-API-ClientId": amazon_ads_service.client_id,
        "Amazon-Advertising-API-Scope": profile_id,
        "Content-Type": "application/vnd.spCampaign.v3+json",
        "Accept": "application/vnd.spCampaign.v3+json"
    }
    
    # 使用空的請求體獲取所有廣告活動
    request_body = {}
    
    try:
        async with amazon_ads_service.httpx_client() as client:
            response = await client.post(endpoint, headers=headers, json=request_body)
            response.raise_for_status()
            response_data = response.json()
            
            campaigns = response_data.get("campaigns", [])
            logger.info(f"從 SP API 獲取到 {len(campaigns)} 個廣告活動")
            
            # 將廣告活動保存到資料庫
            if campaigns:
                await save_campaigns_to_db(profile_id, "SP", campaigns)
            
            return campaigns
    except Exception as e:
        logger.error(f"調用 SP API 時出錯: {str(e)}")
        logger.error(traceback.format_exc())
        raise

# 同步 Sponsored Brands (SB) 廣告活動
async def sync_sb_campaigns(profile_id: str, access_token: str) -> List[Dict[str, Any]]:
    """
    同步特定連接檔案的 Sponsored Brands 廣告活動
    
    參數:
        profile_id: Amazon Ads 連接檔案 ID
        access_token: 訪問令牌
        
    返回:
        List[Dict[str, Any]]: 同步的廣告活動列表
    """
    logger.info(f"開始同步 SB 廣告活動: profile_id={profile_id}")
    
    # 調用 SB API 獲取廣告活動列表
    endpoint = "https://advertising-api.amazon.com/sb/v4/campaigns/list"
    headers = {
        "Authorization": f"Bearer {access_token}",
        "Amazon-Advertising-API-ClientId": amazon_ads_service.client_id,
        "Amazon-Advertising-API-Scope": profile_id,
        "Accept": "application/vnd.sbcampaignresource.v4+json"
    }
    
    # 使用空的請求體獲取所有廣告活動
    request_body = {}
    
    try:
        async with amazon_ads_service.httpx_client() as client:
            response = await client.post(endpoint, headers=headers, json=request_body)
            response.raise_for_status()
            response_data = response.json()
            
            campaigns = response_data.get("campaigns", [])
            logger.info(f"從 SB API 獲取到 {len(campaigns)} 個廣告活動")
            
            # 將廣告活動保存到資料庫
            if campaigns:
                await save_campaigns_to_db(profile_id, "SB", campaigns)
            
            return campaigns
    except Exception as e:
        logger.error(f"調用 SB API 時出錯: {str(e)}")
        logger.error(traceback.format_exc())
        raise

# 同步 Sponsored Display (SD) 廣告活動
async def sync_sd_campaigns(profile_id: str, access_token: str) -> List[Dict[str, Any]]:
    """
    同步特定連接檔案的 Sponsored Display 廣告活動
    
    參數:
        profile_id: Amazon Ads 連接檔案 ID
        access_token: 訪問令牌
        
    返回:
        List[Dict[str, Any]]: 同步的廣告活動列表
    """
    logger.info(f"開始同步 SD 廣告活動: profile_id={profile_id}")
    
    # 調用 SD API 獲取廣告活動列表
    endpoint = "https://advertising-api.amazon.com/sd/campaigns"
    headers = {
        "Authorization": f"Bearer {access_token}",
        "Amazon-Advertising-API-ClientId": amazon_ads_service.client_id,
        "Amazon-Advertising-API-Scope": profile_id
    }
    
    try:
        async with amazon_ads_service.httpx_client() as client:
            response = await client.get(endpoint, headers=headers)
            response.raise_for_status()
            campaigns = response.json()
            
            logger.info(f"從 SD API 獲取到 {len(campaigns)} 個廣告活動")
            
            # 將廣告活動保存到資料庫
            if campaigns:
                await save_campaigns_to_db(profile_id, "SD", campaigns)
            
            return campaigns
    except Exception as e:
        logger.error(f"調用 SD API 時出錯: {str(e)}")
        logger.error(traceback.format_exc())
        raise

# 將廣告活動保存到資料庫
async def save_campaigns_to_db(profile_id: str, ad_type: str, campaigns: List[Dict[str, Any]]) -> int:
    """
    將廣告活動保存到資料庫
    
    參數:
        profile_id: Amazon Ads 連接檔案 ID
        ad_type: 廣告活動類型 (SP, SB, SD)
        campaigns: 要保存的廣告活動列表
        
    返回:
        int: 成功保存的廣告活動數量
    """
    logger.info(f"開始保存 {len(campaigns)} 個 {ad_type} 廣告活動到數據庫")
    
    if not supabase:
        logger.error("無法保存廣告活動: Supabase 客戶端不可用")
        return 0
    
    # 定義用於處理不同廣告類型的設置的排除欄位
    exclude_fields = {
        "SP": ["campaignId", "name", "state", "startDate", "portfolioId"],
        "SB": ["campaignId", "name", "state", "startDate", "portfolioId", "budget", "budgetType", "costType"],
        "SD": ["campaignId", "name", "state", "startDate", "portfolioId", "budget", "budgetType", "costType", 
               "budgettype", "costtype", "portfolioid"]  # 同時包含大小寫欄位名
    }
    
    # 欄位名稱映射 (用於處理 SD 的小寫欄位名稱)
    field_mapping = {
        "budgettype": "budgetType",
        "costtype": "costType",
        "portfolioid": "portfolioId"
    }
    
    saved_count = 0
    batch_size = 100
    
    for i in range(0, len(campaigns), batch_size):
        batch = campaigns[i:min(i+batch_size, len(campaigns))]
        batch_data = []
        
        for campaign in batch:
            # 標準化欄位名稱 (特別是 SD 的小寫欄位)
            if ad_type == "SD":
                for old_key, new_key in field_mapping.items():
                    if old_key in campaign and new_key not in campaign:
                        campaign[new_key] = campaign.pop(old_key)
            
            # 獲取 campaign ID
            campaign_id = campaign.get("campaignId")
            if not campaign_id:
                logger.warning(f"跳過缺少 campaignId 的廣告活動")
                continue
            
            # 標準化日期格式
            start_date = None
            raw_date = campaign.get("startDate")
            if raw_date:
                # SD 的日期格式為 YYYYMMDD，需轉換為 YYYY-MM-DD
                if ad_type == "SD" and len(raw_date) == 8:
                    start_date = f"{raw_date[:4]}-{raw_date[4:6]}-{raw_date[6:8]}"
                else:
                    start_date = raw_date
            
            # 標準化狀態為大寫
            state = campaign.get("state", "").upper() if campaign.get("state") else None
            
            # 獲取預算信息
            budget = None
            budget_type = None
            
            if ad_type == "SP":
                # SP 的預算是一個物件，包含 budget 和 budgetType
                budget_data = campaign.get("budget", {})
                budget = budget_data.get("budget")
                budget_type = budget_data.get("budgetType")
            else:  # SB 和 SD
                budget = campaign.get("budget")
                budget_type = campaign.get("budgetType")
            
            # 創建廣告類型特定設置
            settings = {"sp_settings": None, "sb_settings": None, "sd_settings": None}
            settings_key = f"{ad_type.lower()}_settings"
            
            # 複製所有欄位，然後移除標準欄位
            type_settings = campaign.copy()
            for field in exclude_fields.get(ad_type, []):
                type_settings.pop(field, None)
            
            settings[settings_key] = type_settings
            
            # 準備待保存的數據
            campaign_data = {
                "campaign_id": campaign_id,
                "ad_type": ad_type,
                "profile_id": profile_id,
                "name": campaign.get("name", ""),
                "state": state,
                "start_date": start_date,
                "budget": budget,
                "budget_type": budget_type,
                "cost_type": campaign.get("costType"),
                "portfolio_id": campaign.get("portfolioId"),
                **settings,  # 展開設置
                "sync_status": "SYNCED",
                "last_synced_at": datetime.now().isoformat()
            }
            
            batch_data.append(campaign_data)
        
        # 使用 UPSERT 批量保存廣告活動
        if batch_data:
            try:
                result = supabase.table('amazon_ads_campaigns').upsert(batch_data).execute()
                
                if result and result.data:
                    saved_count += len(result.data)
                    logger.info(f"成功保存 {len(result.data)} 個廣告活動")
                else:
                    logger.warning(f"批量保存廣告活動可能失敗, 響應: {result}")
            except Exception as e:
                logger.error(f"保存批次廣告活動時出錯: {str(e)}")
                logger.error(traceback.format_exc())
    
    logger.info(f"共保存 {saved_count} 個 {ad_type} 廣告活動")
    return saved_count 