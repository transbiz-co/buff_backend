from fastapi import APIRouter, HTTPException, Depends, Query, Path
from typing import Dict, Any, List, Optional
from datetime import datetime, timedelta
import logging
import traceback

from ...services.amazon_ads import amazon_ads_service, supabase
from ...core.security import decrypt_token

# 設定全局日誌
logger = logging.getLogger(__name__)

router = APIRouter(
    prefix="/reports", 
    tags=["amazon-ads-reports"],
    responses={404: {"description": "未找到"}}
)

@router.post(
    "/sync/user/{user_id}",
    summary="同步用戶的所有廣告活動報告",
    description="""
    同步指定用戶的所有 Amazon Ads 廣告活動報告。
    
    此 API 會從用戶的所有配置檔案中獲取廣告活動信息，為每個廣告活動創建報告請求。
    可以選擇指定廣告產品類型 (SPONSORED_PRODUCTS, SPONSORED_BRANDS, SPONSORED_DISPLAY)。
    如果不指定廣告產品類型，將生成所有三種類型的報告。
    支持設置報告的時間範圍。
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
                        "created_reports": 120,
                        "message": "Successfully created 120 reports from 5 of 5 profiles",
                        "details": {
                            "SPONSORED_PRODUCTS": {
                                "success": True,
                                "created_reports": 50
                            },
                            "SPONSORED_BRANDS": {
                                "success": True,
                                "created_reports": 40
                            },
                            "SPONSORED_DISPLAY": {
                                "success": True,
                                "created_reports": 30
                            }
                        }
                    }
                }
            }
        },
        400: {"description": "同步失敗"},
        404: {"description": "未找到用戶或連接檔案"}
    }
)
async def sync_user_reports(
    user_id: str = Path(..., description="用戶 ID"),
    ad_product: Optional[str] = Query(None, description="廣告產品類型 (SPONSORED_PRODUCTS, SPONSORED_BRANDS, SPONSORED_DISPLAY)，不指定則生成所有類型報告"),
    start_date: Optional[str] = Query(None, description="報告開始日期 (YYYY-MM-DD)，默認為前7天"),
    end_date: Optional[str] = Query(None, description="報告結束日期 (YYYY-MM-DD)，默認為前1天")
):
    """
    同步指定用戶的所有廣告活動報告
    
    參數:
        user_id: 用戶 ID
        ad_product: 廣告產品類型，不指定則生成所有類型報告
        start_date: 報告開始日期
        end_date: 報告結束日期
        
    返回:
        同步操作結果，包括創建的報告數量和各類型的詳細信息
    """
    logger.info(f"開始同步用戶 {user_id} 的報告")
    
    # 確定日期範圍
    if not start_date:
        start_date = (datetime.now() - timedelta(days=7)).strftime("%Y-%m-%d")
    if not end_date:
        end_date = (datetime.now() - timedelta(days=1)).strftime("%Y-%m-%d")
    
    logger.info(f"報告時間範圍: {start_date} 至 {end_date}")
    
    # 如果未指定廣告產品類型，則處理所有類型
    ad_product_types = ["SPONSORED_PRODUCTS", "SPONSORED_BRANDS", "SPONSORED_DISPLAY"]
    
    if ad_product:
        # 如果指定了特定類型，只處理該類型
        if ad_product in ad_product_types:
            ad_product_types = [ad_product]
        else:
            logger.warning(f"無效的廣告產品類型: {ad_product}")
            raise HTTPException(status_code=400, detail=f"Invalid ad_product: {ad_product}")
    
    # 用於存儲所有類型的結果
    combined_result = {
        "success": False,
        "total_profiles": 0,
        "processed_profiles": 0,
        "created_reports": 0,
        "details": {},
        "failed_profiles": []
    }
    
    at_least_one_success = False
    
    # 處理每種廣告產品類型
    for product_type in ad_product_types:
        logger.info(f"處理廣告產品類型: {product_type}")
        
        try:
            # 調用服務方法批量創建報告
            result = await amazon_ads_service.bulk_create_reports(
                user_id=user_id,
                ad_product=product_type,
                start_date=start_date,
                end_date=end_date
            )
            
            # 記錄詳細結果
            combined_result["details"][product_type] = {
                "success": result.get("success", False),
                "created_reports": result.get("created_reports", 0),
                "message": result.get("message", "")
            }
            
            # 如果至少有一種類型成功，則整體結果為成功
            if result.get("success", False):
                at_least_one_success = True
                
            # 更新合計數據
            combined_result["total_profiles"] = max(combined_result["total_profiles"], result.get("total_profiles", 0))
            combined_result["processed_profiles"] += result.get("processed_profiles", 0)
            combined_result["created_reports"] += result.get("created_reports", 0)
            
            # 合併失敗的配置檔案
            if "failed_profiles" in result:
                combined_result["failed_profiles"].extend(result["failed_profiles"])
            
        except Exception as e:
            logger.error(f"處理 {product_type} 時出錯: {str(e)}")
            logger.error(traceback.format_exc())
            
            # 記錄錯誤
            combined_result["details"][product_type] = {
                "success": False,
                "created_reports": 0,
                "message": f"Error: {str(e)}"
            }
    
    # 設置整體結果的成功狀態
    combined_result["success"] = at_least_one_success
    
    # 創建摘要信息
    if combined_result["created_reports"] > 0:
        combined_result["message"] = (
            f"Successfully created {combined_result['created_reports']} reports " 
            f"from {combined_result['processed_profiles']} profile operations " 
            f"across {len([t for t, r in combined_result['details'].items() if r['success']])} ad types"
        )
    else:
        combined_result["message"] = "No reports were created"
    
    # 檢查是否有錯誤條件
    if not at_least_one_success:
        logger.warning(f"同步用戶 {user_id} 的報告失敗: {combined_result['message']}")
        
        # 如果找不到連接，返回404
        for product_type, result in combined_result["details"].items():
            if "No Amazon Ads connections found" in result.get("message", ""):
                raise HTTPException(status_code=404, detail="No Amazon Ads connections found for this user")
        
        # 其他錯誤返回400
        raise HTTPException(status_code=400, detail=combined_result["message"])
    
    return combined_result

@router.post(
    "/sync/profile/{profile_id}",
    summary="同步特定配置檔案的廣告活動報告",
    description="""
    同步特定 Amazon Ads 配置檔案的所有廣告活動報告。
    
    此 API 會從指定的配置檔案中獲取廣告活動信息，為每個廣告活動創建報告請求。
    可以選擇指定廣告產品類型 (SPONSORED_PRODUCTS, SPONSORED_BRANDS, SPONSORED_DISPLAY)。
    如果不指定廣告產品類型，將生成所有三種類型的報告。
    支持設置報告的時間範圍。
    """,
    responses={
        200: {
            "description": "同步操作結果",
            "content": {
                "application/json": {
                    "example": {
                        "success": True,
                        "profile_id": "3392083858428582",
                        "created_reports": 3,
                        "message": "Successfully created 3 reports across 3 ad types",
                        "details": {
                            "SPONSORED_PRODUCTS": {
                                "success": True,
                                "created_reports": 1
                            },
                            "SPONSORED_BRANDS": {
                                "success": True,
                                "created_reports": 1
                            },
                            "SPONSORED_DISPLAY": {
                                "success": True,
                                "created_reports": 1
                            }
                        }
                    }
                }
            }
        },
        400: {"description": "同步失敗"},
        404: {"description": "未找到配置檔案或連接"}
    }
)
async def sync_profile_reports(
    profile_id: str = Path(..., description="Amazon Ads 配置檔案 ID"),
    ad_product: Optional[str] = Query(None, description="廣告產品類型 (SPONSORED_PRODUCTS, SPONSORED_BRANDS, SPONSORED_DISPLAY)，不指定則生成所有類型報告"),
    start_date: Optional[str] = Query(None, description="報告開始日期 (YYYY-MM-DD)，默認為前7天"),
    end_date: Optional[str] = Query(None, description="報告結束日期 (YYYY-MM-DD)，默認為前1天")
):
    """
    同步特定配置檔案的所有廣告活動報告
    
    參數:
        profile_id: Amazon Ads 配置檔案 ID
        ad_product: 廣告產品類型，不指定則生成所有類型報告
        start_date: 報告開始日期
        end_date: 報告結束日期
        
    返回:
        同步操作結果，包括創建的報告數量和各類型的詳細信息
    """
    logger.info(f"開始同步配置檔案 {profile_id} 的報告")
    
    # 確定日期範圍
    if not start_date:
        start_date = (datetime.now() - timedelta(days=7)).strftime("%Y-%m-%d")
    if not end_date:
        end_date = (datetime.now() - timedelta(days=1)).strftime("%Y-%m-%d")
    
    logger.info(f"報告時間範圍: {start_date} 至 {end_date}")
    
    # 如果未指定廣告產品類型，則處理所有類型
    ad_product_types = ["SPONSORED_PRODUCTS", "SPONSORED_BRANDS", "SPONSORED_DISPLAY"]
    
    if ad_product:
        # 如果指定了特定類型，只處理該類型
        if ad_product in ad_product_types:
            ad_product_types = [ad_product]
        else:
            logger.warning(f"無效的廣告產品類型: {ad_product}")
            raise HTTPException(status_code=400, detail=f"Invalid ad_product: {ad_product}")
    
    # 用於存儲所有類型的結果
    combined_result = {
        "success": False,
        "profile_id": profile_id,
        "created_reports": 0,
        "details": {}
    }
    
    at_least_one_success = False
    
    # 處理每種廣告產品類型
    for product_type in ad_product_types:
        logger.info(f"處理廣告產品類型: {product_type}")
        
        try:
            # 調用服務方法創建報告
            result = await amazon_ads_service.create_profile_reports(
                profile_id=profile_id,
                ad_product=product_type,
                start_date=start_date,
                end_date=end_date
            )
            
            # 記錄詳細結果
            combined_result["details"][product_type] = {
                "success": result.get("success", False),
                "created_reports": result.get("created_reports", 0),
                "message": result.get("message", "")
            }
            
            # 如果至少有一種類型成功，則整體結果為成功
            if result.get("success", False):
                at_least_one_success = True
                
            # 更新合計數據
            combined_result["created_reports"] += result.get("created_reports", 0)
            
        except Exception as e:
            logger.error(f"處理 {product_type} 時出錯: {str(e)}")
            logger.error(traceback.format_exc())
            
            # 記錄錯誤
            combined_result["details"][product_type] = {
                "success": False,
                "created_reports": 0,
                "message": f"Error: {str(e)}"
            }
    
    # 設置整體結果的成功狀態
    combined_result["success"] = at_least_one_success
    
    # 創建摘要信息
    if combined_result["created_reports"] > 0:
        combined_result["message"] = (
            f"Successfully created {combined_result['created_reports']} reports " 
            f"across {len([t for t, r in combined_result['details'].items() if r['success']])} ad types"
        )
    else:
        combined_result["message"] = "No reports were created"
    
    # 檢查是否有錯誤條件
    if not at_least_one_success:
        logger.warning(f"同步配置檔案 {profile_id} 的報告失敗: {combined_result['message']}")
        
        # 如果找不到連接，返回404
        for product_type, result in combined_result["details"].items():
            if "Connection not found" in result.get("message", ""):
                raise HTTPException(status_code=404, detail="Connection not found for this profile")
        
        # 其他錯誤返回400
        raise HTTPException(status_code=400, detail=combined_result["message"])
    
    return combined_result
