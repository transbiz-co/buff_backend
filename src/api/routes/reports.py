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

@router.post(
    "/check",
    summary="檢查報告狀態並處理",
    description="""
    檢查報告狀態並處理報告內容。
    
    - 如果提供了 report_id，則只檢查並處理該特定報告
    - 如果未提供 report_id，則處理所有待處理的報告（狀態為 "COMPLETED" 但尚未下載的報告）
    
    處理後的報告將被上傳到 Supabase 存儲桶。
    可以選擇性地指定用戶 ID 或配置檔案 ID 來限制檢查範圍。
    
    注意: 這是唯一的報告檢查 API，可以處理單個報告和批量報告。
    """,
    responses={
        200: {
            "description": "報告處理結果",
            "content": {
                "application/json": {
                    "example": {
                        "total_reports": 5,
                        "processed_reports": 3,
                        "failed_reports": 2,
                        "details": [
                            {
                                "report_id": "12345678-1234-1234-1234-123456789012",
                                "status": "COMPLETED",
                                "download_status": "DOWNLOADED",
                                "message": "報告已成功下載和處理"
                            }
                        ]
                    }
                }
            }
        },
        404: {"description": "未找到報告"},
        500: {"description": "處理報告時出錯"}
    }
)
async def check_and_process_reports(
    report_id: Optional[str] = Query(None, description="報告 ID，可選，若提供則只處理該特定報告"),
    user_id: Optional[str] = Query(None, description="用戶 ID，可選，用於只處理特定用戶的報告"),
    profile_id: Optional[str] = Query(None, description="Amazon Ads 配置檔案 ID，可選，用於只處理特定配置檔案的報告"),
    limit: int = Query(20, description="處理的最大報告數量，默認為 20，僅在批量處理時有效")
):
    """
    檢查報告狀態並處理
    
    參數:
        report_id: 報告 ID，可選，若提供則只處理該特定報告
        user_id: 用戶 ID，可選
        profile_id: Amazon Ads 配置檔案 ID，可選
        limit: 處理的最大報告數量，僅在批量處理時有效
        
    返回:
        報告處理結果
    """
    # 如果提供了特定的 report_id，則只處理該報告
    if report_id:
        logger.info(f"檢查特定報告: {report_id}")
        try:
            # 調用服務方法檢查並下載報告
            result = await amazon_ads_service.check_and_download_report(report_id)
            
            # 將單個報告結果包裝為一致的格式
            return {
                "total_reports": 1,
                "processed_reports": 1 if result.get('download_status') == "DOWNLOADED" else 0,
                "failed_reports": 0 if result.get('download_status') == "DOWNLOADED" else 1,
                "details": [result]
            }
        except ValueError as e:
            raise HTTPException(status_code=404, detail=str(e))
        except Exception as e:
            logger.error(f"檢查報告 {report_id} 時出錯: {str(e)}")
            logger.error(traceback.format_exc())
            raise HTTPException(status_code=500, detail=f"處理報告時出錯: {str(e)}")
    
    # 否則處理所有待處理的報告
    else:
        logger.info("開始檢查和處理待處理的報告")
        
        try:
            # 構建查詢
            query = supabase.table('amazon_ads_reports').select('*')
            
            # 僅選擇已完成但未下載的報告
            query = query.eq('status', 'COMPLETED').eq('download_status', 'PENDING')
            
            # 如果指定了用戶 ID，則添加過濾條件
            if user_id:
                query = query.eq('user_id', user_id)
                
            # 如果指定了配置檔案 ID，則添加過濾條件
            if profile_id:
                query = query.eq('profile_id', profile_id)
                
            # 執行查詢
            reports_result = query.limit(limit).execute()
            pending_reports = reports_result.data
            
            # 處理結果
            result = {
                "total_reports": len(pending_reports),
                "processed_reports": 0,
                "failed_reports": 0,
                "details": []
            }
            
            # 處理每個報告
            for report in pending_reports:
                try:
                    report_result = await amazon_ads_service.check_and_download_report(report['report_id'])
                    
                    # 更新計數
                    if report_result.get('download_status') == "DOWNLOADED":
                        result["processed_reports"] += 1
                    else:
                        result["failed_reports"] += 1
                        
                    # 添加詳細信息
                    result["details"].append(report_result)
                    
                except Exception as e:
                    logger.error(f"處理報告 {report['report_id']} 時出錯: {str(e)}")
                    logger.error(traceback.format_exc())
                    
                    # 更新計數
                    result["failed_reports"] += 1
                    
                    # 添加詳細信息
                    result["details"].append({
                        "report_id": report['report_id'],
                        "status": report.get('status'),
                        "download_status": "FAILED",
                        "message": f"處理出錯: {str(e)}"
                    })
            
            # 返回結果
            return result
            
        except Exception as e:
            logger.error(f"批量處理報告時出錯: {str(e)}")
            logger.error(traceback.format_exc())
            raise HTTPException(status_code=500, detail=f"批量處理報告時出錯: {str(e)}")

@router.post(
    "/sync/amazon/advertising/reporting/campaign/reports",
    summary="申請廣告活動報告",
    description="""
    申請 Amazon 廣告活動報告。
    
    此 API 可以根據不同參數組合申請廣告報告：
    - 若提供 profile_id，則使用該 profile 申請指定的 ad_product campaign report
    - 若提供 user_id，則透過 user_id 找到 public.amazon_ads_connections 底下所有 profile，逐個申請指定的 ad_product campaign report
    - 若未提供 ad_product 參數，則 SP, SB, SD report 都要申請
    
    報告資料將存入 public.amazon_ads_reports 表。
    """,
    responses={
        200: {
            "description": "申請操作結果",
            "content": {
                "application/json": {
                    "example": {
                        "success": True,
                        "total_profiles": 5,
                        "processed_profiles": 5,
                        "created_reports": 120,
                        "message": "Successfully created 120 reports from 5 profiles",
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
        400: {"description": "申請失敗"},
        404: {"description": "未找到用戶或連接檔案"}
    }
)
async def sync_amazon_advertising_campaign_reports(
    start_date: str = Query(..., description="報告開始日期 (YYYY-MM-DD)"),
    end_date: str = Query(..., description="報告結束日期 (YYYY-MM-DD)"),
    user_id: Optional[str] = Query(None, description="用戶 ID，若提供則為該用戶的所有 profile 申請報告"),
    profile_id: Optional[str] = Query(None, description="Amazon Ads 配置檔案 ID，若提供則為該 profile 申請報告"),
    ad_product: Optional[str] = Query(None, description="廣告產品類型 (SPONSORED_PRODUCTS, SPONSORED_BRANDS, SPONSORED_DISPLAY)，不指定則生成所有類型報告")
):
    """
    申請 Amazon 廣告活動報告
    
    參數:
        start_date: 報告開始日期 (YYYY-MM-DD)
        end_date: 報告結束日期 (YYYY-MM-DD)
        user_id: 用戶 ID，選填
        profile_id: Amazon Ads 配置檔案 ID，選填
        ad_product: 廣告產品類型，不指定則生成所有類型報告
        
    返回:
        申請操作結果，包括創建的報告數量和各類型的詳細信息
    """
    logger.info(f"開始申請廣告活動報告 start_date={start_date}, end_date={end_date}, user_id={user_id}, profile_id={profile_id}, ad_product={ad_product}")
    
    
    try:
        datetime.strptime(start_date, "%Y-%m-%d")
        datetime.strptime(end_date, "%Y-%m-%d")
    except ValueError as e:
        logger.warning(f"日期格式錯誤: {str(e)}")
        raise HTTPException(status_code=400, detail=f"日期格式錯誤: {str(e)}")
    
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
        "processed_profiles": 0,
        "created_reports": 0,
        "details": {},
        "failed_profiles": []
    }
    
    at_least_one_success = False
    
    if not user_id and not profile_id:
        logger.info("未提供 user_id 或 profile_id，使用所有 profiles")
        
        all_connections = await amazon_ads_service.get_all_connections()
        
        if not all_connections:
            logger.warning("未找到任何 Amazon Ads 連接")
            raise HTTPException(status_code=404, detail="No Amazon Ads connections found")
        
        logger.info(f"找到 {len(all_connections)} 個 Amazon Ads 連接")
        
        all_profiles_stats = {
            "total_profiles": len(all_connections),
            "processed_profiles": 0
        }
        
        # 處理每種廣告產品類型
        for product_type in ad_product_types:
            logger.info(f"處理廣告產品類型: {product_type}")
            
            product_result = {
                "success": False,
                "created_reports": 0,
                "processed_profiles": 0,
                "failed_profiles": []
            }
            
            for connection in all_connections:
                profile_id = connection.profile_id
                logger.info(f"處理 profile_id={profile_id}")
                
                try:
                    # 調用服務方法創建報告
                    result = await amazon_ads_service.create_profile_reports(
                        profile_id=profile_id,
                        ad_product=product_type,
                        start_date=start_date,
                        end_date=end_date
                    )
                    
                    if result.get("success", False):
                        product_result["processed_profiles"] += 1
                        product_result["created_reports"] += result.get("created_reports", 0)
                    else:
                        product_result["failed_profiles"].append({
                            "profile_id": profile_id,
                            "message": result.get("message", "Unknown error")
                        })
                    
                except Exception as e:
                    logger.error(f"處理 profile_id={profile_id}, product_type={product_type} 時出錯: {str(e)}")
                    logger.error(traceback.format_exc())
                    
                    product_result["failed_profiles"].append({
                        "profile_id": profile_id,
                        "message": str(e)
                    })
            
            product_result["success"] = product_result["processed_profiles"] > 0
            
            # 如果至少有一個 profile 成功，則整體結果為成功
            if product_result["success"]:
                at_least_one_success = True
            
            # 更新合計數據
            all_profiles_stats["processed_profiles"] += product_result["processed_profiles"]
            combined_result["created_reports"] += product_result["created_reports"]
            combined_result["failed_profiles"].extend(product_result["failed_profiles"])
            
            # 記錄詳細結果
            combined_result["details"][product_type] = {
                "success": product_result["success"],
                "created_reports": product_result["created_reports"],
                "message": f"Processed {product_result['processed_profiles']} of {len(all_connections)} profiles"
            }
        
        combined_result["total_profiles"] = all_profiles_stats["total_profiles"]
        combined_result["processed_profiles"] = all_profiles_stats["processed_profiles"]
    
    elif profile_id:
        logger.info(f"使用 profile_id={profile_id} 申請報告")
        
        combined_result["profile_id"] = profile_id
        
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
                combined_result["processed_profiles"] += (1 if result.get("success", False) else 0)
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
    
    if user_id:
        logger.info(f"使用 user_id={user_id} 申請報告")
        
        combined_result["user_id"] = user_id
        user_result_stats = {
            "total_profiles": 0,
            "processed_profiles": 0
        }
        
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
                user_result_stats["total_profiles"] = max(user_result_stats["total_profiles"], result.get("total_profiles", 0))
                user_result_stats["processed_profiles"] += result.get("processed_profiles", 0)
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
        
        combined_result["total_profiles"] = user_result_stats["total_profiles"]
        combined_result["processed_profiles"] += user_result_stats["processed_profiles"]
    
    # 設置整體結果的成功狀態
    combined_result["success"] = at_least_one_success
    
    # 創建摘要信息
    if combined_result["created_reports"] > 0:
        if profile_id and user_id:
            combined_result["message"] = (
                f"Successfully created {combined_result['created_reports']} reports "
                f"from {combined_result['processed_profiles']} profiles "
                f"across {len([t for t, r in combined_result['details'].items() if r['success']])} ad types"
            )
        elif profile_id:
            combined_result["message"] = (
                f"Successfully created {combined_result['created_reports']} reports "
                f"across {len([t for t, r in combined_result['details'].items() if r['success']])} ad types"
            )
        elif user_id:
            combined_result["message"] = (
                f"Successfully created {combined_result['created_reports']} reports "
                f"from {combined_result['processed_profiles']} of {combined_result['total_profiles']} profiles "
                f"across {len([t for t, r in combined_result['details'].items() if r['success']])} ad types"
            )
        else:  # 所有 profiles
            combined_result["message"] = (
                f"Successfully created {combined_result['created_reports']} reports "
                f"from {combined_result['processed_profiles']} of {combined_result['total_profiles']} profiles "
                f"across {len([t for t, r in combined_result['details'].items() if r['success']])} ad types"
            )
    else:
        combined_result["message"] = "No reports were created"
    
    # 檢查是否有錯誤條件
    if not at_least_one_success:
        logger.warning(f"申請報告失敗: {combined_result['message']}")
        
        if profile_id:
            for product_type, result in combined_result["details"].items():
                if "Connection not found" in result.get("message", ""):
                    raise HTTPException(status_code=404, detail="Connection not found for this profile")
        else:  # user_id
            for product_type, result in combined_result["details"].items():
                if "No Amazon Ads connections found" in result.get("message", ""):
                    raise HTTPException(status_code=404, detail="No Amazon Ads connections found for this user")
        
        # 其他錯誤返回400
        raise HTTPException(status_code=400, detail=combined_result["message"])
    
    return combined_result
