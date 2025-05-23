from fastapi import APIRouter, HTTPException, Depends, Query, Path
from typing import Dict, Any, List, Optional
from datetime import datetime, timedelta
import logging
import traceback

from ...services.amazon_ads import amazon_ads_service, supabase
from ...core.security import decrypt_token
from ...models.enums import ReportStatus, DownloadStatus, ProcessedStatus, AdProduct
from ...models.enums import ReportStatus, DownloadStatus, ProcessedStatus, AdProduct
from ...services.report_processor import ReportProcessor

# 設定全局日誌
logger = logging.getLogger(__name__)

router = APIRouter(
    prefix="/reports", 
    tags=["amazon-ads-reports"],
    responses={404: {"description": "未找到"}}
)

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
    
    report_processor = ReportProcessor(amazon_ads_service)
    
    try:
        # 統一的查詢邏輯
        reports_to_process = await _get_reports_to_process(
            report_id=report_id,
            user_id=user_id,
            profile_id=profile_id,
            limit=limit
        )
        
        # 初始化結果結構
        result = {
            "total_reports": len(reports_to_process),
            "processed_reports": 0,
            "failed_reports": 0,
            "details": []
        }
        
        # 處理每個報告
        for report in reports_to_process:
            report_result = await _process_single_report(
                report_processor, 
                report['report_id'],
                report.get('status')
            )
            
            # 更新計數和詳細信息
            _update_result_counts(result, report_result)
            result["details"].append(report_result)
        
        return result
        
    except ValueError as e:
        # 特定報告不存在的情況
        raise HTTPException(status_code=404, detail=str(e))
    except Exception as e:
        # 其他錯誤
        logger.error(f"處理報告時出錯: {str(e)}")
        logger.error(traceback.format_exc())
        raise HTTPException(status_code=500, detail=f"處理報告時出錯: {str(e)}")


async def _get_reports_to_process(
    report_id: Optional[str],
    user_id: Optional[str],
    profile_id: Optional[str],
    limit: int
) -> list:
    """
    獲取需要處理的報告列表
    
    參數:
        report_id: 特定報告 ID (最高優先級)
        user_id: 用戶 ID (最低優先級)
        profile_id: 配置檔案 ID (中間優先級)
        limit: 最大數量限制
        
    返回:
        報告列表
    
    優先級順序:
    1. 如果提供 report_id，僅查詢該特定報告
    2. 如果無 report_id 但有 profile_id，僅根據 profile_id 查詢
    3. 如果無 report_id 和 profile_id 但有 user_id，僅根據 user_id 查詢
    4. 如果都沒有提供，則返回所有待處理 (PENDING) 的報告
    
    錯誤處理:
    - 當指定條件查詢結果為空時，會拋出 ValueError 錯誤
    """
    # 建立基本查詢
    query = supabase.table('amazon_ads_reports').select('*')
    
    # 根據優先級順序添加過濾條件
    if report_id:
        logger.info(f"根據報告 ID 查詢: {report_id}")
        query = query.eq('report_id', report_id)
        result = query.execute()
        if not result.data:
            raise ValueError(f"報告 {report_id} 不存在")
            
        return result.data
        
    elif profile_id:
        logger.info(f"根據配置檔案 ID 查詢: {profile_id}")
        query = query.eq('profile_id', profile_id)
        result = query.execute()
        if not result.data:
            raise ValueError(f"配置檔案 {profile_id} 沒有相關報告")
            
        return result.data
        
    elif user_id:
        logger.info(f"根據用戶 ID 查詢: {user_id}")
        query = query.eq('user_id', user_id)
        result = query.execute()
        if not result.data:
            raise ValueError(f"用戶 {user_id} 沒有相關報告")
            
        return result.data
        
    else:
        # 如果沒有提供任何特定條件，則只查詢待處理的報告
        logger.info("查詢所有待處理的報告")
        query = query.eq('status', ReportStatus.PENDING.value)
    
    # 添加限制並執行查詢
    result = query.limit(limit).execute()
    return result.data


async def _process_single_report(
    report_processor: ReportProcessor, 
    report_id: str,
    current_status: Optional[str] = None
) -> dict:
    """
    處理單個報告並返回統一格式的結果
    
    參數:
        report_processor: 報告處理器實例
        report_id: 報告 ID
        current_status: 報告當前狀態
        
    返回:
        處理結果字典
    """
    try:
        report_result = await report_processor.process_report(report_id)
        return report_result
        
    except Exception as e:
        logger.error(f"處理報告 {report_id} 時出錯: {str(e)}")
        logger.error(traceback.format_exc())
        
        # 返回錯誤結果
        return {
            "report_id": report_id,
            "status": current_status,
            "download_status": DownloadStatus.FAILED.value,
            "message": f"處理出錯: {str(e)}"
        }


def _update_result_counts(result: dict, report_result: dict) -> None:
    """
    根據報告處理結果更新計數
    
    參數:
        result: 總結果字典
        report_result: 單個報告的處理結果
    """
    if report_result.get('download_status') == DownloadStatus.COMPLETED.value:
        result["processed_reports"] += 1
    else:
        result["failed_reports"] += 1

@router.post(
    "/campaigns/",
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
        profile_id: Amazon Ads 配置檔案 ID，選填（優先級高於 user_id）
        ad_product: 廣告產品類型，不指定則生成所有類型報告
        
    返回:
        申請操作結果，包括創建的報告數量和各類型的詳細信息
    """
    logger.info(f"開始申請廣告活動報告 start_date={start_date}, end_date={end_date}, user_id={user_id}, profile_id={profile_id}, ad_product={ad_product}")
    
    # 創建 ReportProcessor 實例
    report_processor = ReportProcessor(amazon_ads_service)
    
    try:
        # 驗證日期格式
        datetime.strptime(start_date, "%Y-%m-%d")
        datetime.strptime(end_date, "%Y-%m-%d")
    except ValueError as e:
        logger.warning(f"日期格式錯誤: {str(e)}")
        raise HTTPException(status_code=400, detail=f"日期格式錯誤: {str(e)}")
    
    try:
        # 1. 獲取目標 profiles（優先級：profile_id > user_id > 所有 profiles）
        profiles = await report_processor.get_target_profiles(user_id, profile_id)
        
        if not profiles:
            error_msg = "Connection not found for this profile" if profile_id else "No Amazon Ads connections found for this user" if user_id else "No Amazon Ads connections found"
            status_code = 404
            raise HTTPException(status_code=status_code, detail=error_msg)
        
        # 2. 獲取要處理的廣告產品類型
        try:
            ad_products = report_processor.get_ad_products(ad_product)
        except ValueError as e:
            logger.warning(f"無效的廣告產品類型: {str(e)}")
            raise HTTPException(status_code=400, detail=str(e))
        
        # 3. 創建報告
        result = await report_processor.create_reports_for_profiles(
            profiles=profiles,
            ad_products=ad_products,
            start_date=start_date,
            end_date=end_date
        )
        
        # 4. 檢查結果並返回適當的響應
        if not result.get("success", False):
            logger.warning(f"申請報告失敗: {result.get('message', 'Unknown error')}")
            raise HTTPException(status_code=400, detail=result.get("message", "Failed to create reports"))
        
        return result
        
    except HTTPException:
        # 重新拋出 HTTP 異常
        raise
    except Exception as e:
        logger.error(f"申請廣告活動報告時出錯: {str(e)}")
        logger.error(traceback.format_exc())
        raise HTTPException(status_code=500, detail=f"Internal server error: {str(e)}")
