import logging
import traceback
from datetime import datetime, timedelta
from typing import Dict, Any, List, Optional, Union
import json
import gzip
import io
import asyncio
import httpx

from ..models.enums import ReportStatus, DownloadStatus, ProcessedStatus, AdProduct
from .amazon_ads import supabase
from ..core.security import decrypt_token

logger = logging.getLogger(__name__)

class ReportProcessor:
    """
    報告處理器，負責檢查、下載和處理報告
    使用狀態機模式處理不同狀態的報告
    """
    
    def __init__(self, amazon_ads_service):
        self.amazon_ads_service = amazon_ads_service
    
    async def create_report(self, 
                          profile_id: str, 
                          access_token: str, 
                          ad_product: str,
                          start_date: Optional[str] = None, 
                          end_date: Optional[str] = None,
                          user_id: Optional[str] = None,
                          report_name: Optional[str] = None,
                          report_type_id: Optional[str] = None) -> Dict[str, Any]:
        """
        創建Amazon廣告報告請求
        
        Args:
            profile_id: Amazon Ads 配置檔案 ID
            access_token: 訪問令牌
            ad_product: 廣告產品類型 (SPONSORED_PRODUCTS, SPONSORED_BRANDS, SPONSORED_DISPLAY)
            start_date: 報告開始日期 (YYYY-MM-DD)，默認為前7天
            end_date: 報告結束日期 (YYYY-MM-DD)，默認為前1天
            user_id: 用戶ID，用於記錄
            report_name: 報告名稱，默認自動生成
            report_type_id: 報告類型ID，默認根據ad_product選擇
            
        Returns:
            Dict[str, Any]: 報告請求的響應
        """
        logger.info(f"開始創建 {ad_product} 報告，profile_id={profile_id}")
        
        # 確定報告類型ID
        if not report_type_id:
            if ad_product == "SPONSORED_PRODUCTS":
                report_type_id = "spCampaigns"
            elif ad_product == "SPONSORED_BRANDS":
                report_type_id = "sbCampaigns"
            elif ad_product == "SPONSORED_DISPLAY":
                report_type_id = "sdCampaigns"
            else:
                raise ValueError(f"不支援的廣告產品類型: {ad_product}")
        
        # 確定日期範圍
        today = datetime.now()
        if not start_date:
            # 默認前7天
            start_date = (today - timedelta(days=7)).strftime("%Y-%m-%d")
        if not end_date:
            # 默認前1天
            end_date = (today - timedelta(days=1)).strftime("%Y-%m-%d")
            
        # 確定報告名稱
        if not report_name:
            report_name = f"{ad_product} report {start_date} to {end_date} for Profile {profile_id}"
        
        # 構建報告配置
        configuration = {
            "adProduct": ad_product,
            "groupBy": ["campaign"],
            "columns": self._get_report_columns(ad_product),
            "reportTypeId": report_type_id,
            "timeUnit": "DAILY",
            "format": "GZIP_JSON"
        }
        
        # 構建請求體
        request_body = {
            "name": report_name,
            "startDate": start_date,
            "endDate": end_date,
            "configuration": configuration
        }
        
        # 設置請求頭
        headers = {
            "Authorization": f"Bearer {access_token}",
            "Amazon-Advertising-API-ClientId": self.amazon_ads_service.client_id,
            "Amazon-Advertising-API-Scope": profile_id,
            "Content-Type": "application/vnd.createasyncreportrequest.v3+json"
        }
        
        # 調用API創建報告
        endpoint = f"{self.amazon_ads_service.api_host}/reporting/reports"
        
        try:
            async with self.amazon_ads_service.httpx_client() as client:
                response = await client.post(endpoint, headers=headers, json=request_body)
                response.raise_for_status()
                report_data = response.json()
                
                logger.info(f"成功創建報告: report_id={report_data.get('reportId')}, status={report_data.get('status')}")
                
                # 將報告信息保存到數據庫
                if supabase:
                    try:
                        report_record = {
                            "report_id": report_data.get("reportId"),
                            "user_id": user_id,
                            "profile_id": profile_id,
                            "name": report_data.get("name"),
                            "status": report_data.get("status"),
                            "ad_product": ad_product,
                            "report_type_id": report_type_id,
                            "start_date": start_date,
                            "end_date": end_date,
                            "time_unit": configuration.get("timeUnit"),
                            "format": configuration.get("format"),
                            "configuration": configuration,
                            "created_at": datetime.now().isoformat(),
                            "updated_at": datetime.now().isoformat(),
                            "amazon_created_at": report_data.get("createdAt"),
                            "amazon_updated_at": report_data.get("updatedAt"),
                            "url": report_data.get("url"),
                            "url_expires_at": report_data.get("urlExpiresAt"),
                            "file_size": report_data.get("fileSize"),
                            "failure_reason": report_data.get("failureReason")
                        }
                        
                        result = supabase.table('amazon_ads_reports').upsert(
                            report_record,
                            on_conflict='profile_id,ad_product,start_date,end_date,report_type_id'
                        ).execute()
                        logger.info(f"報告信息已保存/更新到數據庫: {report_data.get('reportId')}")
                    except Exception as db_error:
                        logger.error(f"保存報告信息到數據庫時出錯: {str(db_error)}")
                        logger.error(traceback.format_exc())
                
                return report_data
        except Exception as e:
            if isinstance(e, httpx.HTTPStatusError):
                status_code = e.response.status_code
                response_text = e.response.text
                
                if status_code == 400:
                    logger.error(f"Amazon API 請求無效 (400 Bad Request): {response_text}")
                    error_message = f"Amazon API 返回錯誤 (400 Bad Request): {response_text}"
                    raise ValueError(error_message)
                    
                elif status_code == 425:
                    logger.info(f"檢測到重複報告請求 (425): {response_text}")
                    
                    duplicate_report_id = None
                    try:
                        error_response = json.loads(response_text)
                        if "detail" in error_response:
                            detail = error_response["detail"]
                            logger.info(f"重複報告詳情: {detail}")
                            
                            if "duplicate of :" in detail:
                                duplicate_report_id = detail.split("duplicate of :")[1].strip()
                                logger.info(f"重複報告 ID: {duplicate_report_id}")
                    except Exception as extract_error:
                        logger.info(f"無法從響應中提取報告 ID: {str(extract_error)}")
                    
                    error_message = f"報告請求重複"
                    if duplicate_report_id:
                        error_message += f", 重複報告 ID: {duplicate_report_id}"
                    
                    raise ValueError(f"DUPLICATE_REPORT:{duplicate_report_id}:{error_message}")
                    
                else:
                    logger.error(f"Amazon API 請求失敗 ({status_code}): {response_text}")
                    error_message = f"Amazon API 返回錯誤 ({status_code}): {response_text}"
                    raise ValueError(error_message)
            else:
                logger.error(f"創建報告時出錯: {str(e)}")
                raise
    
    def _get_report_columns(self, ad_product: str) -> List[str]:
        """
        獲取指定廣告產品的報告欄位
        
        Args:
            ad_product: 廣告產品類型
            
        Returns:
            List[str]: 欄位列表
        """
        
        match ad_product:
            case "SPONSORED_PRODUCTS":
                return [ 
                    "impressions","clicks","cost","purchases1d","purchases7d","purchases14d","purchases30d",
                    "purchasesSameSku1d","purchasesSameSku7d","purchasesSameSku14d","purchasesSameSku30d",
                    "unitsSoldClicks1d","unitsSoldClicks7d","unitsSoldClicks14d","unitsSoldClicks30d",
                    "sales1d","sales7d","sales14d","sales30d",
                    "attributedSalesSameSku1d","attributedSalesSameSku7d","attributedSalesSameSku14d","attributedSalesSameSku30d",
                    "unitsSoldSameSku1d","unitsSoldSameSku7d","unitsSoldSameSku14d","unitsSoldSameSku30d",
                    "kindleEditionNormalizedPagesRead14d","kindleEditionNormalizedPagesRoyalties14d",
                    "qualifiedBorrows","royaltyQualifiedBorrows","addToList","date",
                    "campaignBiddingStrategy","costPerClick","clickThroughRate","spend",
                    "acosClicks14d","roasClicks14d","retailer",
                    "campaignName","campaignId","campaignStatus","campaignBudgetAmount","campaignBudgetType","campaignRuleBasedBudgetAmount",
                    "campaignApplicableBudgetRuleId","campaignApplicableBudgetRuleName","campaignBudgetCurrencyCode","topOfSearchImpressionShare"
                ]
            case "SPONSORED_BRANDS":
                return [
                    "campaignName","campaignId","campaignStatus","impressions","clicks","cost","date",
                    "brandedSearches","purchases","purchasesPromoted","detailPageViews",
                    "newToBrandPurchasesRate","newToBrandPurchases","newToBrandPurchasesPercentage",
                    "sales","salesPromoted","newToBrandSales","newToBrandSalesPercentage","newToBrandUnitsSold","newToBrandUnitsSoldPercentage",
                    "unitsSold","viewClickThroughRate","video5SecondViewRate","video5SecondViews",
                    "videoCompleteViews","videoFirstQuartileViews","videoMidpointViews","videoThirdQuartileViews",
                    "videoUnmutes","viewableImpressions","viewabilityRate",
                    "brandedSearchesClicks","purchasesClicks","detailPageViewsClicks","newToBrandPurchasesClicks","salesClicks",
                    "newToBrandSalesClicks","newToBrandUnitsSoldClicks","unitsSoldClicks","costType","newToBrandDetailPageViews",
                    "newToBrandDetailPageViewsClicks","newToBrandDetailPageViewRate","newToBrandECPDetailPageView",
                    "addToCart","addToCartClicks","addToCartRate","eCPAddToCart",
                    "kindleEditionNormalizedPagesRead14d","kindleEditionNormalizedPagesRoyalties14d",
                    "qualifiedBorrows","qualifiedBorrowsFromClicks","royaltyQualifiedBorrows","royaltyQualifiedBorrowsFromClicks",
                    "addToList","addToListFromClicks","longTermSales","longTermROAS",
                    "campaignBudgetAmount","campaignBudgetCurrencyCode","campaignBudgetType","topOfSearchImpressionShare","campaignRuleBasedBudgetAmount"
                ]
            case "SPONSORED_DISPLAY":
                return [
                    "date","purchasesClicks","purchasesPromotedClicks","detailPageViewsClicks","newToBrandPurchasesClicks",
                    "salesClicks","salesPromotedClicks","newToBrandSalesClicks","unitsSoldClicks","newToBrandUnitsSoldClicks",
                    "campaignId","campaignName","clicks","cost","campaignBudgetCurrencyCode","impressions","purchases","detailPageViews",
                    "sales","unitsSold","impressionsViews","newToBrandPurchases","newToBrandUnitsSold","brandedSearchesClicks",
                    "brandedSearches","brandedSearchesViews","brandedSearchRate","eCPBrandSearch","videoCompleteViews",
                    "videoFirstQuartileViews","videoMidpointViews","videoThirdQuartileViews","videoUnmutes","viewabilityRate",
                    "viewClickThroughRate","addToCart","addToCartViews","addToCartClicks","addToCartRate","eCPAddToCart",
                    "qualifiedBorrows","qualifiedBorrowsFromClicks","qualifiedBorrowsFromViews","royaltyQualifiedBorrows",
                    "royaltyQualifiedBorrowsFromClicks","royaltyQualifiedBorrowsFromViews","addToList","addToListFromClicks",
                    "addToListFromViews","linkOuts","leadFormOpens","leads","longTermSales","longTermROAS","newToBrandSales",
                    "campaignStatus","campaignBudgetAmount","costType","impressionsFrequencyAverage","cumulativeReach",
                    "newToBrandDetailPageViews","newToBrandDetailPageViewViews","newToBrandDetailPageViewClicks",
                    "newToBrandDetailPageViewRate","newToBrandECPDetailPageView"
                ]
            case _:
                return []
            
    async def get_report_status(self, 
                              profile_id: str, 
                              access_token: str, 
                              report_id: str) -> Dict[str, Any]:
        """
        獲取報告狀態
        
        Args:
            profile_id: Amazon Ads 配置檔案 ID
            access_token: 訪問令牌
            report_id: 報告ID
            
        Returns:
            Dict[str, Any]: 報告狀態信息
        """
        logger.info(f"獲取報告狀態: report_id={report_id}")
        
        # 設置請求頭
        headers = {
            "Authorization": f"Bearer {access_token}",
            "Amazon-Advertising-API-ClientId": self.amazon_ads_service.client_id,
            "Amazon-Advertising-API-Scope": profile_id,
            "Accept": "application/json"
        }
        
        # 調用API獲取報告狀態
        endpoint = f"{self.amazon_ads_service.api_host}/reporting/reports/{report_id}"
        
        try:
            async with self.amazon_ads_service.httpx_client() as client:
                response = await client.get(endpoint, headers=headers)
                response.raise_for_status()
                status_data = response.json()
                
                logger.info(f"報告狀態: report_id={report_id}, status={status_data.get('status')}")
                
                # 更新數據庫中的報告狀態
                if supabase:
                    try:
                        update_data = {
                            "status": status_data.get("status"),
                            "updated_at": datetime.now().isoformat(),
                            "amazon_updated_at": status_data.get("updatedAt"),
                            "url": status_data.get("url"),
                            "url_expires_at": status_data.get("urlExpiresAt"),
                            "file_size": status_data.get("fileSize"),
                            "failure_reason": status_data.get("failureReason")
                        }
                        
                        result = supabase.table('amazon_ads_reports').update(update_data).eq('report_id', report_id).execute()
                        logger.info(f"報告狀態已更新: {report_id}")
                    except Exception as db_error:
                        logger.error(f"更新報告狀態到數據庫時出錯: {str(db_error)}")
                
                return status_data
        except Exception as e:
            logger.error(f"獲取報告狀態時出錯: {str(e)}")
            logger.error(traceback.format_exc())
            raise
    
    async def create_reports_for_profiles(self, 
                                        profiles: List,
                                        ad_products: List[str],
                                        start_date: Optional[str] = None, 
                                        end_date: Optional[str] = None) -> Dict[str, Any]:
        """
        為指定的 profiles 統一創建報告
        
        Args:
            profiles: Amazon Ads 連接列表
            ad_products: 廣告產品類型列表
            start_date: 報告開始日期 (YYYY-MM-DD)，默認為前7天
            end_date: 報告結束日期 (YYYY-MM-DD)，默認為前1天
            
        Returns:
            Dict[str, Any]: 處理結果統計
        """
        logger.info(f"開始為 {len(profiles)} 個 profiles 創建 {len(ad_products)} 種類型的報告")
        
        # 處理結果統計
        result_stats = {
            "success": True,
            "total_profiles": len(profiles),
            "processed_profiles": 0,
            "created_reports": 0,
            "failed_profiles": [],
            "details": {}
        }
        
        # 為每種廣告產品類型初始化詳情
        for ad_product in ad_products:
            result_stats["details"][ad_product] = {
                "success": False,
                "created_reports": 0,
                "processed_profiles": 0,
                "failed_profiles": []
            }
        
        # 處理每個 profile
        processed_profiles = set()
        
        for connection in profiles:
            try:
                profile_id = connection.profile_id
                logger.info(f"處理連接檔案: {profile_id}")
                
                # 只處理啟用且國籍為美國的連接檔案
                if connection.country_code != "US":
                    logger.info(f"跳過非美國國籍的檔案: {profile_id}, 國籍: {connection.country_code}")
                    continue
                
                # 獲取訪問令牌
                try:
                    access_token = await self._get_access_token(connection)
                except Exception as e:
                    logger.error(f"獲取訪問令牌失敗: {str(e)}")
                    error_info = {
                        "profile_id": profile_id,
                        "error": f"Failed to get access token: {str(e)}"
                    }
                    result_stats["failed_profiles"].append(error_info)
                    # 為所有產品類型添加失敗記錄
                    for ad_product in ad_products:
                        result_stats["details"][ad_product]["failed_profiles"].append(error_info)
                    continue
                
                profile_success = False
                
                # 為該 profile 創建每種產品類型的報告
                for ad_product in ad_products:
                    try:
                        # 創建報告請求
                        report_data = await self.create_report(
                            profile_id=profile_id,
                            access_token=access_token,
                            ad_product=ad_product,
                            start_date=start_date,
                            end_date=end_date,
                            user_id=connection.user_id
                        )
                        
                        if report_data and report_data.get("reportId"):
                            result_stats["details"][ad_product]["created_reports"] += 1
                            result_stats["created_reports"] += 1
                            result_stats["details"][ad_product]["success"] = True
                            profile_success = True
                            logger.info(f"為配置檔案 {profile_id} 創建了 {ad_product} 報告")
                        else:
                            logger.warning(f"為配置檔案 {profile_id} 創建 {ad_product} 報告失敗")
                            result_stats["details"][ad_product]["failed_profiles"].append({
                                "profile_id": profile_id,
                                "error": "Failed to create report, unknown reason"
                            })
                            
                    except ValueError as ve:
                        error_message = str(ve)
                        
                        # 檢查是否為重複報告錯誤 (425)
                        if "DUPLICATE_REPORT:" in error_message:
                            try:
                                duplicate_info = await self.handle_duplicate_report_error(
                                    error_message, profile_id, access_token, 
                                    connection.user_id, ad_product, start_date, end_date
                                )
                                
                                if duplicate_info.get("handled", False):
                                    result_stats["details"][ad_product]["created_reports"] += 1
                                    result_stats["created_reports"] += 1
                                    result_stats["details"][ad_product]["success"] = True
                                    profile_success = True
                                    logger.info(f"處理重複報告成功: {profile_id} - {ad_product}")
                                else:
                                    result_stats["details"][ad_product]["failed_profiles"].append({
                                        "profile_id": profile_id,
                                        "error": f"Duplicate report handling failed: {error_message}"
                                    })
                            except Exception as dup_error:
                                logger.error(f"處理重複報告錯誤時出錯: {str(dup_error)}")
                                result_stats["details"][ad_product]["failed_profiles"].append({
                                    "profile_id": profile_id,
                                    "error": f"Duplicate report error: {error_message}"
                                })
                        else:
                            logger.error(f"為配置檔案 {profile_id} 創建 {ad_product} 報告時出錯: {error_message}")
                            result_stats["details"][ad_product]["failed_profiles"].append({
                                "profile_id": profile_id,
                                "error": f"Failed to create report: {error_message}"
                            })
                            
                    except Exception as e:
                        logger.error(f"為配置檔案 {profile_id} 創建 {ad_product} 報告時出錯: {str(e)}")
                        logger.error(traceback.format_exc())
                        result_stats["details"][ad_product]["failed_profiles"].append({
                            "profile_id": profile_id,
                            "error": f"Failed to create report: {str(e)}"
                        })
                
                # 記錄成功處理的 profile
                if profile_success and profile_id not in processed_profiles:
                    processed_profiles.add(profile_id)
                    result_stats["processed_profiles"] += 1
                    # 為每個成功的產品類型更新 processed_profiles 計數
                    for ad_product in ad_products:
                        if result_stats["details"][ad_product]["created_reports"] > 0:
                            result_stats["details"][ad_product]["processed_profiles"] += 1
                
            except Exception as e:
                logger.error(f"處理連接檔案 {connection.profile_id} 時出錯: {str(e)}")
                logger.error(traceback.format_exc())
                error_info = {
                    "profile_id": connection.profile_id,
                    "error": str(e)
                }
                result_stats["failed_profiles"].append(error_info)
                # 為所有產品類型添加失敗記錄
                for ad_product in ad_products:
                    result_stats["details"][ad_product]["failed_profiles"].append(error_info)
        
        # 更新整體成功狀態
        if result_stats["created_reports"] > 0:
            result_stats["message"] = f"Successfully created {result_stats['created_reports']} reports from {result_stats['processed_profiles']} of {result_stats['total_profiles']} profiles"
        else:
            result_stats["success"] = False
            result_stats["message"] = "No reports were created"
        
        return result_stats
    
    async def handle_duplicate_report_error(self, 
                                          error_message: str, 
                                          profile_id: str, 
                                          access_token: str,
                                          user_id: str,
                                          ad_product: str,
                                          start_date: str,
                                          end_date: str) -> Dict[str, Any]:
        """
        處理重複報告錯誤 (425)
        
        Args:
            error_message: 錯誤信息
            profile_id: Amazon Ads 配置檔案 ID
            access_token: 訪問令牌
            user_id: 用戶ID
            ad_product: 廣告產品類型
            start_date: 報告開始日期
            end_date: 報告結束日期
            
        Returns:
            Dict[str, Any]: 處理結果
        """
        logger.info(f"處理重複報告錯誤: {profile_id}")
        
        try:
            # 從錯誤信息中提取重複報告 ID
            duplicate_report_id = None
            if "DUPLICATE_REPORT:" in error_message:
                parts = error_message.split("DUPLICATE_REPORT:")
                if len(parts) > 1:
                    # 格式: "DUPLICATE_REPORT:report_id:message"
                    info_parts = parts[1].split(":", 1)
                    if info_parts[0]:
                        duplicate_report_id = info_parts[0].strip()
                        
            if not duplicate_report_id:
                logger.warning(f"無法從錯誤信息中提取報告 ID: {error_message}")
                return {"handled": False, "error": "Could not extract report ID"}
            
            logger.info(f"找到重複報告 ID: {duplicate_report_id}")
            
            # 檢查我們的數據庫是否已有該報告記錄
            existing_report = await self._get_report_record(duplicate_report_id)
            
            if existing_report:
                logger.info(f"數據庫中已存在報告記錄: {duplicate_report_id}")
                return {
                    "handled": True, 
                    "message": "Report already exists in database",
                    "report_id": duplicate_report_id
                }
            
            # 如果數據庫中沒有記錄，從 Amazon 獲取報告狀態並保存
            logger.info(f"數據庫中不存在報告記錄，從 Amazon 獲取狀態: {duplicate_report_id}")
            
            try:
                # 直接調用 Amazon API 獲取報告狀態
                headers = {
                    "Authorization": f"Bearer {access_token}",
                    "Amazon-Advertising-API-ClientId": self.amazon_ads_service.client_id,
                    "Amazon-Advertising-API-Scope": profile_id,
                    "Accept": "application/json"
                }
                
                endpoint = f"{self.amazon_ads_service.api_host}/reporting/reports/{duplicate_report_id}"
                
                async with self.amazon_ads_service.httpx_client() as client:
                    response = await client.get(endpoint, headers=headers)
                    response.raise_for_status()
                    status_data = response.json()
                
                logger.info(f"成功從 Amazon 獲取報告狀態: {duplicate_report_id}, status={status_data.get('status')}")
                
                # 確定報告類型ID
                if ad_product == "SPONSORED_PRODUCTS":
                    report_type_id = "spCampaigns"
                elif ad_product == "SPONSORED_BRANDS":
                    report_type_id = "sbCampaigns"
                elif ad_product == "SPONSORED_DISPLAY":
                    report_type_id = "sdCampaigns"
                else:
                    report_type_id = "unknown"
                
                # 構建完整的報告記錄
                report_record = {
                    "report_id": duplicate_report_id,
                    "user_id": user_id,
                    "profile_id": profile_id,
                    "name": status_data.get("name", f"{ad_product} report {start_date} to {end_date} for Profile {profile_id}"),
                    "status": status_data.get("status"),
                    "ad_product": ad_product,
                    "report_type_id": report_type_id,
                    "start_date": start_date,
                    "end_date": end_date,
                    "time_unit": "DAILY",
                    "format": "GZIP_JSON",
                    "configuration": {
                        "adProduct": ad_product,
                        "groupBy": ["campaign"],
                        "columns": self._get_report_columns(ad_product),
                        "reportTypeId": report_type_id,
                        "timeUnit": "DAILY",
                        "format": "GZIP_JSON"
                    },
                    "created_at": datetime.now().isoformat(),
                    "updated_at": datetime.now().isoformat(),
                    "amazon_created_at": status_data.get("createdAt"),
                    "amazon_updated_at": status_data.get("updatedAt"),
                    "url": status_data.get("url"),
                    "url_expires_at": status_data.get("urlExpiresAt"),
                    "file_size": status_data.get("fileSize"),
                    "failure_reason": status_data.get("failureReason")
                }
                
                # 使用 upsert 操作插入/更新報告記錄
                if supabase:
                    try:
                        result = supabase.table('amazon_ads_reports').upsert(
                            report_record,
                            on_conflict='report_id'
                        ).execute()
                        logger.info(f"成功保存重複報告記錄到數據庫: {duplicate_report_id}")
                    except Exception as db_error:
                        logger.error(f"保存重複報告記錄到數據庫時出錯: {str(db_error)}")
                        logger.error(traceback.format_exc())
                
                logger.info(f"成功處理重複報告: {duplicate_report_id}")
                return {
                    "handled": True,
                    "message": "Retrieved and saved duplicate report",
                    "report_id": duplicate_report_id,
                    "status": status_data.get("status")
                }
                
            except Exception as status_error:
                logger.error(f"獲取重複報告狀態時出錯: {str(status_error)}")
                return {
                    "handled": False,
                    "error": f"Failed to get report status: {str(status_error)}"
                }
                
        except Exception as e:
            logger.error(f"處理重複報告錯誤時出錯: {str(e)}")
            logger.error(traceback.format_exc())
            return {
                "handled": False,
                "error": f"Error handling duplicate report: {str(e)}"
            }
    
    async def get_target_profiles(self, 
                                user_id: Optional[str] = None, 
                                profile_id: Optional[str] = None) -> List:
        """
        根據參數獲取目標 profiles
        
        Args:
            user_id: 用戶 ID，可選
            profile_id: Amazon Ads 配置檔案 ID，可選
            
        Returns:
            List: 目標 profiles 列表
        """
        if profile_id:
            logger.info(f"獲取單一 profile: {profile_id}")
            connection = await self.amazon_ads_service.get_connection_by_profile_id(profile_id)
            if not connection:
                logger.warning(f"未找到配置檔案 {profile_id} 的連接")
                return []
            return [connection]
        
        elif user_id:
            logger.info(f"獲取用戶 {user_id} 的所有 profiles")
            connections = await self.amazon_ads_service.get_user_connections(user_id)
            if not connections:
                logger.warning(f"未找到用戶 {user_id} 的連接檔案")
            return connections
        
        else:
            logger.info("獲取所有 profiles")
            connections = await self.amazon_ads_service.get_all_connections()
            if not connections:
                logger.warning("未找到任何 Amazon Ads 連接")
            return connections
    
    def get_ad_products(self, ad_product: Optional[str] = None) -> List[str]:
        """
        獲取要處理的廣告產品類型列表
        
        Args:
            ad_product: 指定的廣告產品類型，可選
            
        Returns:
            List[str]: 廣告產品類型列表
        """
        available_products = [
            AdProduct.SPONSORED_PRODUCTS.value,
            AdProduct.SPONSORED_BRANDS.value,
            AdProduct.SPONSORED_DISPLAY.value
        ]
        
        if ad_product:
            if ad_product in available_products:
                return [ad_product]
            else:
                raise ValueError(f"Invalid ad_product: {ad_product}")
        
        return available_products
    
    async def process_report(self, report_id: str) -> Dict[str, Any]:
        """
        處理單個報告
        
        Args:
            report_id: 報告 ID
            
        Returns:
            Dict[str, Any]: 處理結果
        """
        logger.info(f"開始處理報告: {report_id}")
        
        report_record = await self._get_report_record(report_id)
        if not report_record:
            logger.error(f"找不到報告: {report_id}")
            raise ValueError(f"Report not found: {report_id}")
        
        connection = await self._get_connection(report_record['profile_id'])
        if not connection:
            logger.error(f"找不到連接: {report_record['profile_id']}")
            raise ValueError(f"Connection not found: {report_record['profile_id']}")
        
        access_token = await self._get_access_token(connection)
        
        status_data = await self.get_report_status(
            report_record['profile_id'], 
            access_token, 
            report_id
        )
        
        result = {
            "report_id": report_id,
            "status": status_data.get("status"),
            "download_status": report_record.get("download_status", DownloadStatus.PENDING.value),
            "processed_status": report_record.get("processed_status", ProcessedStatus.PENDING.value),
            "message": ""
        }
        
        if status_data.get("status") == ReportStatus.COMPLETED.value and status_data.get("url"):
            result = await self._handle_completed_report(
                report_record, 
                status_data, 
                result
            )
        else:
            result = self._handle_non_completed_report(status_data, result)
        
        return result
    
    async def process_multiple_reports(
        self, 
        user_id: Optional[str] = None, 
        profile_id: Optional[str] = None, 
        limit: int = 20
    ) -> Dict[str, Any]:
        """
        批量處理報告
        
        Args:
            user_id: 用戶 ID，可選
            profile_id: Amazon Ads 配置檔案 ID，可選
            limit: 處理的最大報告數量
            
        Returns:
            Dict[str, Any]: 處理結果
        """
        logger.info("開始批量處理報告")
        
        pending_reports = await self._get_pending_reports(user_id, profile_id, limit)
        
        result = {
            "total_reports": len(pending_reports),
            "processed_reports": 0,
            "failed_reports": 0,
            "details": []
        }
        
        for report in pending_reports:
            try:
                report_result = await self.process_report(report['report_id'])
                
                if report_result.get('download_status') == DownloadStatus.COMPLETED.value:
                    result["processed_reports"] += 1
                else:
                    result["failed_reports"] += 1
                    
                result["details"].append(report_result)
                
            except Exception as e:
                logger.error(f"處理報告 {report['report_id']} 時出錯: {str(e)}")
                logger.error(traceback.format_exc())
                
                result["failed_reports"] += 1
                
                result["details"].append({
                    "report_id": report['report_id'],
                    "status": report.get('status'),
                    "download_status": DownloadStatus.FAILED.value,
                    "message": f"處理出錯: {str(e)}"
                })
        
        return result
    
    async def _get_report_record(self, report_id: str) -> Optional[Dict[str, Any]]:
        """
        從數據庫獲取報告記錄
        
        Args:
            report_id: 報告 ID
            
        Returns:
            Optional[Dict[str, Any]]: 報告記錄，若不存在則返回 None
        """
        report_query = supabase.table('amazon_ads_reports').select('*').eq('report_id', report_id).execute()
        if not report_query.data:
            return None
        return report_query.data[0]
    
    async def _get_connection(self, profile_id: str) -> Any:
        """
        獲取連接信息
        
        Args:
            profile_id: Amazon Ads 配置檔案 ID
            
        Returns:
            Any: 連接信息
        """
        return await self.amazon_ads_service.get_connection_by_profile_id(profile_id)
    
    async def _get_access_token(self, connection) -> str:
        """
        獲取訪問令牌
        
        Args:
            connection: 連接信息
            
        Returns:
            str: 訪問令牌
        """
        from ..core.security import decrypt_token
        
        refresh_token = decrypt_token(connection.refresh_token)
        
        try:
            token_response = await self.amazon_ads_service.refresh_access_token(refresh_token)
            access_token = token_response.get("access_token")
            
            if not access_token:
                logger.error("無法獲取訪問令牌")
                raise ValueError("Failed to get access token")
                
            return access_token
        except Exception as e:
            logger.error(f"獲取訪問令牌時出錯: {str(e)}")
            logger.error(traceback.format_exc())
            raise
    
    async def _handle_completed_report(
        self, 
        report_record: Dict[str, Any], 
        status_data: Dict[str, Any], 
        result: Dict[str, Any]
    ) -> Dict[str, Any]:
        """
        處理已完成的報告
        
        Args:
            report_record: 報告記錄
            status_data: 報告狀態數據
            result: 處理結果
            
        Returns:
            Dict[str, Any]: 更新後的處理結果
        """
        if report_record.get("download_status") == DownloadStatus.COMPLETED.value and report_record.get("storage_path"):
            result["message"] = "報告已下載過"
            result["storage_path"] = report_record.get("storage_path")
            return result
        
        try:
            report_content = await self.amazon_ads_service.download_report(status_data.get("url"))
            
            processed_data = await self._process_report_content(report_content)
            
            storage_path = await self._upload_report_to_supabase(
                report_record['user_id'], 
                report_record['profile_id'], 
                report_record['ad_product'], 
                report_record['report_id'], 
                processed_data
            )
            
            await self._store_report_in_timescaledb(
                report_record, 
                processed_data
            )
            
            update_data = {
                "download_status": DownloadStatus.COMPLETED.value,
                "processed_status": ProcessedStatus.COMPLETED.value,
                "storage_path": storage_path,
                "updated_at": datetime.now().isoformat()
            }
            
            supabase.table('amazon_ads_reports').update(update_data).eq('report_id', report_record['report_id']).execute()
            
            result["download_status"] = DownloadStatus.COMPLETED.value
            result["processed_status"] = ProcessedStatus.COMPLETED.value
            result["storage_path"] = storage_path
            result["message"] = "報告已成功下載和處理"
            
            logger.info(f"報告 {report_record['report_id']} 已成功下載和處理，存儲路徑: {storage_path}")
            
        except Exception as e:
            error_msg = str(e)
            logger.error(f"處理報告 {report_record['report_id']} 時出錯: {error_msg}")
            
            update_data = {
                "download_status": DownloadStatus.FAILED.value,
                "failure_reason": error_msg,
                "updated_at": datetime.now().isoformat()
            }
            
            supabase.table('amazon_ads_reports').update(update_data).eq('report_id', report_record['report_id']).execute()
            
            result["download_status"] = DownloadStatus.FAILED.value
            result["message"] = f"報告下載失敗: {error_msg}"
        
        return result
    
    def _handle_non_completed_report(
        self, 
        status_data: Dict[str, Any], 
        result: Dict[str, Any]
    ) -> Dict[str, Any]:
        """
        處理未完成的報告
        
        Args:
            status_data: 報告狀態數據
            result: 處理結果
            
        Returns:
            Dict[str, Any]: 更新後的處理結果
        """
        if status_data.get("status") != ReportStatus.COMPLETED.value:
            result["message"] = f"報告尚未完成，狀態: {status_data.get('status')}"
        elif not status_data.get("url"):
            result["message"] = "報告已完成但沒有下載 URL"
            
        return result
    
    async def _get_pending_reports(
        self, 
        user_id: Optional[str] = None, 
        profile_id: Optional[str] = None, 
        limit: int = 20
    ) -> List[Dict[str, Any]]:
        """
        獲取待處理的報告
        
        Args:
            user_id: 用戶 ID，可選
            profile_id: Amazon Ads 配置檔案 ID，可選
            limit: 獲取的最大報告數量
            
        Returns:
            List[Dict[str, Any]]: 待處理的報告列表
        """
        query = supabase.table('amazon_ads_reports').select('*')
        
        query = query.eq('status', ReportStatus.PENDING.value).eq('download_status', DownloadStatus.PENDING.value)
        
        if user_id:
            query = query.eq('user_id', user_id)
            
        if profile_id:
            query = query.eq('profile_id', profile_id)
            
        reports_result = query.limit(limit).execute()
        return reports_result.data
    
    async def _process_report_content(self, content: bytes) -> Union[Dict[str, Any], List[Dict[str, Any]]]:
        """
        處理報告內容 - 解壓並解析 GZIP_JSON 格式
        
        Args:
            content: 報告的原始字節數據
            
        Returns:
            Union[Dict[str, Any], List[Dict[str, Any]]]: 解析後的報告數據
        """
        logger.info("正在處理報告內容...")
        
        try:
            with gzip.GzipFile(fileobj=io.BytesIO(content), mode="rb") as f:
                decompressed_content = f.read()
            
            logger.info(f"報告解壓成功，大小: {len(decompressed_content)} 字節")
            
            parsed_data = json.loads(decompressed_content)
            logger.info(f"報告解析成功，包含 {len(parsed_data) if isinstance(parsed_data, list) else '1'} 條記錄")
            
            return parsed_data
        except Exception as e:
            logger.error(f"處理報告內容時出錯: {str(e)}")
            logger.error(traceback.format_exc())
            raise
    
    async def _upload_report_to_supabase(
        self, 
        user_id: str, 
        profile_id: str, 
        ad_product: str, 
        report_id: str, 
        content: Union[bytes, Dict, List]
    ) -> str:
        """
        將報告上傳到 Supabase 存儲桶
        
        Args:
            user_id: 用戶 ID
            profile_id: Amazon Ads 配置檔案 ID
            ad_product: 廣告產品類型
            report_id: 報告 ID
            content: 報告內容 (字節、字典或列表)
            
        Returns:
            str: Supabase 存儲路徑
        """
        logger.info(f"正在上傳報告到 Supabase: {report_id}")
        
        storage_path = f"reports/{user_id}/{profile_id}/{ad_product}/{report_id}.json"
        
        try:
            if isinstance(content, (dict, list)):
                content_str = json.dumps(content)
                content_bytes = content_str.encode('utf-8')
            elif isinstance(content, bytes):
                try:
                    parsed = json.loads(content.decode('utf-8'))
                    content_str = json.dumps(parsed)
                    content_bytes = content_str.encode('utf-8')
                except:
                    content_bytes = content
            else:
                raise TypeError(f"不支持的內容類型: {type(content)}")
            
            bucket_name = "amazon-ads-data"
            try:
                buckets = supabase.storage.list_buckets()
                bucket_exists = any(bucket.name == bucket_name for bucket in buckets)
                
                if not bucket_exists:
                    logger.info(f"創建存儲桶: {bucket_name}")
                    supabase.storage.create_bucket(bucket_name)
            except Exception as bucket_error:
                logger.warning(f"檢查或創建存儲桶時出錯: {str(bucket_error)}")
            
            logger.info(f"開始上傳文件: {storage_path}")
            result = supabase.storage.from_(bucket_name).upload(
                path=storage_path,
                file=content_bytes,
                file_options={"content-type": "application/json", "upsert": "true"}
            )
            
            logger.info(f"文件上傳成功: {storage_path}")
            return storage_path
        except Exception as e:
            logger.error(f"上傳報告到 Supabase 時出錯: {str(e)}")
            logger.error(traceback.format_exc())
            raise
            
    async def _store_report_in_timescaledb(self, report_record: Dict[str, Any], report_data: Union[Dict[str, Any], List[Dict[str, Any]]]) -> None:
        """
        將報告數據存儲到 TimescaleDB
        
        Args:
            report_record: 報告記錄
            report_data: 解析後的報告數據
            
        Returns:
            None
        """
        ad_product = report_record['ad_product']
        
        if not isinstance(report_data, list):
            logger.warning(f"報告數據不是列表格式: {type(report_data)}")
            return
            
        if ad_product == AdProduct.SPONSORED_PRODUCTS.value:
            await self._store_sp_report(report_record, report_data)
        elif ad_product == AdProduct.SPONSORED_BRANDS.value:
            await self._store_sb_report(report_record, report_data)
        elif ad_product == AdProduct.SPONSORED_DISPLAY.value:
            await self._store_sd_report(report_record, report_data)
        else:
            logger.warning(f"未知的廣告產品類型: {ad_product}")
            
    async def _batch_insert(self, table_name: str, records: List[Dict[str, Any]], batch_size: int = 200) -> None:
        """
        批量插入數據到指定表
        
        Args:
            table_name: 表名
            records: 要插入的記錄列表
            batch_size: 批量插入的大小
            
        Returns:
            None
        """
        if not records:
            logger.warning(f"沒有記錄要插入到 {table_name}")
            return
            
        total_records = len(records)
        logger.info(f"開始批量插入 {total_records} 條記錄到 {table_name}")
        
        for i in range(0, total_records, batch_size):
            batch = records[i:i + batch_size]
            try:
                supabase.table(table_name).upsert(batch).execute()
                logger.info(f"成功插入/更新批次 {i//batch_size + 1}/{(total_records + batch_size - 1)//batch_size}：{len(batch)} 條記錄")
            except Exception as e:
                logger.error(f"批量插入/更新到 {table_name} 時出錯: {str(e)}")
                logger.error(traceback.format_exc())
    
    async def _store_sp_report(self, report_record: Dict[str, Any], report_data: List[Dict[str, Any]]) -> None:
        """
        存儲 Sponsored Products 報告數據
        
        Args:
            report_record: 報告記錄
            report_data: 解析後的報告數據
            
        Returns:
            None
        """
        logger.info(f"存儲 SP 報告數據: {report_record['report_id']}, 共 {len(report_data)} 條記錄")
        
        if not isinstance(report_data, list):
            logger.warning(f"報告數據不是列表格式: {type(report_data)}")
            return
        
        batch_records = []
        
        for item in report_data:
            try:
                campaign_id = item.get('campaignId')
                if not campaign_id:
                    logger.warning(f"報告項目缺少 campaignId: {item}")
                    continue
                
                insert_data = {
                    "report_id": report_record['report_id'],
                    "profile_id": report_record['profile_id'],
                    "campaignId": str(campaign_id),
                    "date": item.get('date'),
                    "user_id": report_record['user_id'],
                }
                
                for key, value in item.items():
                    if key not in insert_data:  # 避免覆蓋已設置的值
                        insert_data[key] = value
                
                batch_records.append(insert_data)
                
            except Exception as e:
                logger.error(f"準備 SP 報告數據時出錯: {str(e)}")
                logger.error(traceback.format_exc())
        
        await self._batch_insert('amazon_ads_campaigns_reports_sp', batch_records)
    
    async def _store_sb_report(self, report_record: Dict[str, Any], report_data: List[Dict[str, Any]]) -> None:
        """
        存儲 Sponsored Brands 報告數據
        
        Args:
            report_record: 報告記錄
            report_data: 解析後的報告數據
            
        Returns:
            None
        """
        logger.info(f"存儲 SB 報告數據: {report_record['report_id']}, 共 {len(report_data)} 條記錄")
        
        if not isinstance(report_data, list):
            logger.warning(f"報告數據不是列表格式: {type(report_data)}")
            return
        
        batch_records = []
        
        for item in report_data:
            try:
                campaign_id = item.get('campaignId')
                if not campaign_id:
                    logger.warning(f"報告項目缺少 campaignId: {item}")
                    continue
                
                insert_data = {
                    "report_id": report_record['report_id'],
                    "profile_id": report_record['profile_id'],
                    "campaignId": str(campaign_id),
                    "date": item.get('date'),
                    "user_id": report_record['user_id'],
                }
                
                for key, value in item.items():
                    if key not in insert_data:  # 避免覆蓋已設置的值
                        insert_data[key] = value
                
                batch_records.append(insert_data)
                
            except Exception as e:
                logger.error(f"準備 SB 報告數據時出錯: {str(e)}")
                logger.error(traceback.format_exc())
        
        await self._batch_insert('amazon_ads_campaigns_reports_sb', batch_records)
    
    async def _store_sd_report(self, report_record: Dict[str, Any], report_data: List[Dict[str, Any]]) -> None:
        """
        存儲 Sponsored Display 報告數據
        
        Args:
            report_record: 報告記錄
            report_data: 解析後的報告數據
            
        Returns:
            None
        """
        logger.info(f"存儲 SD 報告數據: {report_record['report_id']}, 共 {len(report_data)} 條記錄")
        
        if not isinstance(report_data, list):
            logger.warning(f"報告數據不是列表格式: {type(report_data)}")
            return
        
        batch_records = []
        
        for item in report_data:
            try:
                campaign_id = item.get('campaignId')
                if not campaign_id:
                    logger.warning(f"報告項目缺少 campaignId: {item}")
                    continue
                
                insert_data = {
                    "report_id": report_record['report_id'],
                    "profile_id": report_record['profile_id'],
                    "campaignId": str(campaign_id),
                    "date": item.get('date'),
                    "user_id": report_record['user_id'],
                }
                
                for key, value in item.items():
                    if key not in insert_data:  # 避免覆蓋已設置的值
                        insert_data[key] = value
                
                batch_records.append(insert_data)
                
            except Exception as e:
                logger.error(f"準備 SD 報告數據時出錯: {str(e)}")
                logger.error(traceback.format_exc())
        
        await self._batch_insert('amazon_ads_campaigns_reports_sd', batch_records)
