import logging
import traceback
from datetime import datetime
from typing import Dict, Any, List, Optional, Union
import json
import gzip
import io

from ..models.enums import ReportStatus, DownloadStatus, ProcessedStatus, AdProduct
from .amazon_ads import supabase

logger = logging.getLogger(__name__)

class ReportProcessor:
    """
    報告處理器，負責檢查、下載和處理報告
    使用狀態機模式處理不同狀態的報告
    """
    
    def __init__(self, amazon_ads_service):
        self.amazon_ads_service = amazon_ads_service
    
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
        
        status_data = await self.amazon_ads_service.get_report_status(
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
