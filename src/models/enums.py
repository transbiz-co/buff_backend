from enum import Enum

class ReportStatus(str, Enum):
    """Amazon 報告狀態枚舉"""
    PENDING = "PENDING"
    PROCESSING = "PROCESSING"
    COMPLETED = "COMPLETED"
    FAILED = "FAILED"

class DownloadStatus(str, Enum):
    """報告下載狀態枚舉"""
    PENDING = "PENDING"
    COMPLETED = "COMPLETED"
    FAILED = "FAILED"

class ProcessedStatus(str, Enum):
    """報告處理狀態枚舉"""
    PENDING = "PENDING"
    COMPLETED = "COMPLETED"
    FAILED = "FAILED"

class AdProduct(str, Enum):
    """廣告產品類型枚舉"""
    SPONSORED_PRODUCTS = "SPONSORED_PRODUCTS"
    SPONSORED_BRANDS = "SPONSORED_BRANDS"
    SPONSORED_DISPLAY = "SPONSORED_DISPLAY"
