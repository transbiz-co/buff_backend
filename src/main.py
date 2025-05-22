from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from fastapi.openapi.docs import get_swagger_ui_html, get_redoc_html
from fastapi.openapi.utils import get_openapi
import logging
from fastapi.responses import JSONResponse
from datetime import datetime

# 導入所有路由
from .api.routes import routers
from .core.config import settings
from .core.supabase import supabase

# 設定日誌
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
)
logger = logging.getLogger("buff_api")

app = FastAPI(
    title=settings.PROJECT_NAME,
    description="Buff API - Amazon FBA 廣告優化平台 API",
    version="0.1.0",
    docs_url="/docs",  # Swagger UI 的 URL
    redoc_url="/redoc",  # ReDoc 的 URL
    openapi_url="/openapi.json",  # OpenAPI 規範的 URL
)

# 設定 CORS
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],  # 修改為允許所有來源
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# 註冊所有路由
for router in routers:
    app.include_router(router, prefix=settings.API_V1_STR)

@app.get("/")
async def root():
    """
    首頁 API
    
    返回：
        dict: 歡迎信息
    """
    return {"message": "Welcome to Buff API"}

@app.get("/api/health")
async def health_check():
    """
    健康檢查端點
    
    簡單檢查 API 服務和資料庫連接狀態
    
    返回：
        dict: 系統狀態信息
    """
    # 準備基本響應
    response = {
        "status": "healthy",
        "api_time": datetime.now().isoformat()
    }
    
    # 檢查資料庫連接
    if not supabase:
        response["status"] = "unhealthy"
        response["error"] = "Supabase client not initialized"
        return JSONResponse(status_code=500, content=response)
    
    try:
        # 嘗試執行簡單查詢，只要不報錯就表示連接正常
        # 使用實際存在的表格執行最簡單的查詢
        result = supabase.table('amazon_ads_connections').select('count').limit(1).execute()
        
        # 只要成功執行查詢，就表示資料庫連接正常
        response["db_connected"] = True
        return response
    except Exception as e:
        response["status"] = "unhealthy"
        response["db_connected"] = False
        response["error"] = str(e)
        return JSONResponse(status_code=500, content=response)

# 自定義 OpenAPI 規範
def custom_openapi():
    if app.openapi_schema:
        return app.openapi_schema
    openapi_schema = get_openapi(
        title=app.title,
        version=app.version,
        description=app.description,
        routes=app.routes,
    )
    # 自定義 OpenAPI 規範
    # 可以在這裡添加更多自定義設定，如外部文檔連結等
    
    # 添加伺服器資訊
    openapi_schema["servers"] = [
        {"url": "http://localhost:8000", "description": "開發環境"},
        {"url": "https://buff-backend-oi2s.onrender.com", "description": "生產環境"}
    ]
    
    # 添加標籤說明
    openapi_schema["tags"] = [
        {
            "name": "connections",
            "description": "Amazon 授權帳號管理",
            "externalDocs": {
                "description": "文檔",
                "url": "https://example.com/docs/connections"
            }
        },
        {
            "name": "amazon-ads-metadatas",
            "description": "Amazon 廣告 Metadata",
            "externalDocs": {
                "description": "文檔",
                "url": "https://example.com/docs/amazon-ads-metadatas"
            }
        },
    ]
    
    app.openapi_schema = openapi_schema
    return app.openapi_schema

app.openapi = custom_openapi

# 應用啟動時事件
@app.on_event("startup")
async def startup_event():
    """應用啟動時執行的事件"""
    logger.info("應用啟動中...")
    logger.info("應用啟動完成") 