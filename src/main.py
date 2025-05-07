from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from fastapi.openapi.docs import get_swagger_ui_html, get_redoc_html
from fastapi.openapi.utils import get_openapi
import logging

from .api.routes.connections import router as connections_router
from .api.routes.examples.campaigns import router as campaigns_examples_router
from .core.config import settings
from .core.supabase import check_supabase_migrations, get_supabase_migrations

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
    allow_origins=[settings.FRONTEND_URL],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# 註冊路由
app.include_router(connections_router, prefix=settings.API_V1_STR)
app.include_router(campaigns_examples_router, prefix=settings.API_V1_STR)

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
    
    檢查 API 服務和資料庫遷移狀態
    
    返回：
        dict: 系統狀態信息
    """
    # 檢查遷移狀態
    migrations_ok, migrations_message = check_supabase_migrations()
    
    # 獲取已應用的遷移列表
    applied_migrations = get_supabase_migrations()
    
    return {
        "status": "healthy",
        "version": "0.1.0",
        "migrations_ok": migrations_ok,
        "migrations_message": migrations_message,
        "applied_migrations_count": len(applied_migrations)
    }

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
        {"url": "https://api.example.com", "description": "生產環境"}
    ]
    
    # 添加標籤說明
    openapi_schema["tags"] = [
        {
            "name": "connections",
            "description": "第三方平台連接管理",
            "externalDocs": {
                "description": "文檔",
                "url": "https://example.com/docs/connections"
            }
        },
        {
            "name": "examples",
            "description": "API 使用示例"
        }
    ]
    
    app.openapi_schema = openapi_schema
    return app.openapi_schema

app.openapi = custom_openapi

# 應用啟動時檢查遷移狀態
@app.on_event("startup")
async def startup_event():
    """應用啟動時執行的事件"""
    logger.info("應用啟動中...")
    
    # 檢查 Supabase 遷移狀態
    migrations_ok, migrations_message = check_supabase_migrations()
    if not migrations_ok:
        logger.warning(f"遷移狀態檢查: {migrations_message}")
    else:
        logger.info(f"遷移狀態檢查: {migrations_message}")
    
    # 獲取已應用的遷移
    migrations = get_supabase_migrations()
    logger.info(f"已應用 {len(migrations)} 個遷移")
    
    logger.info("應用啟動完成")
