# 空檔案初始化模塊
from .connections import router as connections_router
from .metadatas import router as metadatas_router
from .reports import router as reports_router

# 導出所有路由
routers = [
    connections_router,
    metadatas_router,
    reports_router
]
