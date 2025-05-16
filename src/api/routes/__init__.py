# 空檔案初始化模塊
from .connections import router as connections_router
from .campaigns import router as campaigns_router
from .reports import router as reports_router

# 導出所有路由
routers = [
    connections_router,
    campaigns_router,
    reports_router
]
