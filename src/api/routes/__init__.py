# 空檔案初始化模塊
from .connections import router as connections_router
from .campaigns import router as campaigns_router

# 導出所有路由
routers = [
    connections_router,
    campaigns_router
]
