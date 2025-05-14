# 空檔案初始化模塊
from .connections import router as connections_router
from .amazon_ads_campaigns import router as amazon_ads_campaigns_router

# 導出所有路由
routers = [
    connections_router,
    amazon_ads_campaigns_router
]
