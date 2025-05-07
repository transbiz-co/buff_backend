import os
from dotenv import load_dotenv

# 加載環境變數
load_dotenv()

class Settings:
    # 應用配置
    PROJECT_NAME: str = "Buff API"
    API_V1_STR: str = "/api/v1"
    
    # 前端 URL
    FRONTEND_URL: str = os.getenv("FRONTEND_URL", "http://localhost:3000")
    
    # Amazon Ads API 配置
    AMAZON_ADS_CLIENT_ID: str = os.getenv("AMAZON_ADS_CLIENT_ID", "")
    AMAZON_ADS_CLIENT_SECRET: str = os.getenv("AMAZON_ADS_CLIENT_SECRET", "")
    AMAZON_ADS_REDIRECT_URI: str = os.getenv("AMAZON_ADS_REDIRECT_URI", "")
    
    # Supabase 配置
    SUPABASE_URL: str = os.getenv("SUPABASE_URL", "")
    SUPABASE_KEY: str = os.getenv("SUPABASE_KEY", "")
    
    # 安全配置
    SECRET_KEY: str = os.getenv("SECRET_KEY", "your-secret-key")
    ALGORITHM: str = "HS256"
    ACCESS_TOKEN_EXPIRE_MINUTES: int = 60 * 24 * 7  # 7 天

# 創建設置實例
settings = Settings()
