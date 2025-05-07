from fastapi import APIRouter, HTTPException, Query, Path, Body
from typing import List, Optional, Dict, Any
from pydantic import BaseModel, Field
from enum import Enum
from datetime import datetime, date

router = APIRouter(
    prefix="/examples/campaigns",
    tags=["examples"],
    responses={404: {"description": "未找到"}}
)

# 枚舉類型
class CampaignState(str, Enum):
    """
    廣告活動狀態
    """
    ENABLED = "ENABLED"
    PAUSED = "PAUSED"
    ARCHIVED = "ARCHIVED"

class CampaignType(str, Enum):
    """
    廣告活動類型
    """
    SPONSORED_PRODUCTS = "SPONSORED_PRODUCTS"
    SPONSORED_BRANDS = "SPONSORED_BRANDS"
    SPONSORED_DISPLAY = "SPONSORED_DISPLAY"

# 模型定義
class Campaign(BaseModel):
    """
    廣告活動資料模型
    """
    id: str = Field(..., description="廣告活動 ID")
    name: str = Field(..., description="廣告活動名稱")
    state: CampaignState = Field(..., description="廣告活動狀態")
    type: CampaignType = Field(..., description="廣告活動類型")
    daily_budget: float = Field(..., description="每日預算", gt=0)
    start_date: date = Field(..., description="開始日期")
    end_date: Optional[date] = Field(None, description="結束日期")
    created_at: datetime = Field(..., description="創建時間")
    updated_at: datetime = Field(..., description="更新時間")
    
    class Config:
        schema_extra = {
            "example": {
                "id": "c123456789",
                "name": "Summer Sale 2025",
                "state": "ENABLED",
                "type": "SPONSORED_PRODUCTS",
                "daily_budget": 50.0,
                "start_date": "2025-06-01",
                "end_date": "2025-07-31",
                "created_at": "2025-05-01T12:00:00Z",
                "updated_at": "2025-05-15T09:30:00Z"
            }
        }

class CampaignCreate(BaseModel):
    """
    創建廣告活動請求模型
    """
    name: str = Field(..., description="廣告活動名稱", min_length=1, max_length=128)
    state: CampaignState = Field(..., description="廣告活動狀態")
    type: CampaignType = Field(..., description="廣告活動類型")
    daily_budget: float = Field(..., description="每日預算", gt=0)
    start_date: date = Field(..., description="開始日期")
    end_date: Optional[date] = Field(None, description="結束日期")
    
    class Config:
        schema_extra = {
            "example": {
                "name": "Summer Sale 2025",
                "state": "ENABLED",
                "type": "SPONSORED_PRODUCTS",
                "daily_budget": 50.0,
                "start_date": "2025-06-01",
                "end_date": "2025-07-31"
            }
        }

class CampaignUpdate(BaseModel):
    """
    更新廣告活動請求模型
    """
    name: Optional[str] = Field(None, description="廣告活動名稱", min_length=1, max_length=128)
    state: Optional[CampaignState] = Field(None, description="廣告活動狀態")
    daily_budget: Optional[float] = Field(None, description="每日預算", gt=0)
    start_date: Optional[date] = Field(None, description="開始日期")
    end_date: Optional[date] = Field(None, description="結束日期")
    
    class Config:
        schema_extra = {
            "example": {
                "name": "Updated Summer Sale 2025",
                "state": "PAUSED",
                "daily_budget": 75.0
            }
        }

class CampaignsList(BaseModel):
    """
    廣告活動列表響應
    """
    items: List[Campaign] = Field(..., description="廣告活動列表")
    total: int = Field(..., description="總數")
    
    class Config:
        schema_extra = {
            "example": {
                "items": [
                    {
                        "id": "c123456789",
                        "name": "Summer Sale 2025",
                        "state": "ENABLED",
                        "type": "SPONSORED_PRODUCTS",
                        "daily_budget": 50.0,
                        "start_date": "2025-06-01",
                        "end_date": "2025-07-31",
                        "created_at": "2025-05-01T12:00:00Z",
                        "updated_at": "2025-05-15T09:30:00Z"
                    },
                    {
                        "id": "c987654321",
                        "name": "Winter Promotion",
                        "state": "PAUSED",
                        "type": "SPONSORED_BRANDS",
                        "daily_budget": 75.0,
                        "start_date": "2025-11-01",
                        "end_date": "2025-12-31",
                        "created_at": "2025-10-01T10:00:00Z",
                        "updated_at": "2025-10-05T14:20:00Z"
                    }
                ],
                "total": 2
            }
        }

# API 端點
@router.get(
    "/",
    response_model=CampaignsList,
    summary="獲取廣告活動列表",
    description="""
    獲取當前用戶的廣告活動列表，可以按狀態和類型進行過濾。
    支持分頁和排序。
    """,
    responses={
        200: {
            "description": "廣告活動列表",
            "content": {
                "application/json": {
                    "example": {
                        "items": [
                            {
                                "id": "c123456789",
                                "name": "Summer Sale 2025",
                                "state": "ENABLED",
                                "type": "SPONSORED_PRODUCTS",
                                "daily_budget": 50.0,
                                "start_date": "2025-06-01",
                                "end_date": "2025-07-31",
                                "created_at": "2025-05-01T12:00:00Z",
                                "updated_at": "2025-05-15T09:30:00Z"
                            },
                            {
                                "id": "c987654321",
                                "name": "Winter Promotion",
                                "state": "PAUSED",
                                "type": "SPONSORED_BRANDS",
                                "daily_budget": 75.0,
                                "start_date": "2025-11-01",
                                "end_date": "2025-12-31",
                                "created_at": "2025-10-01T10:00:00Z",
                                "updated_at": "2025-10-05T14:20:00Z"
                            }
                        ],
                        "total": 2
                    }
                }
            }
        }
    }
)
async def get_campaigns(
    state: Optional[CampaignState] = Query(None, description="按廣告活動狀態過濾"),
    type: Optional[CampaignType] = Query(None, description="按廣告活動類型過濾"),
    offset: int = Query(0, description="分頁偏移量", ge=0),
    limit: int = Query(10, description="每頁數量", ge=1, le=100),
    sort_by: str = Query("created_at", description="排序字段"),
    order: str = Query("desc", description="排序方向: asc（升序）或 desc（降序）")
):
    """
    獲取廣告活動列表
    
    參數:
        state: 按廣告活動狀態過濾
        type: 按廣告活動類型過濾
        offset: 分頁偏移量
        limit: 每頁數量
        sort_by: 排序字段
        order: 排序方向
        
    返回:
        廣告活動列表和總數
    """
    # 這裡實際應用中會從資料庫查詢，這裡僅作示例
    campaigns = [
        Campaign(
            id="c123456789",
            name="Summer Sale 2025",
            state=CampaignState.ENABLED,
            type=CampaignType.SPONSORED_PRODUCTS,
            daily_budget=50.0,
            start_date=date(2025, 6, 1),
            end_date=date(2025, 7, 31),
            created_at=datetime(2025, 5, 1, 12, 0, 0),
            updated_at=datetime(2025, 5, 15, 9, 30, 0)
        ),
        Campaign(
            id="c987654321",
            name="Winter Promotion",
            state=CampaignState.PAUSED,
            type=CampaignType.SPONSORED_BRANDS,
            daily_budget=75.0,
            start_date=date(2025, 11, 1),
            end_date=date(2025, 12, 31),
            created_at=datetime(2025, 10, 1, 10, 0, 0),
            updated_at=datetime(2025, 10, 5, 14, 20, 0)
        )
    ]
    
    # 過濾
    if state:
        campaigns = [c for c in campaigns if c.state == state]
    if type:
        campaigns = [c for c in campaigns if c.type == type]
    
    # 返回結果
    return {
        "items": campaigns[offset:offset+limit],
        "total": len(campaigns)
    }

@router.post(
    "/",
    response_model=Campaign,
    summary="創建廣告活動",
    description="創建新的廣告活動",
    responses={
        201: {
            "description": "廣告活動創建成功",
            "content": {
                "application/json": {
                    "example": {
                        "id": "c123456789",
                        "name": "Summer Sale 2025",
                        "state": "ENABLED",
                        "type": "SPONSORED_PRODUCTS",
                        "daily_budget": 50.0,
                        "start_date": "2025-06-01",
                        "end_date": "2025-07-31",
                        "created_at": "2025-05-01T12:00:00Z",
                        "updated_at": "2025-05-01T12:00:00Z"
                    }
                }
            }
        },
        400: {"description": "無效的請求數據"}
    }
)
async def create_campaign(
    campaign: CampaignCreate = Body(..., description="廣告活動創建數據")
):
    """
    創建新的廣告活動
    
    參數:
        campaign: 廣告活動創建數據
        
    返回:
        創建的廣告活動
    """
    # 這裡實際應用中會創建數據庫記錄，這裡僅作示例
    new_campaign = Campaign(
        id=f"c{datetime.now().timestamp():.0f}",
        name=campaign.name,
        state=campaign.state,
        type=campaign.type,
        daily_budget=campaign.daily_budget,
        start_date=campaign.start_date,
        end_date=campaign.end_date,
        created_at=datetime.now(),
        updated_at=datetime.now()
    )
    
    return new_campaign

@router.get(
    "/{campaign_id}",
    response_model=Campaign,
    summary="獲取廣告活動詳情",
    description="根據 ID 獲取單個廣告活動的詳細信息",
    responses={
        200: {
            "description": "廣告活動詳情",
            "content": {
                "application/json": {
                    "example": {
                        "id": "c123456789",
                        "name": "Summer Sale 2025",
                        "state": "ENABLED",
                        "type": "SPONSORED_PRODUCTS",
                        "daily_budget": 50.0,
                        "start_date": "2025-06-01",
                        "end_date": "2025-07-31",
                        "created_at": "2025-05-01T12:00:00Z",
                        "updated_at": "2025-05-15T09:30:00Z"
                    }
                }
            }
        },
        404: {"description": "找不到廣告活動"}
    }
)
async def get_campaign(
    campaign_id: str = Path(..., description="廣告活動 ID")
):
    """
    獲取廣告活動詳情
    
    參數:
        campaign_id: 廣告活動 ID
        
    返回:
        廣告活動詳情
    """
    # 這裡實際應用中會從資料庫查詢，這裡僅作示例
    if campaign_id == "c123456789":
        return Campaign(
            id="c123456789",
            name="Summer Sale 2025",
            state=CampaignState.ENABLED,
            type=CampaignType.SPONSORED_PRODUCTS,
            daily_budget=50.0,
            start_date=date(2025, 6, 1),
            end_date=date(2025, 7, 31),
            created_at=datetime(2025, 5, 1, 12, 0, 0),
            updated_at=datetime(2025, 5, 15, 9, 30, 0)
        )
    elif campaign_id == "c987654321":
        return Campaign(
            id="c987654321",
            name="Winter Promotion",
            state=CampaignState.PAUSED,
            type=CampaignType.SPONSORED_BRANDS,
            daily_budget=75.0,
            start_date=date(2025, 11, 1),
            end_date=date(2025, 12, 31),
            created_at=datetime(2025, 10, 1, 10, 0, 0),
            updated_at=datetime(2025, 10, 5, 14, 20, 0)
        )
    else:
        raise HTTPException(status_code=404, detail="Campaign not found")

@router.patch(
    "/{campaign_id}",
    response_model=Campaign,
    summary="更新廣告活動",
    description="更新現有廣告活動的部分字段",
    responses={
        200: {
            "description": "廣告活動更新成功",
            "content": {
                "application/json": {
                    "example": {
                        "id": "c123456789",
                        "name": "Updated Summer Sale 2025",
                        "state": "PAUSED",
                        "type": "SPONSORED_PRODUCTS",
                        "daily_budget": 75.0,
                        "start_date": "2025-06-01",
                        "end_date": "2025-07-31",
                        "created_at": "2025-05-01T12:00:00Z",
                        "updated_at": "2025-05-20T14:30:00Z"
                    }
                }
            }
        },
        404: {"description": "找不到廣告活動"},
        400: {"description": "無效的更新數據"}
    }
)
async def update_campaign(
    campaign_id: str = Path(..., description="廣告活動 ID"),
    campaign_update: CampaignUpdate = Body(..., description="廣告活動更新數據")
):
    """
    更新廣告活動
    
    參數:
        campaign_id: 廣告活動 ID
        campaign_update: 廣告活動更新數據
        
    返回:
        更新後的廣告活動
    """
    # 這裡實際應用中會更新資料庫記錄，這裡僅作示例
    if campaign_id not in ["c123456789", "c987654321"]:
        raise HTTPException(status_code=404, detail="Campaign not found")
    
    # 獲取現有廣告活動
    base_campaign = Campaign(
        id="c123456789",
        name="Summer Sale 2025",
        state=CampaignState.ENABLED,
        type=CampaignType.SPONSORED_PRODUCTS,
        daily_budget=50.0,
        start_date=date(2025, 6, 1),
        end_date=date(2025, 7, 31),
        created_at=datetime(2025, 5, 1, 12, 0, 0),
        updated_at=datetime(2025, 5, 15, 9, 30, 0)
    )
    
    # 應用更新
    updated_campaign = base_campaign.copy(
        update={
            k: v for k, v in campaign_update.dict().items() 
            if v is not None
        }
    )
    
    # 更新時間戳
    updated_campaign.updated_at = datetime.now()
    
    return updated_campaign

@router.delete(
    "/{campaign_id}",
    summary="刪除廣告活動",
    description="根據 ID 刪除廣告活動",
    responses={
        204: {"description": "廣告活動刪除成功"},
        404: {"description": "找不到廣告活動"}
    }
)
async def delete_campaign(
    campaign_id: str = Path(..., description="廣告活動 ID")
):
    """
    刪除廣告活動
    
    參數:
        campaign_id: 廣告活動 ID
        
    返回:
        無內容，狀態碼 204
    """
    # 這裡實際應用中會刪除資料庫記錄，這裡僅作示例
    if campaign_id not in ["c123456789", "c987654321"]:
        raise HTTPException(status_code=404, detail="Campaign not found")
    
    # 刪除成功，返回 204 No Content
    return None
