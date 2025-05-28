"""
Campaign Groups Pydantic schemas
Defines data models for campaign group operations
"""
from pydantic import BaseModel, Field, field_validator
from typing import Optional, List
from datetime import datetime
from decimal import Decimal


class CampaignGroupBase(BaseModel):
    """Base campaign group model with common fields"""
    name: str = Field(..., min_length=1, max_length=255, description="Campaign group name")
    description: Optional[str] = Field(None, description="Optional description of the group")
    target_acos: Optional[Decimal] = Field(
        None, 
        ge=0, 
        le=100, 
        alias="targetAcos",
        description="Target Advertising Cost of Sales percentage (0-100)"
    )
    preset_goal: Optional[str] = Field(
        None, 
        alias="presetGoal",
        description="Optimization strategy: Balanced, Reduce ACoS, or Increase Sales"
    )
    bid_ceiling: Optional[Decimal] = Field(
        None, 
        ge=0, 
        alias="bidCeiling",
        description="Maximum bid limit"
    )
    bid_floor: Optional[Decimal] = Field(
        None, 
        ge=0, 
        alias="bidFloor",
        description="Minimum bid limit"
    )
    
    @field_validator('preset_goal')
    @classmethod
    def validate_preset_goal(cls, v: Optional[str]) -> Optional[str]:
        """Validate preset_goal is one of the allowed values"""
        if v is not None and v not in ['Balanced', 'Reduce ACoS', 'Increase Sales']:
            raise ValueError('preset_goal must be one of: Balanced, Reduce ACoS, Increase Sales')
        return v
    
    @field_validator('bid_floor', 'bid_ceiling')
    @classmethod
    def validate_bid_values(cls, v: Optional[Decimal], info) -> Optional[Decimal]:
        """Validate bid_floor is not greater than bid_ceiling"""
        if info.field_name == 'bid_ceiling' and v is not None:
            bid_floor = info.data.get('bid_floor')
            if bid_floor is not None and v < bid_floor:
                raise ValueError('bid_ceiling must be greater than or equal to bid_floor')
        return v

    class Config:
        populate_by_name = True


class CampaignGroupCreate(CampaignGroupBase):
    """Schema for creating a new campaign group"""
    pass


class CampaignGroupUpdate(BaseModel):
    """Schema for updating a campaign group"""
    name: Optional[str] = Field(None, min_length=1, max_length=255)
    description: Optional[str] = None
    target_acos: Optional[Decimal] = Field(None, ge=0, le=100, alias="targetAcos")
    preset_goal: Optional[str] = Field(None, alias="presetGoal")
    bid_ceiling: Optional[Decimal] = Field(None, ge=0, alias="bidCeiling")
    bid_floor: Optional[Decimal] = Field(None, ge=0, alias="bidFloor")
    
    @field_validator('preset_goal')
    @classmethod
    def validate_preset_goal(cls, v: Optional[str]) -> Optional[str]:
        """Validate preset_goal is one of the allowed values"""
        if v is not None and v not in ['Balanced', 'Reduce ACoS', 'Increase Sales']:
            raise ValueError('preset_goal must be one of: Balanced, Reduce ACoS, Increase Sales')
        return v

    class Config:
        populate_by_name = True


class CampaignGroupResponse(CampaignGroupBase):
    """Schema for campaign group API responses"""
    id: str = Field(..., description="Campaign group ID (as string for frontend compatibility)")
    user_id: str = Field(..., description="User ID who owns this group")
    campaigns: List[str] = Field(default_factory=list, description="List of campaign IDs in this group")
    created_at: str = Field(..., description="ISO format creation timestamp")
    updated_at: str = Field(..., description="ISO format last update timestamp")
    
    class Config:
        from_attributes = True
        populate_by_name = True

    @classmethod
    def from_db(cls, db_group: dict, campaign_ids: List[str] = None) -> "CampaignGroupResponse":
        """Create response from database record"""
        return cls(
            id=str(db_group['id']),
            name=db_group['name'],
            user_id=db_group['user_id'],
            description=db_group.get('description'),
            targetAcos=db_group.get('target_acos'),
            presetGoal=db_group.get('preset_goal'),
            bidCeiling=db_group.get('bid_ceiling'),
            bidFloor=db_group.get('bid_floor'),
            campaigns=campaign_ids or [],
            created_at=db_group['created_at'].isoformat() if isinstance(db_group['created_at'], datetime) else db_group['created_at'],
            updated_at=db_group['updated_at'].isoformat() if isinstance(db_group['updated_at'], datetime) else db_group['updated_at']
        )


class CampaignAssignment(BaseModel):
    """Schema for assigning campaigns to a group"""
    campaign_ids: List[str] = Field(..., min_items=1, description="List of campaign IDs to assign")
    
    class Config:
        json_schema_extra = {
            "example": {
                "campaign_ids": ["12345", "67890", "54321"]
            }
        }


class CampaignGroupListResponse(BaseModel):
    """Schema for listing campaign groups with metadata"""
    groups: List[CampaignGroupResponse]
    total: int = Field(..., description="Total number of groups")
    unassigned_campaigns_count: int = Field(..., description="Number of campaigns not in any group")
    
    class Config:
        json_schema_extra = {
            "example": {
                "groups": [
                    {
                        "id": "1",
                        "name": "High Performance Group",
                        "description": "Top performing campaigns",
                        "targetAcos": 15.5,
                        "presetGoal": "Reduce ACoS",
                        "bidCeiling": 5.00,
                        "bidFloor": 0.50,
                        "user_id": "user123",
                        "campaigns": ["12345", "67890"],
                        "created_at": "2024-01-01T00:00:00Z",
                        "updated_at": "2024-01-01T00:00:00Z"
                    }
                ],
                "total": 1,
                "unassigned_campaigns_count": 5
            }
        }