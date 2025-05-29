"""
Campaign Groups API Routes
Provides endpoints for managing campaign groups
"""
import logging
from typing import List, Optional
from fastapi import APIRouter, HTTPException, Query, Path, Depends

from ...models.schemas.campaign_groups import (
    CampaignGroupCreate,
    CampaignGroupUpdate,
    CampaignGroupResponse,
    CampaignGroupListResponse,
    CampaignAssignment
)
from ...services.campaign_groups import campaign_group_service

logger = logging.getLogger(__name__)

router = APIRouter(
    prefix="/campaign-groups",
    tags=["campaign-groups"],
    responses={
        404: {"description": "Not found"},
        401: {"description": "Unauthorized"},
        400: {"description": "Bad request"}
    }
)


@router.post("", response_model=CampaignGroupResponse, status_code=201)
async def create_campaign_group(
    group_data: CampaignGroupCreate,
    user_id: str = Query(..., description="User ID")
):
    """
    Create a new campaign group
    
    - **name**: Campaign group name (required)
    - **description**: Optional description
    - **target_acos**: Target ACoS percentage (0-100)
    - **preset_goal**: Optimization strategy (Balanced, Reduce ACoS, Increase Sales)
    - **bid_ceiling**: Maximum bid limit
    - **bid_floor**: Minimum bid limit
    """
    try:
        return await campaign_group_service.create_group(user_id, group_data)
    except Exception as e:
        logger.error(f"Error creating campaign group: {str(e)}")
        if "unique" in str(e).lower():
            raise HTTPException(status_code=400, detail="A campaign group with this name already exists")
        raise HTTPException(status_code=500, detail=str(e))


@router.get("", response_model=CampaignGroupListResponse)
async def get_campaign_groups(
    user_id: str = Query(..., description="User ID"),
    profile_id: Optional[int] = Query(None, description="Filter by profile ID")
):
    """
    Get all campaign groups for a user
    
    Returns a list of campaign groups with:
    - Campaign assignments
    - Total group count
    - Count of unassigned campaigns
    """
    try:
        return await campaign_group_service.get_user_groups(user_id, profile_id)
    except Exception as e:
        logger.error(f"Error getting campaign groups: {str(e)}")
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/{group_id}", response_model=CampaignGroupResponse)
async def get_campaign_group(
    group_id: int = Path(..., description="Campaign group ID"),
    user_id: str = Query(..., description="User ID")
):
    """
    Get a specific campaign group by ID
    
    Returns detailed information about the campaign group including assigned campaigns
    """
    try:
        group = await campaign_group_service.get_group_by_id(group_id, user_id)
        if not group:
            raise HTTPException(status_code=404, detail="Campaign group not found")
        return group
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error getting campaign group: {str(e)}")
        raise HTTPException(status_code=500, detail=str(e))


@router.put("/{group_id}", response_model=CampaignGroupResponse)
async def update_campaign_group(
    group_id: int = Path(..., description="Campaign group ID"),
    update_data: CampaignGroupUpdate = ...,
    user_id: str = Query(..., description="User ID")
):
    """
    Update a campaign group
    
    All fields are optional - only provided fields will be updated
    """
    try:
        updated_group = await campaign_group_service.update_group(group_id, user_id, update_data)
        if not updated_group:
            raise HTTPException(status_code=404, detail="Campaign group not found")
        return updated_group
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error updating campaign group: {str(e)}")
        if "unique" in str(e).lower():
            raise HTTPException(status_code=400, detail="A campaign group with this name already exists")
        raise HTTPException(status_code=500, detail=str(e))


@router.delete("/{group_id}", status_code=204)
async def delete_campaign_group(
    group_id: int = Path(..., description="Campaign group ID"),
    user_id: str = Query(..., description="User ID")
):
    """
    Delete a campaign group
    
    Campaigns in the group will be unassigned (not deleted)
    """
    try:
        success = await campaign_group_service.delete_group(group_id, user_id)
        if not success:
            raise HTTPException(status_code=404, detail="Campaign group not found")
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error deleting campaign group: {str(e)}")
        raise HTTPException(status_code=500, detail=str(e))


@router.post("/{group_id}/campaigns", status_code=204)
async def assign_campaigns_to_group(
    group_id: int = Path(..., description="Campaign group ID"),
    assignment: CampaignAssignment = ...,
    user_id: str = Query(..., description="User ID")
):
    """
    Assign campaigns to a campaign group
    
    Campaigns will be moved from their current group (if any) to this group
    """
    try:
        success = await campaign_group_service.assign_campaigns(group_id, user_id, assignment.campaign_ids)
        if not success:
            raise HTTPException(status_code=400, detail="Failed to assign campaigns")
    except Exception as e:
        logger.error(f"Error assigning campaigns: {str(e)}")
        if "not found" in str(e).lower():
            raise HTTPException(status_code=404, detail=str(e))
        raise HTTPException(status_code=500, detail=str(e))


@router.delete("/{group_id}/campaigns", status_code=204)
async def remove_campaigns_from_group(
    group_id: int = Path(..., description="Campaign group ID"),
    campaign_ids: List[str] = Query(..., description="Campaign IDs to remove"),
    user_id: str = Query(..., description="User ID")
):
    """
    Remove campaigns from a campaign group
    
    Campaigns will be unassigned from the group (not deleted)
    """
    try:
        success = await campaign_group_service.remove_campaigns_from_group(group_id, user_id, campaign_ids)
        if not success:
            raise HTTPException(status_code=400, detail="Failed to remove campaigns")
    except Exception as e:
        logger.error(f"Error removing campaigns: {str(e)}")
        if "not found" in str(e).lower():
            raise HTTPException(status_code=404, detail=str(e))
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/stats/unassigned-campaigns-count")
async def get_unassigned_campaigns_count(
    user_id: str = Query(..., description="User ID")
):
    """
    Get count of campaigns not assigned to any group
    """
    try:
        count = await campaign_group_service.get_unassigned_campaigns_count(user_id)
        return {"count": count}
    except Exception as e:
        logger.error(f"Error getting unassigned campaigns count: {str(e)}")
        raise HTTPException(status_code=500, detail=str(e))