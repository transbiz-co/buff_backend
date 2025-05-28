"""
Campaign Groups Service
Handles business logic for campaign group operations
"""
import logging
from typing import List, Optional, Dict, Any
from datetime import datetime

from ..core.supabase import supabase
from ..models.schemas.campaign_groups import (
    CampaignGroupCreate,
    CampaignGroupUpdate,
    CampaignGroupResponse,
    CampaignGroupListResponse
)

logger = logging.getLogger(__name__)


class CampaignGroupService:
    """Service class for campaign group operations"""
    
    async def create_group(self, user_id: str, group_data: CampaignGroupCreate) -> CampaignGroupResponse:
        """
        Create a new campaign group
        
        Args:
            user_id: User ID creating the group
            group_data: Campaign group data
            
        Returns:
            Created campaign group
            
        Raises:
            Exception: If creation fails
        """
        try:
            # Prepare data for insertion
            insert_data = {
                'user_id': user_id,
                'name': group_data.name,
                'description': group_data.description,
                'target_acos': float(group_data.target_acos) if group_data.target_acos else None,
                'preset_goal': group_data.preset_goal,
                'bid_ceiling': float(group_data.bid_ceiling) if group_data.bid_ceiling else None,
                'bid_floor': float(group_data.bid_floor) if group_data.bid_floor else None
            }
            
            # Insert into database
            result = supabase.table('campaign_groups').insert(insert_data).execute()
            
            if not result.data:
                raise Exception("Failed to create campaign group")
            
            created_group = result.data[0]
            logger.info(f"Created campaign group {created_group['id']} for user {user_id}")
            
            return CampaignGroupResponse.from_db(created_group)
            
        except Exception as e:
            logger.error(f"Error creating campaign group: {str(e)}")
            raise
    
    async def get_user_groups(self, user_id: str) -> CampaignGroupListResponse:
        """
        Get all campaign groups for a user with campaign assignments
        
        Args:
            user_id: User ID
            
        Returns:
            List of campaign groups with metadata
        """
        try:
            # Get all groups for the user
            groups_result = supabase.table('campaign_groups').select('*').eq('user_id', user_id).order('created_at', desc=True).limit(10000).execute()
            
            groups = []
            for group_data in groups_result.data:
                # Get campaigns assigned to this group
                campaigns_result = supabase.table('amazon_ads_campaigns').select('campaign_id').eq('group_id', group_data['id']).limit(10000).execute()
                campaign_ids = [str(c['campaign_id']) for c in campaigns_result.data]
                
                groups.append(CampaignGroupResponse.from_db(group_data, campaign_ids))
            
            # Count unassigned campaigns
            unassigned_result = supabase.table('amazon_ads_campaigns').select('campaign_id', count='exact').is_('group_id', 'null').execute()
            unassigned_count = unassigned_result.count or 0
            
            return CampaignGroupListResponse(
                groups=groups,
                total=len(groups),
                unassigned_campaigns_count=unassigned_count
            )
            
        except Exception as e:
            logger.error(f"Error getting user groups: {str(e)}")
            raise
    
    async def get_group_by_id(self, group_id: int, user_id: str) -> Optional[CampaignGroupResponse]:
        """
        Get a specific campaign group by ID
        
        Args:
            group_id: Campaign group ID
            user_id: User ID (for authorization)
            
        Returns:
            Campaign group if found and authorized, None otherwise
        """
        try:
            # Get group with user check
            result = supabase.table('campaign_groups').select('*').eq('id', group_id).eq('user_id', user_id).execute()
            
            if not result.data:
                return None
            
            group_data = result.data[0]
            
            # Get campaigns for this group
            campaigns_result = supabase.table('amazon_ads_campaigns').select('campaign_id').eq('group_id', group_id).limit(10000).execute()
            campaign_ids = [str(c['campaign_id']) for c in campaigns_result.data]
            
            return CampaignGroupResponse.from_db(group_data, campaign_ids)
            
        except Exception as e:
            logger.error(f"Error getting group by ID: {str(e)}")
            raise
    
    async def update_group(self, group_id: int, user_id: str, update_data: CampaignGroupUpdate) -> Optional[CampaignGroupResponse]:
        """
        Update a campaign group
        
        Args:
            group_id: Campaign group ID
            user_id: User ID (for authorization)
            update_data: Update data
            
        Returns:
            Updated campaign group if successful, None if not found
        """
        try:
            # Check if group exists and belongs to user
            existing = await self.get_group_by_id(group_id, user_id)
            if not existing:
                return None
            
            # Prepare update data (only non-None values)
            update_dict = {}
            if update_data.name is not None:
                update_dict['name'] = update_data.name
            if update_data.description is not None:
                update_dict['description'] = update_data.description
            if update_data.target_acos is not None:
                update_dict['target_acos'] = float(update_data.target_acos)
            if update_data.preset_goal is not None:
                update_dict['preset_goal'] = update_data.preset_goal
            if update_data.bid_ceiling is not None:
                update_dict['bid_ceiling'] = float(update_data.bid_ceiling)
            if update_data.bid_floor is not None:
                update_dict['bid_floor'] = float(update_data.bid_floor)
            
            # Perform update
            result = supabase.table('campaign_groups').update(update_dict).eq('id', group_id).eq('user_id', user_id).execute()
            
            if not result.data:
                return None
            
            # Get updated campaigns
            campaigns_result = supabase.table('amazon_ads_campaigns').select('campaign_id').eq('group_id', group_id).limit(10000).execute()
            campaign_ids = [str(c['campaign_id']) for c in campaigns_result.data]
            
            logger.info(f"Updated campaign group {group_id}")
            
            return CampaignGroupResponse.from_db(result.data[0], campaign_ids)
            
        except Exception as e:
            logger.error(f"Error updating group: {str(e)}")
            raise
    
    async def delete_group(self, group_id: int, user_id: str) -> bool:
        """
        Delete a campaign group (campaigns will have group_id set to NULL)
        
        Args:
            group_id: Campaign group ID
            user_id: User ID (for authorization)
            
        Returns:
            True if deleted, False if not found
        """
        try:
            # Check if group exists and belongs to user
            existing = await self.get_group_by_id(group_id, user_id)
            if not existing:
                return False
            
            # Delete the group (campaigns will be unassigned due to ON DELETE SET NULL)
            result = supabase.table('campaign_groups').delete().eq('id', group_id).eq('user_id', user_id).execute()
            
            logger.info(f"Deleted campaign group {group_id}")
            
            return True
            
        except Exception as e:
            logger.error(f"Error deleting group: {str(e)}")
            raise
    
    async def assign_campaigns(self, group_id: int, user_id: str, campaign_ids: List[str]) -> bool:
        """
        Assign campaigns to a group
        
        Args:
            group_id: Campaign group ID
            user_id: User ID (for authorization)
            campaign_ids: List of campaign IDs to assign
            
        Returns:
            True if successful
        """
        try:
            # Check if group exists and belongs to user
            existing = await self.get_group_by_id(group_id, user_id)
            if not existing:
                raise Exception(f"Group {group_id} not found or unauthorized")
            
            # Update campaigns to assign them to the group
            result = supabase.table('amazon_ads_campaigns').update({'group_id': group_id}).in_('campaign_id', campaign_ids).execute()
            
            logger.info(f"Assigned {len(campaign_ids)} campaigns to group {group_id}")
            
            return True
            
        except Exception as e:
            logger.error(f"Error assigning campaigns: {str(e)}")
            raise
    
    async def remove_campaigns_from_group(self, group_id: int, user_id: str, campaign_ids: List[str]) -> bool:
        """
        Remove campaigns from a group
        
        Args:
            group_id: Campaign group ID
            user_id: User ID (for authorization)
            campaign_ids: List of campaign IDs to remove
            
        Returns:
            True if successful
        """
        try:
            # Check if group exists and belongs to user
            existing = await self.get_group_by_id(group_id, user_id)
            if not existing:
                raise Exception(f"Group {group_id} not found or unauthorized")
            
            # Update campaigns to remove them from the group
            result = supabase.table('amazon_ads_campaigns').update({'group_id': None}).eq('group_id', group_id).in_('campaign_id', campaign_ids).execute()
            
            logger.info(f"Removed {len(campaign_ids)} campaigns from group {group_id}")
            
            return True
            
        except Exception as e:
            logger.error(f"Error removing campaigns: {str(e)}")
            raise
    
    async def get_unassigned_campaigns_count(self, user_id: str) -> int:
        """
        Get count of campaigns not assigned to any group
        
        Args:
            user_id: User ID
            
        Returns:
            Count of unassigned campaigns
        """
        try:
            # For now, count all campaigns without a group_id
            # In production, you might want to filter by user's campaigns
            result = supabase.table('amazon_ads_campaigns').select('campaign_id', count='exact').is_('group_id', 'null').execute()
            
            return result.count or 0
            
        except Exception as e:
            logger.error(f"Error counting unassigned campaigns: {str(e)}")
            raise


# Create singleton instance
campaign_group_service = CampaignGroupService()