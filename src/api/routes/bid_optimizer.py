"""
Bid Optimizer API endpoints
用於支援前端 Bid Optimizer 頁面的數據需求
"""
import logging
from datetime import datetime, timedelta
from typing import Dict, Any, List, Optional
from fastapi import APIRouter, HTTPException, Query
from pydantic import BaseModel, Field
from decimal import Decimal

from ...services.amazon_ads import supabase

logger = logging.getLogger(__name__)

router = APIRouter(prefix="/bid-optimizer", tags=["bid-optimizer"])


class MetricSummary(BaseModel):
    """單一時期的指標摘要"""
    impressions: int = 0
    clicks: int = 0
    orders: int = 0
    units: int = 0
    spend: Decimal = Decimal("0.00")
    sales: Decimal = Decimal("0.00")
    acos: Optional[Decimal] = None
    ctr: Optional[Decimal] = None
    cvr: Optional[Decimal] = None
    cpc: Optional[Decimal] = None
    roas: Optional[Decimal] = None
    rpc: Optional[Decimal] = None


class SummaryData(BaseModel):
    """總計統計數據，包含當期、前期和變化"""
    current: MetricSummary
    previous: MetricSummary
    changes: Dict[str, Optional[str]]  # 變化百分比


class DailyPerformance(BaseModel):
    """每日效能數據"""
    date: str
    impressions: int = 0
    clicks: int = 0
    orders: int = 0
    units: int = 0
    spend: Decimal = Decimal("0.00")
    sales: Decimal = Decimal("0.00")
    acos: Optional[Decimal] = None
    ctr: Optional[Decimal] = None
    cvr: Optional[Decimal] = None
    cpc: Optional[Decimal] = None
    roas: Optional[Decimal] = None
    rpc: Optional[Decimal] = None


class CampaignData(BaseModel):
    """Campaign 詳細數據"""
    id: str = Field(..., alias="campaignId")
    campaign: str = Field(..., alias="campaignName")
    adType: str  # SP/SB/SD
    state: str = Field(..., alias="campaignStatus")
    startDate: Optional[str] = None
    endDate: Optional[str] = None
    optGroup: Optional[str] = None  # Campaign Group
    lastOptimized: Optional[str] = None
    impressions: int = 0
    clicks: int = 0
    orders: int = 0
    units: int = 0
    ctr: Optional[Decimal] = None
    cvr: Optional[Decimal] = None
    cpc: Optional[Decimal] = None
    spend: Decimal = Decimal("0.00")
    sales: Decimal = Decimal("0.00")
    acos: Optional[Decimal] = None
    rpc: Optional[Decimal] = None
    roas: Optional[Decimal] = None
    # 趨勢數據（可選）
    salesTrend: Optional[Decimal] = None
    spendTrend: Optional[Decimal] = None

    class Config:
        populate_by_name = True


class BidOptimizerResponse(BaseModel):
    """Bid Optimizer API 回應格式"""
    summary: SummaryData
    daily_performance: List[DailyPerformance]
    campaigns: List[CampaignData]


def calculate_metrics(data: Dict[str, Any]) -> Dict[str, Any]:
    """計算衍生指標"""
    impressions = data.get("impressions", 0) or 0
    clicks = data.get("clicks", 0) or 0
    orders = data.get("orders", 0) or 0
    units = data.get("units", 0) or 0
    cost = Decimal(str(data.get("cost", 0) or 0))
    sales = Decimal(str(data.get("sales", 0) or 0))
    
    # 計算衍生指標，處理除零情況
    acos = (cost / sales * 100) if sales > 0 else None
    ctr = (Decimal(clicks) / Decimal(impressions) * 100) if impressions > 0 else None
    cvr = (Decimal(orders) / Decimal(clicks) * 100) if clicks > 0 else None
    cpc = (cost / Decimal(clicks)) if clicks > 0 else None
    roas = (sales / cost) if cost > 0 else None
    rpc = (sales / Decimal(clicks)) if clicks > 0 else None
    
    return {
        "impressions": impressions,
        "clicks": clicks,
        "orders": orders,
        "units": units,
        "spend": cost,
        "sales": sales,
        "acos": round(acos, 2) if acos is not None else None,
        "ctr": round(ctr, 2) if ctr is not None else None,
        "cvr": round(cvr, 2) if cvr is not None else None,
        "cpc": round(cpc, 2) if cpc is not None else None,
        "roas": round(roas, 2) if roas is not None else None,
        "rpc": round(rpc, 2) if rpc is not None else None
    }


def calculate_change_percentage(current: Decimal, previous: Decimal) -> Optional[str]:
    """計算變化百分比"""
    if previous == 0:
        return None if current == 0 else "+∞"
    
    change = ((current - previous) / previous * 100)
    sign = "+" if change > 0 else ""
    return f"{sign}{round(change, 1)}%"


def build_filter_clause(filters: Optional[Dict[str, Any]]) -> tuple[str, Dict[str, Any]]:
    """構建篩選條件的 SQL 子句"""
    where_clauses = []
    params = {}
    
    if not filters:
        return "", params
    
    # Campaign name 篩選
    if "campaign" in filters:
        campaign_filter = filters["campaign"]
        if campaign_filter.get("operator") == "contains":
            where_clauses.append('"campaignName" ILIKE %(campaign_name)s')
            params["campaign_name"] = f"%{campaign_filter.get('value', '')}%"
        elif campaign_filter.get("operator") == "equals":
            where_clauses.append('"campaignName" = %(campaign_name)s')
            params["campaign_name"] = campaign_filter.get("value", "")
    
    # Ad type 篩選
    if "adType" in filters and filters["adType"]:
        ad_types = filters["adType"] if isinstance(filters["adType"], list) else [filters["adType"]]
        # 這個篩選會在 UNION 查詢外層處理
        params["ad_types"] = ad_types
    
    # State 篩選
    if "state" in filters and filters["state"]:
        states = filters["state"] if isinstance(filters["state"], list) else [filters["state"]]
        state_placeholders = [f"%(state_{i})s" for i in range(len(states))]
        where_clauses.append(f'"campaignStatus" IN ({",".join(state_placeholders)})')
        for i, state in enumerate(states):
            params[f"state_{i}"] = state
    
    # 數值篩選
    numeric_filters = ["impressions", "clicks", "spend", "sales", "acos"]
    for field in numeric_filters:
        if field in filters:
            filter_data = filters[field]
            db_field = "cost" if field == "spend" else field
            
            if filter_data.get("operator") == "greater_than":
                where_clauses.append(f'"{db_field}" > %({field}_value)s')
                params[f"{field}_value"] = filter_data.get("value", 0)
            elif filter_data.get("operator") == "less_than":
                where_clauses.append(f'"{db_field}" < %({field}_value)s')
                params[f"{field}_value"] = filter_data.get("value", 0)
            elif filter_data.get("operator") == "between":
                where_clauses.append(f'"{db_field}" BETWEEN %({field}_min)s AND %({field}_max)s')
                params[f"{field}_min"] = filter_data.get("min", 0)
                params[f"{field}_max"] = filter_data.get("max", 0)
    
    where_sql = " AND " + " AND ".join(where_clauses) if where_clauses else ""
    return where_sql, params


@router.get("", response_model=BidOptimizerResponse)
async def get_bid_optimizer_data(
    profile_id: str = Query(..., description="Amazon Ads Profile ID"),
    start_date: str = Query(..., description="開始日期 (YYYY-MM-DD)"),
    end_date: str = Query(..., description="結束日期 (YYYY-MM-DD)"),
    filters: Optional[str] = Query(None, description="篩選條件 (JSON 格式)")
) -> BidOptimizerResponse:
    """
    獲取 Bid Optimizer 頁面所需的完整數據
    
    包含：
    1. 總計統計數據（當期 vs 前期）
    2. 每日效能趨勢
    3. Campaign 列表詳細數據
    """
    try:
        # 解析篩選條件
        import json
        filter_dict = json.loads(filters) if filters else {}
        
        # 計算前期日期範圍
        start_dt = datetime.strptime(start_date, "%Y-%m-%d")
        end_dt = datetime.strptime(end_date, "%Y-%m-%d")
        date_diff = (end_dt - start_dt).days + 1
        
        prev_start_date = (start_dt - timedelta(days=date_diff)).strftime("%Y-%m-%d")
        prev_end_date = (start_dt - timedelta(days=1)).strftime("%Y-%m-%d")
        
        # 構建篩選條件
        where_clause, filter_params = build_filter_clause(filter_dict)
        
        # 1. 獲取總計數據 - 分別查詢三個表
        current_data = {"impressions": 0, "clicks": 0, "orders": 0, "units": 0, "cost": 0, "sales": 0}
        previous_data = {"impressions": 0, "clicks": 0, "orders": 0, "units": 0, "cost": 0, "sales": 0}
        
        # 查詢 SP 數據
        sp_query = supabase.table('amazon_ads_campaigns_reports_sp').select(
            'impressions, clicks, purchases7d, unitsSoldClicks7d, cost, sales7d, campaignName, campaignStatus, date'
        ).eq('profile_id', profile_id).gte('date', prev_start_date).lte('date', end_date)
        
        # 應用篩選條件
        if filter_dict.get('campaign', {}).get('operator') == 'contains':
            sp_query = sp_query.ilike('campaignName', f"%{filter_dict['campaign']['value']}%")
        elif filter_dict.get('campaign', {}).get('operator') == 'equals':
            sp_query = sp_query.eq('campaignName', filter_dict['campaign']['value'])
            
        if filter_dict.get('state'):
            states = filter_dict['state'] if isinstance(filter_dict['state'], list) else [filter_dict['state']]
            sp_query = sp_query.in_('campaignStatus', states)
        
        sp_result = sp_query.execute()
        
        # 處理 SP 數據
        for row in sp_result.data:
            row_date = datetime.strptime(row['date'], "%Y-%m-%d")
            data_target = current_data if row_date >= start_dt else previous_data
            
            data_target['impressions'] += row.get('impressions', 0) or 0
            data_target['clicks'] += row.get('clicks', 0) or 0
            data_target['orders'] += row.get('purchases7d', 0) or 0
            data_target['units'] += row.get('unitsSoldClicks7d', 0) or 0
            data_target['cost'] += float(row.get('cost', 0) or 0)
            data_target['sales'] += float(row.get('sales7d', 0) or 0)
        
        # 查詢 SB 數據
        if not filter_dict.get('adType') or 'SB' in filter_dict.get('adType', []):
            sb_query = supabase.table('amazon_ads_campaigns_reports_sb').select(
                'impressions, clicks, purchases, unitsSold, cost, sales, campaignName, campaignStatus, date'
            ).eq('profile_id', profile_id).gte('date', prev_start_date).lte('date', end_date)
            
            # 應用篩選條件
            if filter_dict.get('campaign', {}).get('operator') == 'contains':
                sb_query = sb_query.ilike('campaignName', f"%{filter_dict['campaign']['value']}%")
            elif filter_dict.get('campaign', {}).get('operator') == 'equals':
                sb_query = sb_query.eq('campaignName', filter_dict['campaign']['value'])
                
            if filter_dict.get('state'):
                states = filter_dict['state'] if isinstance(filter_dict['state'], list) else [filter_dict['state']]
                sb_query = sb_query.in_('campaignStatus', states)
            
            sb_result = sb_query.execute()
            
            # 處理 SB 數據
            for row in sb_result.data:
                row_date = datetime.strptime(row['date'], "%Y-%m-%d")
                data_target = current_data if row_date >= start_dt else previous_data
                
                data_target['impressions'] += row.get('impressions', 0) or 0
                data_target['clicks'] += row.get('clicks', 0) or 0
                data_target['orders'] += row.get('purchases', 0) or 0
                data_target['units'] += row.get('unitsSold', 0) or 0
                data_target['cost'] += float(row.get('cost', 0) or 0)
                data_target['sales'] += float(row.get('sales', 0) or 0)
        
        # 查詢 SD 數據
        if not filter_dict.get('adType') or 'SD' in filter_dict.get('adType', []):
            sd_query = supabase.table('amazon_ads_campaigns_reports_sd').select(
                'impressions, clicks, purchases, unitsSold, cost, sales, campaignName, campaignStatus, date'
            ).eq('profile_id', profile_id).gte('date', prev_start_date).lte('date', end_date)
            
            # 應用篩選條件
            if filter_dict.get('campaign', {}).get('operator') == 'contains':
                sd_query = sd_query.ilike('campaignName', f"%{filter_dict['campaign']['value']}%")
            elif filter_dict.get('campaign', {}).get('operator') == 'equals':
                sd_query = sd_query.eq('campaignName', filter_dict['campaign']['value'])
                
            if filter_dict.get('state'):
                states = filter_dict['state'] if isinstance(filter_dict['state'], list) else [filter_dict['state']]
                sd_query = sd_query.in_('campaignStatus', states)
            
            sd_result = sd_query.execute()
            
            # 處理 SD 數據
            for row in sd_result.data:
                row_date = datetime.strptime(row['date'], "%Y-%m-%d")
                data_target = current_data if row_date >= start_dt else previous_data
                
                data_target['impressions'] += row.get('impressions', 0) or 0
                data_target['clicks'] += row.get('clicks', 0) or 0
                data_target['orders'] += row.get('purchases', 0) or 0
                data_target['units'] += row.get('unitsSold', 0) or 0
                data_target['cost'] += float(row.get('cost', 0) or 0)
                data_target['sales'] += float(row.get('sales', 0) or 0)
        
        # 計算指標
        current_metrics = calculate_metrics(current_data)
        previous_metrics = calculate_metrics(previous_data)
        
        # 計算變化百分比
        changes = {}
        for key in current_metrics.keys():
            if key in ["impressions", "clicks", "orders", "units", "spend", "sales", "acos", "ctr", "cvr", "cpc", "roas", "rpc"]:
                current_val = current_metrics.get(key, 0) or 0
                previous_val = previous_metrics.get(key, 0) or 0
                changes[key] = calculate_change_percentage(Decimal(str(current_val)), Decimal(str(previous_val)))
        
        # 2. 獲取每日效能數據
        daily_data = {}
        
        # 處理 SP 每日數據
        if not filter_dict.get('adType') or 'SP' in filter_dict.get('adType', []):
            sp_daily_query = supabase.table('amazon_ads_campaigns_reports_sp').select(
                'date, impressions, clicks, purchases7d, unitsSoldClicks7d, cost, sales7d'
            ).eq('profile_id', profile_id).gte('date', start_date).lte('date', end_date)
            
            # 應用篩選條件
            if filter_dict.get('campaign', {}).get('operator') == 'contains':
                sp_daily_query = sp_daily_query.ilike('campaignName', f"%{filter_dict['campaign']['value']}%")
            elif filter_dict.get('campaign', {}).get('operator') == 'equals':
                sp_daily_query = sp_daily_query.eq('campaignName', filter_dict['campaign']['value'])
                
            if filter_dict.get('state'):
                states = filter_dict['state'] if isinstance(filter_dict['state'], list) else [filter_dict['state']]
                sp_daily_query = sp_daily_query.in_('campaignStatus', states)
            
            sp_daily_result = sp_daily_query.execute()
            
            for row in sp_daily_result.data:
                date = row['date']
                if date not in daily_data:
                    daily_data[date] = {"impressions": 0, "clicks": 0, "orders": 0, "units": 0, "cost": 0, "sales": 0}
                
                daily_data[date]['impressions'] += row.get('impressions', 0) or 0
                daily_data[date]['clicks'] += row.get('clicks', 0) or 0
                daily_data[date]['orders'] += row.get('purchases7d', 0) or 0
                daily_data[date]['units'] += row.get('unitsSoldClicks7d', 0) or 0
                daily_data[date]['cost'] += float(row.get('cost', 0) or 0)
                daily_data[date]['sales'] += float(row.get('sales7d', 0) or 0)
        
        # 處理 SB 每日數據
        if not filter_dict.get('adType') or 'SB' in filter_dict.get('adType', []):
            sb_daily_query = supabase.table('amazon_ads_campaigns_reports_sb').select(
                'date, impressions, clicks, purchases, unitsSold, cost, sales'
            ).eq('profile_id', profile_id).gte('date', start_date).lte('date', end_date)
            
            # 應用篩選條件
            if filter_dict.get('campaign', {}).get('operator') == 'contains':
                sb_daily_query = sb_daily_query.ilike('campaignName', f"%{filter_dict['campaign']['value']}%")
            elif filter_dict.get('campaign', {}).get('operator') == 'equals':
                sb_daily_query = sb_daily_query.eq('campaignName', filter_dict['campaign']['value'])
                
            if filter_dict.get('state'):
                states = filter_dict['state'] if isinstance(filter_dict['state'], list) else [filter_dict['state']]
                sb_daily_query = sb_daily_query.in_('campaignStatus', states)
            
            sb_daily_result = sb_daily_query.execute()
            
            for row in sb_daily_result.data:
                date = row['date']
                if date not in daily_data:
                    daily_data[date] = {"impressions": 0, "clicks": 0, "orders": 0, "units": 0, "cost": 0, "sales": 0}
                
                daily_data[date]['impressions'] += row.get('impressions', 0) or 0
                daily_data[date]['clicks'] += row.get('clicks', 0) or 0
                daily_data[date]['orders'] += row.get('purchases', 0) or 0
                daily_data[date]['units'] += row.get('unitsSold', 0) or 0
                daily_data[date]['cost'] += float(row.get('cost', 0) or 0)
                daily_data[date]['sales'] += float(row.get('sales', 0) or 0)
        
        # 處理 SD 每日數據
        if not filter_dict.get('adType') or 'SD' in filter_dict.get('adType', []):
            sd_daily_query = supabase.table('amazon_ads_campaigns_reports_sd').select(
                'date, impressions, clicks, purchases, unitsSold, cost, sales'
            ).eq('profile_id', profile_id).gte('date', start_date).lte('date', end_date)
            
            # 應用篩選條件
            if filter_dict.get('campaign', {}).get('operator') == 'contains':
                sd_daily_query = sd_daily_query.ilike('campaignName', f"%{filter_dict['campaign']['value']}%")
            elif filter_dict.get('campaign', {}).get('operator') == 'equals':
                sd_daily_query = sd_daily_query.eq('campaignName', filter_dict['campaign']['value'])
                
            if filter_dict.get('state'):
                states = filter_dict['state'] if isinstance(filter_dict['state'], list) else [filter_dict['state']]
                sd_daily_query = sd_daily_query.in_('campaignStatus', states)
            
            sd_daily_result = sd_daily_query.execute()
            
            for row in sd_daily_result.data:
                date = row['date']
                if date not in daily_data:
                    daily_data[date] = {"impressions": 0, "clicks": 0, "orders": 0, "units": 0, "cost": 0, "sales": 0}
                
                daily_data[date]['impressions'] += row.get('impressions', 0) or 0
                daily_data[date]['clicks'] += row.get('clicks', 0) or 0
                daily_data[date]['orders'] += row.get('purchases', 0) or 0
                daily_data[date]['units'] += row.get('unitsSold', 0) or 0
                daily_data[date]['cost'] += float(row.get('cost', 0) or 0)
                daily_data[date]['sales'] += float(row.get('sales', 0) or 0)
        
        # 轉換每日數據為列表
        daily_performance = []
        for date in sorted(daily_data.keys()):
            metrics = calculate_metrics(daily_data[date])
            daily_performance.append(DailyPerformance(
                date=date,
                **metrics
            ))
        
        # 3. 獲取 Campaign 列表數據
        campaigns_data = {}
        
        # 處理 SP campaigns
        if not filter_dict.get('adType') or 'SP' in filter_dict.get('adType', []):
            sp_campaign_query = supabase.table('amazon_ads_campaigns_reports_sp').select(
                'campaignId, campaignName, campaignStatus, impressions, clicks, purchases7d, unitsSoldClicks7d, cost, sales7d'
            ).eq('profile_id', profile_id).gte('date', start_date).lte('date', end_date)
            
            # 應用篩選條件
            if filter_dict.get('campaign', {}).get('operator') == 'contains':
                sp_campaign_query = sp_campaign_query.ilike('campaignName', f"%{filter_dict['campaign']['value']}%")
            elif filter_dict.get('campaign', {}).get('operator') == 'equals':
                sp_campaign_query = sp_campaign_query.eq('campaignName', filter_dict['campaign']['value'])
                
            if filter_dict.get('state'):
                states = filter_dict['state'] if isinstance(filter_dict['state'], list) else [filter_dict['state']]
                sp_campaign_query = sp_campaign_query.in_('campaignStatus', states)
            
            sp_campaign_result = sp_campaign_query.execute()
            
            for row in sp_campaign_result.data:
                campaign_id = row['campaignId']
                if campaign_id not in campaigns_data:
                    campaigns_data[campaign_id] = {
                        'campaignId': campaign_id,
                        'campaignName': row['campaignName'],
                        'campaignStatus': row['campaignStatus'],
                        'ad_type': 'SP',
                        'impressions': 0,
                        'clicks': 0,
                        'orders': 0,
                        'units': 0,
                        'cost': 0,
                        'sales': 0
                    }
                
                campaigns_data[campaign_id]['impressions'] += row.get('impressions', 0) or 0
                campaigns_data[campaign_id]['clicks'] += row.get('clicks', 0) or 0
                campaigns_data[campaign_id]['orders'] += row.get('purchases7d', 0) or 0
                campaigns_data[campaign_id]['units'] += row.get('unitsSoldClicks7d', 0) or 0
                campaigns_data[campaign_id]['cost'] += float(row.get('cost', 0) or 0)
                campaigns_data[campaign_id]['sales'] += float(row.get('sales7d', 0) or 0)
        
        # 處理 SB campaigns
        if not filter_dict.get('adType') or 'SB' in filter_dict.get('adType', []):
            sb_campaign_query = supabase.table('amazon_ads_campaigns_reports_sb').select(
                'campaignId, campaignName, campaignStatus, impressions, clicks, purchases, unitsSold, cost, sales'
            ).eq('profile_id', profile_id).gte('date', start_date).lte('date', end_date)
            
            # 應用篩選條件
            if filter_dict.get('campaign', {}).get('operator') == 'contains':
                sb_campaign_query = sb_campaign_query.ilike('campaignName', f"%{filter_dict['campaign']['value']}%")
            elif filter_dict.get('campaign', {}).get('operator') == 'equals':
                sb_campaign_query = sb_campaign_query.eq('campaignName', filter_dict['campaign']['value'])
                
            if filter_dict.get('state'):
                states = filter_dict['state'] if isinstance(filter_dict['state'], list) else [filter_dict['state']]
                sb_campaign_query = sb_campaign_query.in_('campaignStatus', states)
            
            sb_campaign_result = sb_campaign_query.execute()
            
            for row in sb_campaign_result.data:
                campaign_id = row['campaignId']
                if campaign_id not in campaigns_data:
                    campaigns_data[campaign_id] = {
                        'campaignId': campaign_id,
                        'campaignName': row['campaignName'],
                        'campaignStatus': row['campaignStatus'],
                        'ad_type': 'SB',
                        'impressions': 0,
                        'clicks': 0,
                        'orders': 0,
                        'units': 0,
                        'cost': 0,
                        'sales': 0
                    }
                
                campaigns_data[campaign_id]['impressions'] += row.get('impressions', 0) or 0
                campaigns_data[campaign_id]['clicks'] += row.get('clicks', 0) or 0
                campaigns_data[campaign_id]['orders'] += row.get('purchases', 0) or 0
                campaigns_data[campaign_id]['units'] += row.get('unitsSold', 0) or 0
                campaigns_data[campaign_id]['cost'] += float(row.get('cost', 0) or 0)
                campaigns_data[campaign_id]['sales'] += float(row.get('sales', 0) or 0)
        
        # 處理 SD campaigns
        if not filter_dict.get('adType') or 'SD' in filter_dict.get('adType', []):
            sd_campaign_query = supabase.table('amazon_ads_campaigns_reports_sd').select(
                'campaignId, campaignName, campaignStatus, impressions, clicks, purchases, unitsSold, cost, sales'
            ).eq('profile_id', profile_id).gte('date', start_date).lte('date', end_date)
            
            # 應用篩選條件
            if filter_dict.get('campaign', {}).get('operator') == 'contains':
                sd_campaign_query = sd_campaign_query.ilike('campaignName', f"%{filter_dict['campaign']['value']}%")
            elif filter_dict.get('campaign', {}).get('operator') == 'equals':
                sd_campaign_query = sd_campaign_query.eq('campaignName', filter_dict['campaign']['value'])
                
            if filter_dict.get('state'):
                states = filter_dict['state'] if isinstance(filter_dict['state'], list) else [filter_dict['state']]
                sd_campaign_query = sd_campaign_query.in_('campaignStatus', states)
            
            sd_campaign_result = sd_campaign_query.execute()
            
            for row in sd_campaign_result.data:
                campaign_id = row['campaignId']
                if campaign_id not in campaigns_data:
                    campaigns_data[campaign_id] = {
                        'campaignId': campaign_id,
                        'campaignName': row['campaignName'],
                        'campaignStatus': row['campaignStatus'],
                        'ad_type': 'SD',
                        'impressions': 0,
                        'clicks': 0,
                        'orders': 0,
                        'units': 0,
                        'cost': 0,
                        'sales': 0
                    }
                
                campaigns_data[campaign_id]['impressions'] += row.get('impressions', 0) or 0
                campaigns_data[campaign_id]['clicks'] += row.get('clicks', 0) or 0
                campaigns_data[campaign_id]['orders'] += row.get('purchases', 0) or 0
                campaigns_data[campaign_id]['units'] += row.get('unitsSold', 0) or 0
                campaigns_data[campaign_id]['cost'] += float(row.get('cost', 0) or 0)
                campaigns_data[campaign_id]['sales'] += float(row.get('sales', 0) or 0)
        
        # 轉換 campaign 數據為列表
        campaigns = []
        for campaign_data in campaigns_data.values():
            metrics = calculate_metrics(campaign_data)
            campaign = CampaignData(
                campaignId=campaign_data['campaignId'],
                campaignName=campaign_data['campaignName'],
                adType=campaign_data['ad_type'],
                campaignStatus=campaign_data['campaignStatus'],
                **metrics
            )
            campaigns.append(campaign)
        
        # 按 campaign name 排序
        campaigns.sort(key=lambda x: x.campaign)
        
        # 組裝回應
        response = BidOptimizerResponse(
            summary=SummaryData(
                current=MetricSummary(**current_metrics),
                previous=MetricSummary(**previous_metrics),
                changes=changes
            ),
            daily_performance=daily_performance,
            campaigns=campaigns
        )
        
        return response
        
    except Exception as e:
        logger.error(f"Error in get_bid_optimizer_data: {str(e)}")
        import traceback
        logger.error(traceback.format_exc())
        raise HTTPException(status_code=500, detail=str(e)) 