"""
Bid Optimizer API endpoints
用於支援前端 Bid Optimizer 頁面的數據需求

優化更新 (2025-05-27):
- 整合 amazon_ads_daily_summary 聚合表以提升查詢性能
- Summary 和 Daily Performance 數據優先使用聚合表
- 當有 campaign 名稱或狀態篩選時，自動回退到原始表查詢
- Campaign 列表保持原有邏輯（需要詳細的 campaign 級別數據）
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
    
    # 加強類型轉換和 null 值處理
    try:
        cost_value = data.get("cost", 0)
        if cost_value is None or cost_value == "":
            cost = Decimal("0")
        else:
            cost = Decimal(str(cost_value))
    except (ValueError, TypeError):
        logger.warning(f"Invalid cost value: {data.get('cost')}")
        cost = Decimal("0")
    
    try:
        sales_value = data.get("sales", 0)
        if sales_value is None or sales_value == "":
            sales = Decimal("0")
        else:
            sales = Decimal(str(sales_value))
    except (ValueError, TypeError):
        logger.warning(f"Invalid sales value: {data.get('sales')}")
        sales = Decimal("0")
    
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


def map_state_values(states: List[str]) -> List[str]:
    """Convert frontend state values to Amazon Ads campaign status values
    
    Mapping:
    - 'active' -> 'ENABLED'
    - 'paused' -> 'PAUSED'
    - Others -> uppercase
    """
    mapped_states = []
    for state in states:
        if state.lower() == 'active':
            mapped_states.append('ENABLED')
        elif state.lower() == 'paused':
            mapped_states.append('PAUSED')
        else:
            mapped_states.append(state.upper())
    return mapped_states


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
        logger.info(f"Received filters: {filter_dict}")
        
        # Debug: Check available campaign statuses when filter is active
        if filter_dict.get('state'):
            debug_query = supabase.table('amazon_ads_campaigns_reports_sp').select(
                'campaignStatus'
            ).eq('profile_id', profile_id).limit(100).execute()
            
            if debug_query.data:
                unique_statuses = list(set([row.get('campaignStatus', 'N/A') for row in debug_query.data]))
                logger.info(f"Available campaign statuses in database: {unique_statuses}")
        
        # 計算前期日期範圍
        start_dt = datetime.strptime(start_date, "%Y-%m-%d")
        end_dt = datetime.strptime(end_date, "%Y-%m-%d")
        date_diff = (end_dt - start_dt).days + 1
        
        prev_start_date = (start_dt - timedelta(days=date_diff)).strftime("%Y-%m-%d")
        prev_end_date = (start_dt - timedelta(days=1)).strftime("%Y-%m-%d")
        
        # 構建篩選條件
        where_clause, filter_params = build_filter_clause(filter_dict)
        
        # 1. 獲取總計數據 - 使用聚合表 amazon_ads_daily_summary
        current_data = {"impressions": 0, "clicks": 0, "orders": 0, "units": 0, "cost": 0, "sales": 0}
        previous_data = {"impressions": 0, "clicks": 0, "orders": 0, "units": 0, "cost": 0, "sales": 0}
        
        # 查詢聚合表獲取當期和前期數據
        summary_result = supabase.table('amazon_ads_daily_summary').select(
            'date, impressions, clicks, orders, units, cost, sales, sp_campaign_count, sb_campaign_count, sd_campaign_count'
        ).eq('profile_id', profile_id).gte('date', prev_start_date).lte('date', end_date).execute()
        
        # 處理聚合數據
        for row in summary_result.data:
            row_date = datetime.strptime(row['date'], "%Y-%m-%d")
            data_target = current_data if row_date >= start_dt else previous_data
            
            # 檢查 adType 篩選條件
            if filter_dict.get('adType'):
                ad_types = filter_dict['adType'] if isinstance(filter_dict['adType'], list) else [filter_dict['adType']]
                # 如果有 adType 篩選，需要按比例計算（這裡簡化處理，實際可能需要更精確的方法）
                # 注意：聚合表無法精確篩選 campaign 名稱和狀態，這些篩選只能在 campaign 列表中實現
                include_sp = 'SP' in ad_types
                include_sb = 'SB' in ad_types
                include_sd = 'SD' in ad_types
                
                total_campaigns = row['sp_campaign_count'] + row['sb_campaign_count'] + row['sd_campaign_count']
                if total_campaigns > 0:
                    # 按廣告類型比例分配數據
                    sp_ratio = row['sp_campaign_count'] / total_campaigns if include_sp else 0
                    sb_ratio = row['sb_campaign_count'] / total_campaigns if include_sb else 0
                    sd_ratio = row['sd_campaign_count'] / total_campaigns if include_sd else 0
                    total_ratio = sp_ratio + sb_ratio + sd_ratio
                    
                    if total_ratio > 0:
                        data_target['impressions'] += int(row.get('impressions', 0) * total_ratio)
                        data_target['clicks'] += int(row.get('clicks', 0) * total_ratio)
                        data_target['orders'] += int(row.get('orders', 0) * total_ratio)
                        data_target['units'] += int(row.get('units', 0) * total_ratio)
                        data_target['cost'] += float(row.get('cost', 0) or 0) * total_ratio
                        data_target['sales'] += float(row.get('sales', 0) or 0) * total_ratio
            else:
                # 沒有 adType 篩選，使用全部數據
                data_target['impressions'] += row.get('impressions', 0) or 0
                data_target['clicks'] += row.get('clicks', 0) or 0
                data_target['orders'] += row.get('orders', 0) or 0
                data_target['units'] += row.get('units', 0) or 0
                data_target['cost'] += float(row.get('cost', 0) or 0)
                data_target['sales'] += float(row.get('sales', 0) or 0)
        
        # 注意：由於聚合表無法進行 campaign 名稱和狀態篩選，
        # 如果有這些篩選條件，我們需要回退到原始表查詢
        if filter_dict.get('campaign') or filter_dict.get('state'):
            logger.info("Detected campaign or state filters, falling back to detailed table queries for summary")
            # 重置數據
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
                mapped_states = map_state_values(states)
                sp_query = sp_query.in_('campaignStatus', mapped_states)
            
            sp_result = sp_query.limit(10000).execute()
            
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
                    mapped_states = map_state_values(states)
                    sb_query = sb_query.in_('campaignStatus', mapped_states)
                
                sb_result = sb_query.limit(10000).execute()
                
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
                    mapped_states = map_state_values(states)
                    sd_query = sd_query.in_('campaignStatus', mapped_states)
                
                sd_result = sd_query.limit(10000).execute()
                
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
        daily_performance = []
        
        # 優先使用聚合表，除非有 campaign 或 state 篩選條件
        if not filter_dict.get('campaign') and not filter_dict.get('state'):
            # 使用聚合表查詢每日數據
            daily_result = supabase.table('amazon_ads_daily_summary').select(
                'date, impressions, clicks, orders, units, cost, sales, acos, ctr, cvr, cpc, roas, rpc, sp_campaign_count, sb_campaign_count, sd_campaign_count'
            ).eq('profile_id', profile_id).gte('date', start_date).lte('date', end_date).order('date').execute()
            
            for row in daily_result.data:
                # 檢查 adType 篩選條件
                if filter_dict.get('adType'):
                    ad_types = filter_dict['adType'] if isinstance(filter_dict['adType'], list) else [filter_dict['adType']]
                    include_sp = 'SP' in ad_types
                    include_sb = 'SB' in ad_types
                    include_sd = 'SD' in ad_types
                    
                    total_campaigns = row['sp_campaign_count'] + row['sb_campaign_count'] + row['sd_campaign_count']
                    if total_campaigns > 0:
                        # 按廣告類型比例分配數據
                        sp_ratio = row['sp_campaign_count'] / total_campaigns if include_sp else 0
                        sb_ratio = row['sb_campaign_count'] / total_campaigns if include_sb else 0
                        sd_ratio = row['sd_campaign_count'] / total_campaigns if include_sd else 0
                        total_ratio = sp_ratio + sb_ratio + sd_ratio
                        
                        if total_ratio > 0:
                            # 創建調整後的數據
                            adjusted_data = {
                                'impressions': int(row.get('impressions', 0) * total_ratio),
                                'clicks': int(row.get('clicks', 0) * total_ratio),
                                'orders': int(row.get('orders', 0) * total_ratio),
                                'units': int(row.get('units', 0) * total_ratio),
                                'cost': float(row.get('cost', 0) or 0) * total_ratio,
                                'sales': float(row.get('sales', 0) or 0) * total_ratio
                            }
                            metrics = calculate_metrics(adjusted_data)
                            daily_performance.append(DailyPerformance(
                                date=row['date'],
                                **metrics
                            ))
                    # else: 沒有符合的廣告類型，跳過這一天
                else:
                    # 沒有 adType 篩選，直接使用聚合表的數據
                    daily_performance.append(DailyPerformance(
                        date=row['date'],
                        impressions=row.get('impressions', 0) or 0,
                        clicks=row.get('clicks', 0) or 0,
                        orders=row.get('orders', 0) or 0,
                        units=row.get('units', 0) or 0,
                        spend=Decimal(str(row.get('cost', 0) or 0)),
                        sales=Decimal(str(row.get('sales', 0) or 0)),
                        acos=Decimal(str(row.get('acos', 0))) if row.get('acos') is not None else None,
                        ctr=Decimal(str(row.get('ctr', 0))) if row.get('ctr') is not None else None,
                        cvr=Decimal(str(row.get('cvr', 0))) if row.get('cvr') is not None else None,
                        cpc=Decimal(str(row.get('cpc', 0))) if row.get('cpc') is not None else None,
                        roas=Decimal(str(row.get('roas', 0))) if row.get('roas') is not None else None,
                        rpc=Decimal(str(row.get('rpc', 0))) if row.get('rpc') is not None else None
                    ))
        else:
            # 有 campaign 或 state 篩選，需要查詢原始表
            logger.info("Detected campaign or state filters, falling back to detailed table queries for daily performance")
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
                    mapped_states = map_state_values(states)
                    sp_daily_query = sp_daily_query.in_('campaignStatus', mapped_states)
                
                sp_daily_result = sp_daily_query.limit(10000).execute()
                
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
                    mapped_states = map_state_values(states)
                    sb_daily_query = sb_daily_query.in_('campaignStatus', mapped_states)
                
                sb_daily_result = sb_daily_query.limit(10000).execute()
                
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
                    mapped_states = map_state_values(states)
                    sd_daily_query = sd_daily_query.in_('campaignStatus', mapped_states)
                
                sd_daily_result = sd_daily_query.limit(10000).execute()
                
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
            for date in sorted(daily_data.keys()):
                metrics = calculate_metrics(daily_data[date])
                daily_performance.append(DailyPerformance(
                    date=date,
                    **metrics
                ))
        
        # 3. 獲取 Campaign 列表數據
        campaigns_data = {}
        
        # First, get campaign group mappings for this profile
        campaign_groups = {}
        try:
            # Get all campaigns with their group information for this profile
            campaigns_with_groups = supabase.table('amazon_ads_campaigns').select(
                'campaign_id, group_id, campaign_groups(id, name)'
            ).eq('profile_id', profile_id).execute()
            
            # Build a mapping of campaign_id to group name
            logger.info(f"Retrieved {len(campaigns_with_groups.data)} campaigns from amazon_ads_campaigns table")
            for campaign in campaigns_with_groups.data:
                if campaign.get('group_id') and campaign.get('campaign_groups'):
                    campaign_groups[str(campaign['campaign_id'])] = campaign['campaign_groups']['name']
                    logger.info(f"Mapping: Campaign ID {campaign['campaign_id']} -> Group '{campaign['campaign_groups']['name']}'")
            
            logger.info(f"Loaded {len(campaign_groups)} campaign group mappings for profile {profile_id}")
        except Exception as e:
            logger.error(f"Error loading campaign groups: {str(e)}")
            # Continue without campaign groups if there's an error
        
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
                mapped_states = map_state_values(states)
                logger.info(f"SP Campaign state filter - Original: {states}, Mapped: {mapped_states}")
                sp_campaign_query = sp_campaign_query.in_('campaignStatus', mapped_states)
            
            sp_campaign_result = sp_campaign_query.limit(10000).execute()
            logger.info(f"SP Campaign query returned {len(sp_campaign_result.data)} results")
            
            # Log sample campaign statuses for debugging
            if sp_campaign_result.data:
                sample_statuses = list(set([row.get('campaignStatus', 'N/A') for row in sp_campaign_result.data[:10]]))
                logger.info(f"Sample SP campaign statuses: {sample_statuses}")
            
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
                
                # 添加調試日誌和更強大的數據提取
                try:
                    cost_raw = row.get('cost')
                    if cost_raw is None or cost_raw == '':
                        cost_value = 0.0
                    else:
                        cost_value = float(cost_raw)
                except (ValueError, TypeError) as e:
                    logger.warning(f"SP Campaign {campaign_id}: Invalid cost value: {row.get('cost')}, error: {e}")
                    cost_value = 0.0
                
                try:
                    sales_raw = row.get('sales7d')
                    if sales_raw is None or sales_raw == '':
                        sales_value = 0.0
                    else:
                        sales_value = float(sales_raw)
                except (ValueError, TypeError) as e:
                    logger.warning(f"SP Campaign {campaign_id}: Invalid sales7d value: {row.get('sales7d')}, error: {e}")
                    sales_value = 0.0
                
                # if campaign_id and (cost_value > 0 or sales_value > 0):
                #     logger.info(f"SP Campaign {campaign_id}: cost={cost_value}, sales7d={sales_value}")
                
                campaigns_data[campaign_id]['impressions'] += row.get('impressions', 0) or 0
                campaigns_data[campaign_id]['clicks'] += row.get('clicks', 0) or 0
                campaigns_data[campaign_id]['orders'] += row.get('purchases7d', 0) or 0
                campaigns_data[campaign_id]['units'] += row.get('unitsSoldClicks7d', 0) or 0
                campaigns_data[campaign_id]['cost'] += cost_value
                campaigns_data[campaign_id]['sales'] += sales_value
        
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
                mapped_states = map_state_values(states)
                logger.info(f"SB Campaign state filter - Original: {states}, Mapped: {mapped_states}")
                sb_campaign_query = sb_campaign_query.in_('campaignStatus', mapped_states)
            
            sb_campaign_result = sb_campaign_query.limit(10000).execute()
            logger.info(f"SB Campaign query returned {len(sb_campaign_result.data)} results")
            
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
                
                # 添加調試日誌
                cost_value = row.get('cost', 0) or 0
                sales_value = row.get('sales', 0) or 0
                if campaign_id and (cost_value > 0 or sales_value > 0):
                    logger.debug(f"SB Campaign {campaign_id}: cost={cost_value}, sales={sales_value}")
                
                campaigns_data[campaign_id]['impressions'] += row.get('impressions', 0) or 0
                campaigns_data[campaign_id]['clicks'] += row.get('clicks', 0) or 0
                campaigns_data[campaign_id]['orders'] += row.get('purchases', 0) or 0
                campaigns_data[campaign_id]['units'] += row.get('unitsSold', 0) or 0
                campaigns_data[campaign_id]['cost'] += float(cost_value)
                campaigns_data[campaign_id]['sales'] += float(sales_value)
        
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
                mapped_states = map_state_values(states)
                logger.info(f"SD Campaign state filter - Original: {states}, Mapped: {mapped_states}")
                sd_campaign_query = sd_campaign_query.in_('campaignStatus', mapped_states)
            
            sd_campaign_result = sd_campaign_query.limit(10000).execute()
            logger.info(f"SD Campaign query returned {len(sd_campaign_result.data)} results")
            
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
                
                # 添加調試日誌
                cost_value = row.get('cost', 0) or 0
                sales_value = row.get('sales', 0) or 0
                if campaign_id and (cost_value > 0 or sales_value > 0):
                    logger.debug(f"SD Campaign {campaign_id}: cost={cost_value}, sales={sales_value}")
                
                campaigns_data[campaign_id]['impressions'] += row.get('impressions', 0) or 0
                campaigns_data[campaign_id]['clicks'] += row.get('clicks', 0) or 0
                campaigns_data[campaign_id]['orders'] += row.get('purchases', 0) or 0
                campaigns_data[campaign_id]['units'] += row.get('unitsSold', 0) or 0
                campaigns_data[campaign_id]['cost'] += float(cost_value)
                campaigns_data[campaign_id]['sales'] += float(sales_value)
        
        # 轉換 campaign 數據為列表
        campaigns = []
        for campaign_data in campaigns_data.values():
            metrics = calculate_metrics(campaign_data)
            
            # 添加調試日誌檢查最終數據
            # if campaign_data['cost'] > 0 or campaign_data['sales'] > 0:
            #     logger.info(f"Campaign {campaign_data['campaignId']} final data: cost={campaign_data['cost']}, sales={campaign_data['sales']}")
            #     logger.info(f"Campaign {campaign_data['campaignId']} metrics: spend={metrics.get('spend')}, sales={metrics.get('sales')}")
            
            # Get campaign group name from the mapping
            campaign_id = str(campaign_data['campaignId'])
            campaign_group_name = campaign_groups.get(campaign_id)
            # if campaign_group_name:
            #     logger.info(f"Found group '{campaign_group_name}' for campaign {campaign_id}")
            # else:
            #     logger.warning(f"No group found for campaign {campaign_id} - available keys: {list(campaign_groups.keys())[:5]}...")
            
            campaign = CampaignData(
                campaignId=campaign_data['campaignId'],
                campaignName=campaign_data['campaignName'],
                adType=campaign_data['ad_type'],
                campaignStatus=campaign_data['campaignStatus'],
                optGroup=campaign_group_name,  # Set the campaign group name
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