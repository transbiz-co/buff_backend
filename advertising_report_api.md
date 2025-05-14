# Amazon Advertising API V3 報告調用完整指南

## 概述

Amazon Ads API 報告是異步的，需要進行三個 API 調用來生成報告：
1. 請求報告
2. 檢查報告狀態
3. 下載報告

## 必需的請求標頭

每個 API 請求都需要以下四個標頭參數：

| 參數 | 描述 |
|------|------|
| `Amazon-Advertising-API-ClientId` | 與 Login with Amazon 應用程式關聯的客戶端 ID，用於身份驗證 |
| `Authorization` | 您的存取令牌，用於身份驗證 |
| `Amazon-Advertising-API-Scope` | 與特定市場廣告帳戶關聯的 profile ID |
| `Content-Type` | 設為 `application/vnd.createasyncreportrequest.v3+json` |

## 步驟一：請求報告

使用端點 `POST /reporting/reports` 請求報告。

### 重要參數說明

#### 1. timeUnit
可設為 `DAILY` 或 `SUMMARY`
- `DAILY`：包含日期，需在列表中包含 `date`
- `SUMMARY`：可在列表中包含 `startDate` 和 `endDate`

#### 2. groupBy
確定報告的粒度級別，所有報告都必須包含此參數

#### 3. filters
根據 groupBy 級別確定過濾器

### 請求範例

#### SUMMARY 報告
```json
{
    "name":"SP campaigns report 7/5-7/10",
    "startDate":"2022-07-05",
    "endDate":"2022-07-10",
    "configuration":{
        "adProduct":"SPONSORED_PRODUCTS",
        "groupBy":["campaign"],
        "columns":["impressions","clicks","cost","campaignId","startDate","endDate"],
        "reportTypeId":"spCampaigns",
        "timeUnit":"SUMMARY",
        "format":"GZIP_JSON"
    }
}
```

#### DAILY 報告
```json
{
    "name":"SP campaigns report 7/5-7/10",
    "startDate":"2022-07-05",
    "endDate":"2022-07-10",
    "configuration":{
        "adProduct":"SPONSORED_PRODUCTS",
        "groupBy":["campaign"],
        "columns":["impressions","clicks","cost","campaignId","date"],
        "reportTypeId":"spCampaigns",
        "timeUnit":"DAILY",
        "format":"GZIP_JSON"
    }
}
```

#### 使用過濾器的請求
```json
{
    "name":"SP campaigns report 7/5-7/10",
    "startDate":"2022-07-05",
    "endDate":"2022-07-10",
    "configuration":{
        "adProduct":"SPONSORED_PRODUCTS",
        "groupBy":["campaign"],
        "columns":["impressions","clicks","cost","campaignId","startDate","endDate"],
        "reportTypeId":"spCampaigns",
        "timeUnit":"SUMMARY",
        "format":"GZIP_JSON",
        "filters": [
            {
                "field": "campaignStatus",
                "values": ["ENABLED","PAUSED"]
            }
        ]
    }
}
```

### 完整 cURL 範例
```bash
curl --location --request POST 'https://advertising-api.amazon.com/reporting/reports' \
--header 'Content-Type: application/vnd.createasyncreportrequest.v3+json' \
--header 'Amazon-Advertising-API-ClientId: amzn1.application-oa2-client.xxxxxxxxxx' \
--header 'Amazon-Advertising-API-Scope: xxxxxxxxxx' \
--header 'Authorization: Bearer Atza|xxxxxx' \
--data-raw '{
    "name":"SP campaigns report 7/5-7/10",
    "startDate":"2022-07-05",
    "endDate":"2022-07-10",
    "configuration":{
        "adProduct":"SPONSORED_PRODUCTS",
        "groupBy":["campaign","adGroup"],
        "columns":["campaignId","adGroupId","impressions","clicks","cost","purchases1d","purchases7d","purchases14d","purchases30d","startDate","endDate"],
        "reportTypeId":"spCampaigns",
        "timeUnit":"SUMMARY",
        "format":"GZIP_JSON"
    }
}'
```

### 成功回應範例
```json
{
    "configuration": {
        "adProduct": "SPONSORED_PRODUCTS",
        "columns": [
            "campaignId",
            "adGroupId",
            "impressions",
            "clicks",
            "cost",
            "purchases1d",
            "purchases7d",
            "purchases14d",
            "purchases30d",
            "startDate",
            "endDate"
        ],
        "filters": null,
        "format": "GZIP_JSON",
        "groupBy": [
            "campaign",
            "adGroup"
        ],
        "reportTypeId": "spCampaigns",
        "timeUnit": "SUMMARY"
    },
    "createdAt": "2022-07-19T14:03:00.853Z",
    "endDate": "2022-07-05",
    "failureReason": null,
    "fileSize": null,
    "generatedAt": null,
    "name": "SP campaigns report 7/5-7/21",
    "reportId": "xxxxxxx-xxxx-xxxx-xxxx-xxxxxxxx",
    "startDate": "2022-07-05",
    "status": "PENDING",
    "updatedAt": "2022-07-19T14:03:00.853Z",
    "url": null,
    "urlExpiresAt": null
}
```

## 步驟二：檢查報告狀態

報告生成可能需要最多三小時。使用返回的 `reportId` 檢查狀態。

```bash
curl --location --request GET 'https://advertising-api.amazon.com/reporting/reports/xxxxx-xxxxx-xxxxx' \
--header 'Content-Type: application/vnd.createasyncreportrequest.v3+json' \
--header 'Amazon-Advertising-API-ClientId: amzn1.application-oa2-client.xxxxxxxxxx' \
--header 'Amazon-Advertising-API-Scope: xxxxxxx' \
--header 'Authorization: Bearer Atza|xxxxxxxxxxx'
```

### 狀態說明
- `PENDING` 或 `PROCESSING`：報告仍在生成中
- `COMPLETED`：報告已完成，`url` 欄位包含下載連結

### 重要提醒
- 重複調用可能會收到 429 回應（請求限制）
- 建議使用指數退避重試邏輯
- 兩次請求之間需要適當的延遲

## 步驟三：下載報告

當報告狀態變為 `COMPLETED` 時，`url` 欄位會包含 S3 存儲桶的下載連結。

可以使用 cURL 或直接在瀏覽器中輸入 URL 來下載報告。

## 讀取報告數據

下載並解壓縮報告文件後，會看到原始 JSON 格式的數據。

```json
[
    {
        "purchases7d":2,
        "cost":14.5,
        "purchases30d":3,
        "endDate":"2022-07-10",
        "campaignId":158410630682987,
        "clicks":13,
        "purchases1d":1,
        "impressions":2216,
        "adGroupId":72320882861500,
        "startDate":"2022-07-05",
        "purchases14d":3
    },
    {
        "purchases7d":2,
        "cost":9.45,
        "purchases30d":2,
        "endDate":"2022-07-10",
        "campaignId":158410630682987,
        "clicks":10,
        "purchases1d":2,
        "impressions":3721,
        "adGroupId":55720282058882,
        "startDate":"2022-07-05",
        "purchases14d":2
    }
]
```

## 完整 Python 範例代碼

```python
import requests
import time
import json

class AmazonAdsReporting:
    def __init__(self, client_id, access_token, profile_id):
        self.client_id = client_id
        self.access_token = access_token
        self.profile_id = profile_id
        self.base_url = "https://advertising-api.amazon.com"
        self.headers = {
            'Content-Type': 'application/vnd.createasyncreportrequest.v3+json',
            'Amazon-Advertising-API-ClientId': self.client_id,
            'Amazon-Advertising-API-Scope': self.profile_id,
            'Authorization': f'Bearer {self.access_token}'
        }
    
    def create_report(self, report_config):
        """創建報告請求"""
        response = requests.post(
            f"{self.base_url}/reporting/reports",
            headers=self.headers,
            json=report_config
        )
        return response.json()
    
    def check_report_status(self, report_id):
        """檢查報告狀態"""
        response = requests.get(
            f"{self.base_url}/reporting/reports/{report_id}",
            headers=self.headers
        )
        return response.json()
    
    def download_report(self, download_url):
        """下載報告"""
        response = requests.get(download_url)
        return response.content
    
    def wait_for_report(self, report_id, max_wait=10800):  # 最多等 3 小時
        """等待報告完成"""
        start_time = time.time()
        while time.time() - start_time < max_wait:
            status_response = self.check_report_status(report_id)
            status = status_response.get('status')
            
            if status == 'COMPLETED':
                return status_response
            elif status == 'FAILED':
                raise Exception(f"Report failed: {status_response.get('failureReason')}")
            
            time.sleep(30)  # 等 30 秒再檢查
        
        raise Exception("Report timeout")

# 使用範例
if __name__ == "__main__":
    # 初始化
    ads_reporting = AmazonAdsReporting(
        client_id="your_client_id",
        access_token="your_access_token",
        profile_id="your_profile_id"
    )
    
    # 報告配置
    report_config = {
        "name": "SP Campaigns Report",
        "startDate": "2024-01-01",
        "endDate": "2024-01-31",
        "configuration": {
            "adProduct": "SPONSORED_PRODUCTS",
            "groupBy": ["campaign"],
            "columns": ["impressions", "clicks", "cost", "campaignId", "startDate", "endDate"],
            "reportTypeId": "spCampaigns",
            "timeUnit": "SUMMARY",
            "format": "GZIP_JSON"
        }
    }
    
    # 創建報告
    report_response = ads_reporting.create_report(report_config)
    report_id = report_response['reportId']
    print(f"報告已創建，ID: {report_id}")
    
    # 等待報告完成
    try:
        completed_report = ads_reporting.wait_for_report(report_id)
        print(f"報告完成，下載 URL: {completed_report['url']}")
        
        # 下載報告
        report_data = ads_reporting.download_report(completed_report['url'])
        
        # 保存報告
        with open(f"report_{report_id}.gz", "wb") as f:
            f.write(report_data)
        print("報告已下載")
        
    except Exception as e:
        print(f"錯誤: {e}")
```

## 重要提示

1. **API 限制**：避免頻繁調用，使用指數退避重試
2. **報告格式**：支援 GZIP_JSON、GZIP_CSV 等格式
3. **數據範圍**：單個報告最多可包含 90 天的數據
4. **重複請求**：相同參數的重複請求會返回 425 狀態碼
5. **區域端點**：根據市場選擇正確的 API 端點

這個完整的指南涵蓋了調用 Amazon Advertising API V3 報告的所有必要步驟，包括請求、狀態檢查、下載和數據處理。