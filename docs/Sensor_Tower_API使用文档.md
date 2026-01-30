# Sensor Tower API 使用文档

本文档说明本项目中使用的 Sensor Tower API：认证方式、Base URL、分地区数据接口与广告创意接口的路径与参数，以及**请求分地区数据时的地区代码**、**请求广告数据时的网络代码**等细节。所有约定以当前代码实现为准，便于对接与维护。

---

## 一、通用约定

### 1.1 Base URL

- **默认**：`https://api.sensortower.com/v1`
- **覆盖**：可通过环境变量 `OVERRIDE_API_BASE` 覆盖（如测试环境）。
- 请求路径为**相对 Base URL**，例如路径 `unified/sales_report_estimates` 实际请求：  
  `https://api.sensortower.com/v1/unified/sales_report_estimates`

### 1.2 认证

- **方式**：Bearer Token
- **请求头**：
  - `Authorization: Bearer <token>`
  - `Content-Type: application/json`
  - `Accept: application/json`
- **Token 来源**：本项目从 `request/token.txt` 读取（仅一行，去掉首尾空白）。**请勿将 token 提交到版本库。**

### 1.3 请求方法

- 当前使用的接口均为 **GET**，参数通过 **Query 参数** 传递。

### 1.4 调试

- 设置环境变量 `DEBUG_API=1` 时，会打印实际请求 URL 与参数（见 `request/api_request.py`）。

---

## 二、分地区数据 API（下载/收入估算）

### 2.1 接口信息

| 项目 | 说明 |
|------|------|
| **路径** | `GET /v1/{os}/sales_report_estimates` |
| **路径参数** | `os`：平台，见下表 |
| **用途** | 按国家/地区、日期获取 App 的下载量、收入估算 |

### 2.2 路径参数：`os`（平台）

| 取值 | 说明 |
|------|------|
| `unified` | 统一（iOS + Android），**推荐**，与接口文档一致 |
| `ios` | 仅 iOS |
| `android` | 仅 Android |

本项目默认使用 **`unified`**。

### 2.3 查询参数

| 参数 | 必填 | 类型 | 说明 |
|------|------|------|------|
| `app_ids` | 是 | string | 应用 ID，本项目使用 **Unified ID**（单次请求传一个 app_id） |
| `start_date` | 建议 | string | 开始日期，格式 **YYYY-MM-DD** |
| `end_date` | 建议 | string | 结束日期，格式 **YYYY-MM-DD** |
| `date_granularity` | 否 | string | 日期粒度，见下表 |
| `countries` | 否 | string | 地区范围，见下文「地区代码」 |
| `data_model` | 否 | string | 数据模型版本，见下表 |

**date_granularity 取值：**

| 取值 | 说明 |
|------|------|
| `daily` | 按日 |
| `weekly` | 按周（**本项目默认**） |
| `monthly` | 按月 |
| `quarterly` | 按季 |

**data_model 取值：**

| 取值 | 说明 |
|------|------|
| `DM_2025_Q1` | 2025 Q1 模型 |
| `DM_2025_Q2` | 2025 Q2 模型（**本项目默认**） |

### 2.4 地区代码（分地区数据 API）

请求**所有地区**时使用：

| 取值 | 说明 |
|------|------|
| **`ALL`** | 所有国家/地区（**推荐**，与接口文档一致） |
| `WW` | 全球，部分文档中与“全部”等价，本项目以 **ALL** 为准 |

请求**指定国家**时：使用 **ISO 3166-1 alpha-2** 双字母国家代码，多个用**英文逗号**分隔，例如：  
`US,GB,JP,TW`

**本项目常用国家代码（与 mapping/市场T度.csv 一致）：**

- **亚洲 T1**：`TW`（台湾）、`HK`（香港）、`JP`（日本）、`KR`（韩国）  
  （注意：亚洲 T1 **排除 CN**，不做投放时可不传 CN）
- **欧美 T1**：`US`、`GB`、`DE`、`FR`、`CA`、`AU`
- **T2**：`IT`、`ES`、`NL`、`SE`、`NO`、`DK`、`CH`、`AT`、`BE`、`NZ`、`SG`、`IL`、`AE`、`SA`
- **T3（示例）**：`BR`、`MX`、`IN`、`ID`、`TH`、`MY`、`PH`、`VN`、`TR`、`RU`、`PL`、`CO`、`AR`、`CL`、`PE`、`EG`、`NG`、`PK`、`BD`、`UA`、`RO`、`PT`、`GR` 等

更多国家代码请参考 ISO 3166-1 alpha-2 或 Sensor Tower 官方文档。

### 2.5 响应结构

- 响应体为 JSON。
- 本项目实现中，**数据列表在顶层 `data` 字段**中，即：  
  `{ "data": [ { "country": "...", "date": "...", "android_units", "unified_units", ... }, ... ] }`
- 每条记录通常包含：`app_id`、`country`、`date`、`android_units`、`android_revenue`、`iphone_units`、`iphone_revenue`、`unified_units`、`unified_revenue`、`ipad_units`、`ipad_revenue` 等（以实际 API 返回为准）。

---

## 三、广告创意 API

### 3.1 接口信息

| 项目 | 说明 |
|------|------|
| **路径** | `GET /v1/{os}/ad_intel/creatives` |
| **路径参数** | `os`：同分地区 API，取值为 `ios`、`android`、`unified`（**本项目默认 unified**） |
| **用途** | 按国家/地区、时间范围获取指定 App 的广告创意（含视频等） |

### 3.2 查询参数

| 参数 | 必填 | 类型 | 说明 |
|------|------|------|------|
| `app_ids` | 是 | string | 应用 ID，Unified ID；多个用**英文逗号**分隔 |
| `start_date` | 建议 | string | 开始日期，**YYYY-MM-DD** |
| `end_date` | 否 | string | 结束日期，**YYYY-MM-DD** |
| `countries` | 是 | string | 国家代码，**英文逗号**分隔，见下文；**不可用 ALL**，需明确传国家列表 |
| `ad_types` | 否 | string | 广告类型，见下表；多个用逗号分隔 |
| `networks` | 否 | string | 广告网络代码，见下文「网络代码」；多个用逗号分隔 |

### 3.3 广告类型（ad_types）

本项目仅拉取 **video** 类型：

| 取值 | 说明 |
|------|------|
| `video` | 视频创意（**本项目使用**） |

其他类型以 Sensor Tower 文档为准（如 `image`、`carousel` 等）。

### 3.4 网络代码（networks）

请求广告数据时，**networks** 参数使用以下代码（与代码中 `NETWORKS` 列表一致，拼成逗号分隔字符串传给 API）：

| 网络代码 | 说明 |
|----------|------|
| Admob | Google AdMob |
| Applovin | AppLovin |
| BidMachine | BidMachine |
| Chartboost | Chartboost |
| Digital Turbine | Digital Turbine |
| Facebook | Facebook |
| InMobi | InMobi |
| Instagram | Instagram |
| Line | LINE |
| Meta Audience Network | Meta Audience Network |
| Mintegral | Mintegral |
| Moloco | Moloco |
| Pangle | Pangle（穿山甲） |
| Pinterest | Pinterest |
| Smaato | Smaato |
| Snapchat | Snapchat |
| Supersonic | Supersonic |
| TikTok | TikTok |
| Twitter | Twitter（X） |
| Unity | Unity Ads |
| Verve | Verve |
| Vungle | Vungle |
| Youtube | YouTube |

**示例**：  
`networks=Admob,Applovin,Facebook,TikTok,Unity,Vungle,Youtube`  
（本项目脚本中会传**全部**上述网络，逗号拼接。）

### 3.5 地区代码（广告创意 API）

- **不能**使用 `ALL`；必须传**具体国家代码**，多个用**英文逗号**分隔，例如：  
  `US,GB,DE,FR,CA,AU`
- 国家代码与分地区 API 一致（ISO 3166-1 alpha-2）。
- 本项目按**市场 T 度**分四次请求：
  - **亚洲 T1**：`TW,HK,JP,KR`（来自 `mapping/市场T度.csv`）
  - **欧美 T1**：`US,GB,DE,FR,CA,AU`
  - **T2**：`IT,ES,NL,SE,NO,DK,CH,AT,BE,NZ,SG,IL,AE,SA`
  - **T3**：脚本内常量 `T3_COUNTRIES`（完整 50 国中未在亚洲T1/欧美T1/T2 的 26 国），如：  
    `AR,BR,CL,CN,CO,CZ,EC,GR,ID,IN,LU,MX,MY,NG,PA,PE,PH,PL,PT,RO,RU,TH,TR,UA,VN,ZA`

### 3.6 响应结构

- 响应为 JSON；本项目主要使用其中的 **ad_units** 或 **creatives** 数组（具体字段名以 API 返回为准），用于导出 JSON/XLSX 及前端展示。

---

## 四、地区代码汇总表（便于查阅）

### 4.1 分地区数据 API（sales_report_estimates）

| 用途 | 参数值 | 说明 |
|------|--------|------|
| 所有地区 | **ALL** | 推荐，一次拉全量国家维度数据 |
| 指定国家 | 逗号分隔代码，如 `US,GB,JP` | ISO 3166-1 alpha-2 |

### 4.2 广告创意 API（ad_intel/creatives）

| 用途 | 参数值 | 说明 |
|------|--------|------|
| 必须传国家 | 不可用 ALL | 使用逗号分隔的国家代码 |
| 亚洲 T1 | TW,HK,JP,KR | 来自 市场T度.csv |
| 欧美 T1 | US,GB,DE,FR,CA,AU | 来自 市场T度.csv |
| T2 | IT,ES,NL,SE,NO,DK,CH,AT,BE,NZ,SG,IL,AE,SA | 来自 市场T度.csv |
| T3 | AR,BR,CL,CN,CO,CZ,EC,GR,ID,IN,LU,MX,MY,NG,PA,PE,PH,PL,PT,RO,RU,TH,TR,UA,VN,ZA（26 国） | 脚本内 T3_COUNTRIES |

### 4.3 国家代码与 T 度对应（mapping/市场T度.csv）

| T度 | country 代码列表 |
|-----|------------------|
| 亚洲T1 | TW, HK, JP, KR |
| 欧美T1 | US, GB, DE, FR, CA, AU |
| T2 | IT, ES, NL, SE, NO, DK, CH, AT, BE, NZ, SG, IL, AE, SA |
| T3 | 上表未出现的国家均归 T3；脚本中 T3 请求使用 T3_COUNTRIES 列表 |

---

## 五、错误与重试

- Sensor Tower API 网络偶发不稳定，本项目对**分地区数据**与**广告创意**请求均做了重试：
  - **最多请求 3 次**；
  - 失败后**延迟 3 秒**再重试；
  - 若 3 次均失败则抛出异常（或按脚本配置跳过该 app/该地区并继续下一个）。

---

## 六、本项目中的使用方式

| 项目 | 说明 |
|------|------|
| Token 文件 | `request/token.txt`（一行，Bearer 所用字符串） |
| Base URL | 默认 `https://api.sensortower.com/v1`，可用 `OVERRIDE_API_BASE` 覆盖 |
| 分地区数据脚本 | `request/fetch_country_data.py`，参数见脚本 `--help`（如 `--os unified`、`--countries ALL`、`--date_granularity weekly`、`--data_model DM_2025_Q2`） |
| 广告创意脚本 | `request/fetch_ad_creatives.py`，参数见脚本 `--help`（如 `--os unified`、`--countries` 按 T 度传入、脚本内写死 `ad_types=video` 与全部 `networks`） |
| 请求封装 | `request/api_request.py`：`load_token()`、`get_session()`、`get(session, path, params)` |

若 Sensor Tower 官方文档有更新（如新增参数、地区或网络代码），请以官方文档为准，并同步更新本文档与脚本中的默认值/列表。

---

## 七、最简代码用例

以下示例均在**项目根目录**下运行（或保证 `request` 包可被导入）。Token 需已写入 `request/token.txt`。

### 7.1 认证并发起一次 GET 请求（通用）

```python
from request.api_request import load_token, get_session, get

token = load_token()
session = get_session(token)
path = "unified/sales_report_estimates"
params = {"app_ids": "你的UnifiedID", "countries": "ALL", "date_granularity": "weekly"}
resp = get(session, path, params=params)
resp.raise_for_status()
data = resp.json()
```

### 7.2 分地区数据（sales_report_estimates）

```python
from request.api_request import load_token, get_session, get

token = load_token()
session = get_session(token)
path = "unified/sales_report_estimates"
params = {
    "app_ids": "573d973122cbf508c500071e",
    "start_date": "2025-12-08",
    "end_date": "2025-12-14",
    "date_granularity": "weekly",
    "countries": "ALL",
    "data_model": "DM_2025_Q2",
}
resp = get(session, path, params=params)
resp.raise_for_status()
result = resp.json()
# 数据通常在 result["data"] 中
rows = result.get("data", [])
```

### 7.3 广告创意（ad_intel/creatives）

```python
from request.api_request import load_token, get_session, get

token = load_token()
session = get_session(token)
path = "unified/ad_intel/creatives"
params = {
    "app_ids": "573d973122cbf508c500071e",
    "start_date": "2025-12-08",
    "end_date": "2025-12-14",
    "countries": "US,GB,DE,FR,CA,AU",
    "ad_types": "video",
    "networks": "Admob,Facebook,TikTok,Unity,Vungle,Youtube",
}
resp = get(session, path, params=params)
resp.raise_for_status()
result = resp.json()
# 创意列表通常在 result["ad_units"] 或 result["creatives"] 中
creatives = result.get("ad_units") or result.get("creatives") or []
```
