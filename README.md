# GSM-R Data Pipeline & Data Warehouse

A complete **automated ETL/ELT pipeline** and **medallion-architecture data warehouse** for GSM-R (Global System for Mobile Communications – Railway) network monitoring, analysis, and optimization.

---

## 📋 Project Overview

### Context

The GSM-R network is critical infrastructure for rail communications. The Network Optimization Service manages performance monitoring and maintenance across multiple disconnected data sources:

- **Expandium** — Real-time network supervision and diagnostics platform
- **SharePoint** — Collaborative data collection and reporting
- **Local Files** — Manual exports and internal team data

This fragmentation creates:
- ❌ Data silos and inconsistency
- ❌ Manual, error-prone reporting processes
- ❌ Duplicated effort in data preparation
- ❌ Delayed insights and decision-making

### Solution

This project centralizes, automates, and standardizes all GSM-R data into a **unified, production-grade data warehouse** using the **Bronze/Silver/Gold medallion architecture** on PostgreSQL. The result: reliable KPI tracking, automated reporting, and rapid analytics.

---

## 🎯 Project Objectives

| # | Objective | Description |
|---|-----------|-------------|
| 1 | **Automated Ingestion** | Collect data continuously from Expandium, SharePoint, and local sources |
| 2 | **ETL/ELT Pipeline** | Reliable, monitored pipeline with full data lineage and error tracking |
| 3 | **Medallion Architecture** | Bronze (raw) → Silver (cleaned) → Gold (business-ready) layers |
| 4 | **Data Quality** | Standardization, deduplication, type coercion, null handling |
| 5 | **Automation** | Scheduled daily/weekly updates with full data historization |
| 6 | **Centralized KPIs** | Unified network performance indicators from all sources |
| 7 | **Analytics Foundation** | Self-service data for BI tools, dashboards, and reporting |
| 8 | **Complete Documentation** | Architecture, transformations, data lineage, and operation guides |

---

## 🏗️ Architecture
<img width="922" height="501" alt="DATA ARCHITECTURE" src="https://github.com/user-attachments/assets/00dc4765-fbe5-44bd-b3ff-73f0c35adbd5" />

### Medallion Layers
<img width="731" height="621" alt="Layers Design" src="https://github.com/user-attachments/assets/5bad00e3-f6eb-4a6d-971d-7d3f42d47c47" />

### Data Flow
<img width="814" height="474" alt="Data Flow" src="https://github.com/user-attachments/assets/b88ecaa7-2589-4c00-8e4b-ab4f25a73a74" />

---

## 📊 Data Sources

### 1. **Expandium QATS**
   - **Extraction**: Selenium web scraping + automated CSV download
   - **Format**: Semicolon-delimited CSV with `sep=;` directive
   - **Tables**:
     - `exp_etcs_call` — ETCS call establishment and duration
     - `exp_transaction_tracing` — Mobile transaction details (setup, duration, routing)
     - `exp_vgcs_vbs_rec_tracing` — Group call events and success rates
     - `exp_ho_tracing` — Handover tracing (source → target cell, duration)
     - `exp_subscriber_matrix` — Subscriber profiles and IMSI/MSISDN mappings
     - `exp_hdlc_frame_errors` — Network frame retransmissions and errors

### 2. **SharePoint Excel Files**
   - **Extraction**: Microsoft Graph API + Excel sheet parsing
   - **Format**: `.xlsx` with standard column names (French)
   - **Tables**:
     - `sp_ertms_disconnects` — ERTMS disconnection incidents, causes, and corrective actions

### 3. **Tracking Tables**
   - `bronze.ingest_runs` — Batch metadata (start, end, status, row counts)
   - `bronze.ingest_files` — Per-file ingestion records (hash, row count, error logs)

---

## 🛠️ Technology Stack

| Component | Technology | Purpose |
|-----------|-----------|---------|
| **Database** | PostgreSQL 14+ | Data warehouse engine |
| **Language** | Python 3.12+ | ETL orchestration & automation |
| **Frameworks** | SQLAlchemy 2.x, pandas | Data manipulation & DB access |
| **Scraping** | Selenium | Expandium QATS automation |
| **Cloud APIs** | Microsoft Graph API | SharePoint file retrieval |
| **CSV/Excel** | openpyxl, pandas | File parsing |
| **Scheduling** | Cron / APScheduler | Automated daily/weekly runs |

---

## 📦 Project Structure

```
gsmr-data-pipeline-dwh/
├── scripts/
│   ├── bronze/
│   │   ├── common.py                    # Shared utilities (batch_id, hashing, file listing)
│   │   ├── get_db_engine.py             # SQLAlchemy engine factory
│   │   ├── expandium_ingestion.py       # Selenium scraping + CSV loading
│   │   ├── sharepoint_ingestion.py      # Graph API + Excel loading
│   │   ├── ingestion_run.py             # Orchestrator (runs both sources)
│   │   └── ddl_tables.sql               # Bronze schema definition
│   │
│   └── silver/
│       ├── procedure.sql                # Complete silver.load_silver() procedure
│       ├── ddl_tables.sql               # Silver schema + parsing functions
│       └── silver_orchestrator.py       # Runs silver procedure & logs stats
│
├── datasets/
│   ├── expandium/                       # Downloaded Expandium CSVs
│   └── sharepoint/                      # Downloaded SharePoint Excel files
│
├── docs/
│   └── Project Diagrams
│
├── README.md                            # This file
├── LICENSE                              # MIT License
├── run_pipeline.bat                     # the file that runs the pipline
└── scheduler.py                         # scheduler runs the pipline every 1 hour
```

---

## ⚙️ Installation & Setup

### Prerequisites

- **PostgreSQL** 14+ (local or remote)
- **Python** 3.12+
- **Chrome/Chromium** (for Selenium)
- **pip** or **poetry** (for Python dependencies)

### Step 1: Clone & Environment Setup

```bash
git clone https://github.com/yourusername/gsmr-data-pipeline-dwh.git
cd gsmr-data-pipeline-dwh

# Create virtual environment
python -m venv venv
source venv/bin/activate  # On Windows: venv\Scripts\activate

# Install dependencies
pip install -r requirements.txt
```

### Step 2: Configure Database Connection

Edit `scripts/bronze/get_db_engine.py`:

```python
HOST = "localhost"
PORT = 5432
NAME = "gsmr_dwh"
USER = "postgres"
PASSWORD = "your_password"  # Replace with your PostgreSQL password
```

### Step 3: Initialize Database Schema

```bash
# Connect to PostgreSQL
psql -U postgres -h localhost

# Create database and schemas
\i scripts/init_database.sql
```

### Step 4: Load Bronze Layer

```bash
cd scripts/bronze

# Create all bronze tables
psql -U postgres -d gsmr_dwh -f ddl_tables.sql

# Run ingestion
python ingestion_run.py
```

**Expected output:**
```
the run run_2026-05-25_17:40:10_43ebd1i, status=SUCCESS
Total files =7, Total rows = 4627,
Total success=7, Total failed=0, Total skipped=0,
```

### Step 5: Load Silver Layer

```bash
cd ../silver

# Create silver schema & parsing functions
psql -U postgres -d gsmr_dwh -f ddl_tables.sql

# Run silver orchestrator
python silver_orchestrator.py
```

### Step 6: run the pipline

```
run the run_pipline.bat that runs the whole pipline
with a scheduler for every 1 hour
and error handeling when bronze is failed, silver doesn't run
```
#### Step 6.1: Configure The Scheduler
```
1. Open Task Scheduler (search in Start menu)
2. Click Create Basic Task on the right panel
3. Fill in:
   Name: GSM-R Pipeline
   Trigger: When the computer starts
4. Action: Start a program
5. Program: browse to your run_pipeline.bat
6. Start in: your project root folder path (e.g. C:\Projects\gsmr-data-pipeline-dwh)
Click Finish

7. Right-click the new task → Properties → General tab → check "Run whether user is logged on or not"
```

### Step 7: Power BI report

```
connect power bi to the postgres sql
and click the refersh button in power bi in menu bar
that gets data from the database
```

**Expected output:**
```
============================================================
Silver load started  | batch_id = silver_run_2026-05-28_14:32:15_a7f3c2
============================================================
[1/7] Loading silver.exp_etcs_call ... Done — 1256 rows
[2/7] Loading silver.exp_transaction_tracing ... Done — 8934 rows
[3/7] Loading silver.exp_vgcs_vbs_rec_tracing ... Done — 342 rows
...
Silver load finished | status=SUCCESS | total_rows=45923
============================================================
```

---

## 🔑 Key Features

### ✅ Data Lineage & Traceability
Every row is tracked with:
- `_batch_id` — Which ingestion run loaded this data
- `_source_file` — Original filename (audit trail)
- `_ingested_at` — Exact timestamp
- `_file_hash` — SHA256 hash (detect duplicates, reruns)
- `_row_num` — Original row position

### ✅ Robust Type Coercion
Handles common data quality issues:
- **Duration parsing**: `"2s 797ms"` → `2797` (milliseconds)
- **Success rates**: `"4 / 3"` → numerator, denominator, ratio (decimal)
- **Float→Integer**: `13042.0` → `13042` (no precision loss)
- **Nullable integers**: Pandas `Int64` type for NULL-safe operations

### ✅ Deduplication Strategy
Row-level deduplication using:
```sql
ROW_NUMBER() OVER (PARTITION BY [key_columns] ORDER BY _ingested_at DESC) AS rn
WHERE rn = 1
```
Keeps latest version of each logical record based on dedup key (e.g., `imsi, start_time`).

### ✅ Error Handling & Logging
- **File-level tracking**: Each file gets a status record (SUCCESS/FAILED/SKIPPED)
- **Run-level aggregation**: Total files, rows, error messages stored
- **Verbose logging**: `print()` statements + database records for debugging
- **Already-ingested detection**: Skips files seen before (hash-based)

---

## 📊 Data Quality Transformations

### Bronze → Silver Transformations

| Table | Key Transformations | Dedup Key |
|-------|---------------------|-----------|
| `exp_etcs_call` | Duration parsing, stop_time NULL handling, type coercion | `(imsi, start_time)` |
| `exp_transaction_tracing` | Duration parsing, timestamp normalization | `(imsi, start_time)` |
| `exp_vgcs_vbs_rec_tracing` | Duration + success rate splitting, NULL handling | `(functional_number, start_time, gid)` |
| `exp_ho_tracing` | No transformations (already normalized) | `(imsi, ho_start_time, src_lac, src_ci)` |
| `exp_subscriber_matrix` | Latest-version dedupe (one current profile per IMSI) | `(imsi)` |
| `exp_hdlc_frame_errors` | No transformations | `(nid_engine, frame_error_time, calling_number, direction)` |
| `sp_ertms_disconnects` | MRM integer parsing, KM decimal normalization | `(date, heure, imei)` |

---

## 🚀 Operational Commands

### Run Full Ingestion (Bronze)
```bash
python scripts/bronze/ingestion_run.py
```

### Run Silver Transformation
```bash
python scripts/silver/silver_orchestrator.py
```

### Check Ingestion Status
```sql
-- Recent runs
SELECT batch_id, start_time, status, total_files, total_rows, error_message
FROM bronze.ingest_runs
ORDER BY start_time DESC
LIMIT 10;

-- File-level details
SELECT batch_id, filename, source_system, status, row_count, error_message
FROM bronze.ingest_files
WHERE status != 'SUCCESS'
ORDER BY processed_at DESC;

-- Row counts by layer
SELECT COUNT(*) FROM silver.exp_etcs_call;
SELECT COUNT(*) FROM silver.exp_transaction_tracing;
-- ... etc
```
---

## 📄 License

This project is licensed under the **MIT License**. See the [LICENSE](LICENSE) file for details.

You are free to use, modify, and distribute this software for commercial and private purposes, provided you include the original license and copyright notice.

---

## 👤 About the Author

**Aladdin Ait Jana** I am a Data Engineering Student
**Contact & Social:**
- 📧 Email: [aladdin.aitjana@gmail.com](mailto:aladdin.aitjana@gmail.com)
- 💼 LinkedIn: [linkedin.com/in/aladdinaitjana](https://www.linkedin.com/in/aladdin-ait-jana/)
