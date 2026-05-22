# Data Directory for Gold Layer

---

## Overview

The Gold layer contains **business-ready data** designed for analysis, reporting, and dashboarding. All Gold objects are **views**, not physical tables — they compute fresh from the Silver layer on every query, ensuring consistency and eliminating redundancy.

---

# Data Integration Views

## Dimension Table

### `gold.dim_subscriber`

**Purpose:**  
Master dimension containing all unique subscribers (devices/locomotives) in the network. Built by UNION-ing IMSI values across all 5 Expandium sources (ETCS calls, transactions, handovers, group calls, subscriber matrix) plus SharePoint incidents. Represents the union of all subscriber identities across the entire GSM-R network.

| Column Name | Data Type | Description |
|---|---|---|
| `subscriber_key` | INTEGER | Surrogate key (PK). Used to join fact tables to this dimension. |
| `imsi` | BIGINT | International Mobile Subscriber Identity. Universal linking key within Expandium. Example: `460041234567890` |
| `msisdn` | BIGINT | Mobile Station International ISDN Number (phone number). Example: `33123456789` |
| `imei` | BIGINT | International Mobile Equipment Identity (device ID). Links subscribers to equipment. Example: `865123456789012`. NULL for ~18% (subscriber_matrix only). |
| `nid_engine` | INTEGER | Network ID assigned to locomotive engine. Example: `42001`. NULL for ~71% (not all sources provide this). |
| `imsi_last_time` | TIMESTAMP | Timestamp of last IMSI sighting in subscriber_matrix. Example: `2026-05-13 16:44:41`. NULL if subscriber not in subscriber_matrix. |
| `msisdn_last_time` | TIMESTAMP | Timestamp of last MSISDN update in subscriber_matrix. Example: `2026-05-13 16:44:41`. NULL if subscriber not in subscriber_matrix. |

---

## Fact Tables

### `gold.fact_etcs_call`

**Purpose:**  
Records all ETCS (European Train Control System) voice calls. Primary indicator of ETCS communication health. Each row = one call session established between a locomotive and the network.

| Column Name | Data Type | Description |
|---|---|---|
| `subscriber_key` | INTEGER | FK to dim_subscriber. NULL for ~0.5% (NULL IMSI in source = infrastructure calls). |
| `imsi` | BIGINT | Subscriber identity. Degenerate dimension kept for filtering without join. Example: `460041234567890` |
| `start_time` | TIMESTAMP | Call initiation timestamp. Example: `2026-05-13 16:42:18` |
| `stop_time` | TIMESTAMP | Call termination. NULL if "Call running..." in source. Example: `2026-05-13 16:42:45` |
| `nid_engine` | INTEGER | NID of calling locomotive. Example: `42001` |
| `nid_operational` | INTEGER | Operational network ID at start. Example: `12345` |
| `etcs_baseline` | SMALLINT | ETCS version baseline. Example: `3` |
| `system_version` | DECIMAL(3,1) | Locomotive system software version. Example: `2.8` |
| `start_nid_c` | INTEGER | NID_C (control node) at call start. Example: `11001` |
| `start_nid_bg` | INTEGER | NID_BG (geographical node) at call start. Example: `21001` |
| `stop_nid_c` | INTEGER | NID_C at call end. Example: `11002` |
| `stop_nid_bg` | INTEGER | NID_BG at call end. Example: `21002` |
| `stop_d_lrbg` | INTEGER | Distance (m) from LRBG at termination. Example: `12450` |
| `gsmr_connected` | VARCHAR(50) | GSM-R layer status during call. Example: `'Yes'`, `'No'`, `'Partial'` |
| `etcs_connected` | VARCHAR(50) | ETCS layer connectivity. Example: `'Yes'` |
| `root_failure` | VARCHAR(50) | Failure classification. Enum: `'Success'`, `'Dropped'`, `'Rejected'`, `''` (unknown/ongoing). |
| `end_domain` | VARCHAR(50) | Network domain where call terminated. Example: `'RAN'`, `'CORE'` |
| `protocol_layer` | VARCHAR(50) | Protocol stack layer. Example: `'GSM'`, `'GPRS'` |
| `end_event` | VARCHAR(255) | Event that caused call end. Example: `'Disconnect'`, `'User Plane SUBSET026 Term Session'` |
| `end_cause` | VARCHAR(255) | Root cause of end event. Example: `'Normal call clearing'` |
| `isdn_port_probe` | VARCHAR(255) | ISDN port identifier. Example: `'PORT_2B'` |
| `call_setup_duration_ms` | INTEGER | Time to establish call (milliseconds). Example: `2500` (2.5 seconds) |
| `transaction_duration_ms` | INTEGER | Total call lifetime (milliseconds). Example: `27000` (27 seconds) |

---

### `gold.fact_transaction`

**Purpose:**  
All GSM-R transactions (ETCS calls, LDA data, voice). Broader than fact_etcs_call; includes non-ETCS application types. Supports breakdown by application (ETCS vs LDA) and transaction type.

| Column Name | Data Type | Description |
|---|---|---|
| `subscriber_key` | INTEGER | FK to dim_subscriber. NULL for ~47% (anonymous transactions). |
| `imsi` | BIGINT | Subscriber identity. NULL for anonymous transactions. |
| `start_time` | TIMESTAMP | Transaction start. Example: `2026-05-13 16:42:18` |
| `stop_time` | TIMESTAMP | Transaction end. NULL if ongoing. Example: `2026-05-13 16:43:05` |
| `nid_engine` | INTEGER | Locomotive NID. NULL for ~99% (not reliably reported). |
| `start_lac` | INTEGER | Location Area Code at start. Example: `5001` |
| `start_ci` | INTEGER | Cell ID at start. Example: `50010001` |
| `stop_lac` | INTEGER | LAC at end. Example: `5002` |
| `stop_ci` | INTEGER | Cell ID at end. Example: `50020001` |
| `tmsi` | VARCHAR(30) | Temporary Mobile Subscriber Identity (session token). Example: `'TMSI_ABC123'` |
| `reallocated_tmsi` | VARCHAR(30) | TMSI after reallocation (mobility event). Example: `'TMSI_XYZ789'` |
| `direction` | VARCHAR(50) | Call direction. Enum: `'Mobile Originated'`, `'Mobile Terminated'`. |
| `transaction_type` | VARCHAR(50) | Type of transaction. Enum: `'Call'` (in current data). |
| `transaction_subtype` | VARCHAR(50) | Subtype. Example: `'Voice'`, `'Data'` |
| `application_type` | VARCHAR(100) | Application layer. Enum: `'ETCS'`, `'LDA'`, `''` (unknown). |
| `functional_number` | VARCHAR(50) | Dialled or functional number. Example: `'2000'` (emergency) |
| `dest_route_address` | VARCHAR(100) | Routing address of callee. Example: `'RAN_OSLO_01'` |
| `gsmr_connected` | VARCHAR(20) | GSM-R layer available. Example: `'Yes'` |
| `priority` | VARCHAR(20) | Call priority. Example: `'High'`, `'Normal'` |
| `gb_ciphering_algo` | VARCHAR(50) | Encryption algorithm used. Example: `'A5/2'` |
| `root_failure` | VARCHAR(50) | Failure type. Enum: `'Success'`, `'Dropped'`, `'Rejected'`, `''`. |
| `protocol_layer` | VARCHAR(50) | Protocol stack layer. Example: `'GSM'` |
| `end_event` | VARCHAR(255) | Termination event. Example: `'Disconnect'` |
| `end_cause` | VARCHAR(255) | Termination reason. Example: `'Normal call clearing'` |
| `call_setup_duration_ms` | INTEGER | Setup time (milliseconds). Example: `1800` |
| `establishment_delay_ms` | INTEGER | Time from TX to network acknowledgment (milliseconds). Example: `450` |
| `transaction_duration_ms` | INTEGER | Total transaction time (milliseconds). Example: `45000` |

---

### `gold.fact_group_call`

**Purpose:**  
VGCS/VBS (group voice/data) broadcast calls. Tracks multicast communication where one initiator calls a group. Measures group call quality via SCCP, cell, and dispatcher success rates.

| Column Name | Data Type | Description |
|---|---|---|
| `subscriber_key` | INTEGER | FK to dim_subscriber (group call initiator). NULL if initiator unidentified. |
| `imsi` | BIGINT | Initiator IMSI. NULL for ~100% (current data). |
| `start_time` | TIMESTAMP | Group call activation. Example: `2026-05-13 16:42:00` |
| `stop_time` | TIMESTAMP | Group call release. Example: `2026-05-13 16:42:30` |
| `functional_number` | BIGINT | Group address (functional number). Example: `2100` (dispatcher group) |
| `gid` | INTEGER | Group ID (internal identifier). Example: `1` |
| `area` | INTEGER | Area code. Example: `5001` |
| `gcr` | BIGINT | Group Call Register ID. Example: `500100001` |
| `application_type` | VARCHAR(100) | Application. Example: `'VGCS'`, `'VBS'` |
| `transaction_type` | VARCHAR(50) | Type. Enum: `'Call'` |
| `transaction_subtype` | VARCHAR(50) | Subtype. Example: `'Voice'` |
| `priority` | VARCHAR(20) | Group call priority. Example: `'High'` |
| `start_lac` | INTEGER | Initial LAC. Example: `5001` |
| `start_ci` | INTEGER | Initial Cell ID. Example: `50010001` |
| `end_user` | VARCHAR(100) | Name/ID of group recipient. Example: `'CREW_OSLO'` |
| `sccp_success_numerator` | INTEGER | SCCP layer: calls with successful signalling. Example: `4` |
| `sccp_success_denominator` | INTEGER | SCCP total attempts. Example: `5` |
| `sccp_success_rate` | DECIMAL(5,4) | SCCP success ratio (0–1). Example: `0.8000` (80%) |
| `cell_success_numerator` | INTEGER | Cell layer: successful cell allocations. Example: `4` |
| `cell_success_denominator` | INTEGER | Cell total attempts. Example: `5` |
| `cell_success_rate` | DECIMAL(5,4) | Cell success ratio. Example: `0.8000` |
| `dispatcher_success_numerator` | INTEGER | Dispatcher: successful dispatches to group members. Example: `4` |
| `dispatcher_success_denominator` | INTEGER | Dispatcher total attempts. Example: `5` |
| `dispatcher_success_rate` | DECIMAL(5,4) | Dispatcher success ratio. Example: `0.8000` |
| `establishment_delay_ms` | INTEGER | Time to group formation (milliseconds). Example: `1200` |
| `vgcs_duration_ms` | INTEGER | Group call lifetime (milliseconds). Example: `30000` |

---

### `gold.fact_handover`

**Purpose:**  
Intra-cell and inter-cell handovers (mobility events). Tracks the seamless transfer of an active call from one base station to another as a train moves. Critical for call continuity.

| Column Name | Data Type | Description |
|---|---|---|
| `subscriber_key` | INTEGER | FK to dim_subscriber. 100% match (all IMSI in ho_tracing are in dim_subscriber). |
| `imsi` | BIGINT | Subscriber performing handover. Example: `460041234567890` |
| `start_time` | TIMESTAMP | Call start (before handover occurred). Example: `2026-05-13 16:42:00` |
| `stop_time` | TIMESTAMP | Call end. Example: `2026-05-13 16:43:00` |
| `handover_start_time` | TIMESTAMP | Handover initiation. Example: `2026-05-13 16:42:45` |
| `source_lac` | INTEGER | LAC of original cell. Example: `5001` |
| `source_ci` | INTEGER | Cell ID of original cell. Example: `50010001` |
| `target_lac` | INTEGER | LAC of target cell. Example: `5002` |
| `target_ci` | INTEGER | Cell ID of target cell. Example: `50020001` |
| `ms_power` | VARCHAR(50) | Mobile station transmit power. Example: `'20 dBm'` |
| `call_type` | VARCHAR(50) | Type of call being handed over. Example: `'Voice'`, `'Data'` |
| `handover_type` | VARCHAR(50) | Handover category. Example: `'Intra-Cell'`, `'Inter-Cell'`, `'Inter-BSC'` |
| `handover_end_event` | VARCHAR(100) | Result of handover attempt. Enum: `'HO Complete'`, `'HO Performed'`, `'HO Failure'`, `'RLC HO Failure'`. |
| `handover_end_cause` | VARCHAR(100) | Outcome detail. Example: `'Handover successful'`, `'Radio interface failure'` |
| `handover_cause` | VARCHAR(100) | Reason for handover. Example: `'Signal strength'`, `'Interference'` |
| `handover_duration_ms` | INTEGER | Time to complete handover (milliseconds). Example: `2500` (2.5 seconds). Converted from Silver (ho_duration was in seconds, multiplied by 1000). |

---

### `gold.fact_frame_error`

**Purpose:**  
HDLC frame-level transmission errors between locomotive and base station (data-link layer). High retransmission counts indicate radio degradation, interference, or equipment faults.

| Column Name | Data Type | Description |
|---|---|---|
| `subscriber_key` | INTEGER | FK to dim_subscriber (via calling_number = msisdn join). ~90% match; ~10% NULL (calling_number not in subscriber_matrix). |
| `start_time` | TIMESTAMP | Call session start. Example: `2026-05-13 16:42:00` |
| `stop_time` | TIMESTAMP | Call session end. Example: `2026-05-13 16:43:00` |
| `frame_error_time` | TIMESTAMP | Exact moment of frame error. Example: `2026-05-13 16:42:35` |
| `nid_engine` | INTEGER | Locomotive engine ID where error occurred. Example: `42001` |
| `nid_operational` | INTEGER | Operational network ID at error time. Example: `12345` |
| `isdn_port_probe` | VARCHAR(255) | ISDN port identifier on BTS. Example: `'PORT_2B'` |
| `last_nid_c` | INTEGER | Last control node ID. Example: `11001` |
| `last_nid_bg` | INTEGER | Last geographical node ID. Example: `21001` |
| `multiplexing_level` | VARCHAR(20) | Multiplexing protocol level. Example: `'L1'`, `'L2'` |
| `multiplexing_mode` | VARCHAR(50) | Mode of multiplexing. Example: `'TDM'` |
| `direction` | VARCHAR(50) | Direction of frame. Enum: `'Uplink'` (locomotive → BTS), `'Downlink'` (BTS → locomotive). |
| `frame_error` | VARCHAR(100) | Type of error detected. Example: `'CRC Error'`, `'FCS Error'` |
| `frame_error_retransmission_count` | INTEGER | Number of retransmissions triggered by this error. Example: `3` |

---

### `gold.fact_ertms_disconnect`

**Purpose:**  
Snapshots of ERTMS (European Railway Traffic Management System) disconnection incidents reported in SharePoint. Tracks network outages from a business/operational perspective. Unlike technical fact tables, this is lifecycle-oriented: incident is reported, analysed, and resolved.

| Column Name | Data Type | Description |
|---|---|---|
| `subscriber_key` | INTEGER | FK to dim_subscriber (via imei). NULL for ~71% (IMEI not in Expandium subscriber base). |
| `incident_order` | INTEGER | Sequential incident number. Example: `1`, `2`, `3`, ... (used for ordering/sequencing) |
| `is_last_7_days` | BOOLEAN | Flag: incident occurred in last 7 days. Example: `TRUE`, `FALSE` |
| `disconnect_date` | DATE | Date of disconnection. Example: `2026-05-13` |
| `disconnect_time` | TIME | Time of disconnection. Example: `16:42:35` |
| `rame` | INTEGER | Rame ID (train composition identifier). Example: `102` (100% populated in current data) |
| `numero_train` | INTEGER | Train number / service ID. Example: `4501` (98.9% populated) |
| `motrice_cab` | VARCHAR(50) | Locomotive/cabin identifier. Example: `'CAB_A'` (100% populated) |
| `mrm` | INTEGER | Multiplexing/Radio Module ID. Example: `5` (95.6% populated). NULL for equipment without this component. |
| `imei` | BIGINT | Equipment IMEI. Example: `865123456789012` (29% populated) |
| `direction_sens` | VARCHAR(50) | Train direction. Example: `'North'`, `'South'` |
| `km` | DECIMAL(8,3) | Track kilometre position at disconnect. Example: `123.450` (78% numeric; remainder text like 'Unknown') |
| `etcs_level` | VARCHAR(10) | ETCS operating level. Example: `'Level 2'`, `'Level 3'` |
| `event_type` | VARCHAR(255) | Nature of incident. Example: `'Loss of signal'`, `'Equipment failure'` |
| `intervalle` | VARCHAR(20) | Time interval code. Example: `'Peak'`, `'Off-peak'` |
| `root_cause` | TEXT | Engineering assessment of failure cause. Example: `'Infrastructure signal degradation in section 5.2-5.8'` |
| `analysis_smmrgv` | TEXT | Analysis notes by SMMRGV team. Example: `'Weather impact suspected; follow-up inspection scheduled.'` |
| `subsystem_at_fault` | VARCHAR(50) | Subsystem classification. Example: `'Radio'`, `'ETCS'`, `'Signalling'` |
| `rx_quality` | VARCHAR(50) | Received signal quality metric. Example: `'Low'`, `'5'` (1–7 scale) |
| `rx_level` | VARCHAR(100) | Received signal strength (dBm). Example: `'-95 dBm'` |
| `neighbors_10sec` | VARCHAR(10) | Count of neighboring cells visible in 10s window. Example: `'3'`, `'5'` |
| `dte_dce_com` | VARCHAR(10) | DTE-DCE communication status. Example: `'OK'`, `'FAIL'` |
| `timing_offset` | VARCHAR(50) | Clock timing discrepancy (microseconds). Example: `'42.5 μs'` |
| `hdlc_t70_retransmissions` | TEXT | HDLC/T.70 frame retransmission count. Example: `'127'` (near-saturation) |
| `bts_alarms` | VARCHAR(255) | BTS alarm status at time of incident. Example: `'Power supply low; Cooling fan failure'` |
| `action` | TEXT | Corrective action taken or planned. Example: `'Signal booster installed'` |
| `ho_execution` | VARCHAR(255) | Handover execution status during incident. Example: `'Failed'`, `'Degraded'` |
| `handled_by` | VARCHAR(100) | Team/person who processed incident. Example: `'Field_Engineer_Oslo_02'` |
| `cire` | VARCHAR(100) | CIRE (infrastructure operator) code. Example: `'CIRE_OSLO'` |
| `ciers` | VARCHAR(100) | CIERS (traction power) code. Example: `'CIERS_LINE_5'` |
| `auc_acknowledgment` | TEXT | AUC (asset utilisation centre) approval status. Example: `'Acknowledged 2026-05-14 09:15'` |
| `rbc_bug` | VARCHAR(100) | RBC (Radio Block Centre) bug/defect reference. Example: `'RBC_BUG_202605_001'` |
| `pai_links` | VARCHAR(255) | Links to PAI (Performance Analytics Interface). Example: `'https://pai.example.com/incident/1234'` |
| `pilot` | VARCHAR(100) | Pilot/contact person for resolution. Example: `'John_Smith'` |
| `resolution_deadline` | DATE | Target resolution date. Example: `2026-05-20` (7 days after incident) |
