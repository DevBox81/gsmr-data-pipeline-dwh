from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.chrome.options import Options
from pathlib import Path
import time
from dataclasses import dataclass
import pandas as pd
from get_db_engine import get_engine
from common import list_files, compute_hash_file, already_ingested, record_file_result


def auto_data_scraping():
    download_path = Path(__file__).resolve().parent.parent.parent / "datasets" / "expandium"
    for item in download_path.iterdir():
        if item.is_file():
            item.unlink()

    chrome_options = Options()
    chrome_options.add_experimental_option("prefs", {
        "download.default_directory": str(download_path),
        "download.prompt_for_download": False,
        "download.directory_upgrade": True,
        "safebrowsing.enabled": True
    })

    driver = webdriver.Chrome(options=chrome_options)
    wait = WebDriverWait(driver, 15)

    driver.get("http://127.0.0.1:8014/qats/#/railway")

    #login in
    wait.until(EC.presence_of_element_located(
        (By.CSS_SELECTOR, "input[placeholder='Username']")
    )).send_keys("Démo")

    wait.until(EC.presence_of_element_located(
        (By.CSS_SELECTOR, "input[placeholder='Password']")
    )).send_keys("Oncf2026@!")

    wait.until(EC.element_to_be_clickable(
        (By.CSS_SELECTOR, ".btn.btn-lg.btn-block.btn-success")
    )).click()

    #getting the data
    wait.until(EC.element_to_be_clickable(
        (By.CSS_SELECTOR, "[title='Tracing']")
    )).click()

    #ETCS Calls
    wait.until(EC.element_to_be_clickable(
        (By.CSS_SELECTOR, "[title='ETCS Call tracing']")
    )).click()

    wait.until(EC.element_to_be_clickable(
        (By.CSS_SELECTOR, "[title='Download in CSV format']")
    )).click()

    #Transaction
    wait.until(EC.element_to_be_clickable(
        (By.CSS_SELECTOR, "[title='Transaction tracing']")
    )).click()

    wait.until(EC.element_to_be_clickable(
        (By.CSS_SELECTOR, "[title='Download in CSV format']")
    )).click()

    #VGCS / VBS / REC
    wait.until(EC.element_to_be_clickable(
        (By.CSS_SELECTOR, "[title='VGCS / VBS / REC tracing']")
    )).click()

    wait.until(EC.element_to_be_clickable(
        (By.CSS_SELECTOR, "[title='Download in CSV format']")
    )).click()

    #HO
    wait.until(EC.element_to_be_clickable(
        (By.CSS_SELECTOR, "[title='HO tracing']")
    )).click()

    wait.until(EC.element_to_be_clickable(
        (By.CSS_SELECTOR, "[title='Download in CSV format']")
    )).click()

    #Subscriber Matrix
    wait.until(EC.element_to_be_clickable(
        (By.CSS_SELECTOR, "[title='Subscriber Matrix']")
    )).click()

    wait.until(EC.element_to_be_clickable(
        (By.CSS_SELECTOR, "[title='Download in CSV format']")
    )).click()

    #HDLC Frame Error
    wait.until(EC.element_to_be_clickable(
        (By.CSS_SELECTOR, "[title='HDLC Frame Error tracing']")
    )).click()

    wait.until(EC.element_to_be_clickable(
        (By.CSS_SELECTOR, "[title='Download in CSV format']")
    )).click()

    time.sleep(10)
    driver.quit()

SOURCE_FOLDER = Path(__file__).resolve().parent.parent.parent / "datasets" / "expandium"
EXP_ETCS_CALL_TABLE = "exp_etcs_call"
EXP_TRANSACTION_TRACING_TABLE = "exp_transaction_tracing"
EXP_VGCS_VBS_REC_TRACING_TABLE = "exp_vgcs_vbs_rec_tracing"
EXP_HO_TRACING_TABLE = "exp_ho_tracing"
EXP_SUBSCRIBER_MATRIX_TABLE = "exp_subscriber_matrix"
EXP_HDLC_FRAME_ERRORS_TABLE = "exp_hdlc_frame_errors"

@dataclass
class ExpandiumStats:
    files_total : int = 0
    files_success : int = 0
    files_skipped : int = 0
    files_failed : int = 0
    rows_loaded : int = 0

def _add_metadata(df: pd.DataFrame, batch_id: str, source_file: str, file_hash: str) -> pd.DataFrame:
    
    df = df.copy()
    df["_batch_id"] = batch_id
    df["_source_file"] = source_file
    df["_ingested_at"] = pd.Timestamp.now()
    df["_row_num"] = range(1, len(df) + 1)
    df["_file_hash"] = file_hash
    return df

def _ETCS_CALL_data_load(df: pd.DataFrame) -> int:
    engine = get_engine("bronze")
    df.to_sql(EXP_ETCS_CALL_TABLE, engine, if_exists="append", index=False, method="multi")
    return int(len(df))

def _TRANSACTION_TRACING_data_load(df: pd.DataFrame) -> int:
    engine = get_engine("bronze")
    df.to_sql(EXP_TRANSACTION_TRACING_TABLE, engine, if_exists="append", index=False, method="multi")
    return int(len(df))

def _VGCS_VBS_REC_TRACING_data_load(df: pd.DataFrame) -> int:
    engine = get_engine("bronze")
    df.to_sql(EXP_VGCS_VBS_REC_TRACING_TABLE, engine, if_exists="append", index=False, method="multi")
    return int(len(df))

def _HO_TRACING_data_load(df: pd.DataFrame) -> int:
    engine = get_engine("bronze")
    df.to_sql(EXP_HO_TRACING_TABLE, engine, if_exists="append", index=False, method="multi")
    return int(len(df))

def _SUBSCRIBER_MATRIX_data_load(df: pd.DataFrame) -> int:
    engine = get_engine("bronze")
    df.to_sql(EXP_SUBSCRIBER_MATRIX_TABLE, engine, if_exists="append", index=False, method="multi")
    return int(len(df))

def _HDLC_FRAME_ERRORS_data_load(df: pd.DataFrame) -> int:
    engine = get_engine("bronze")
    df.to_sql(EXP_HDLC_FRAME_ERRORS_TABLE, engine, if_exists="append", index=False, method="multi")
    return int(len(df))

def _which_data_load(path: Path):
    name = path.name.lower()

    if "etcs" in name and "call" in name:
        return _ETCS_CALL_data_load
    elif "transaction" in name:
        return _TRANSACTION_TRACING_data_load
    elif "vgcs" in name and "vbs" in name:
        return _VGCS_VBS_REC_TRACING_data_load
    elif "ho" in name:
        return _HO_TRACING_data_load
    elif "subscriber" in name and "matrix" in name:
        return _SUBSCRIBER_MATRIX_data_load
    elif "hdlc" in name and "frame" in name:
        return _HDLC_FRAME_ERRORS_data_load
    
    raise ValueError(f"No matching load function for file: {path.name}")

def _rename_columns(df: pd.DataFrame, path: Path):
    df = df.copy()
    name = path.name.lower()

    if "etcs" in name and "call" in name:
        column_map = {
            "Start Time": "start_time", "Stop Time": "stop_time",
            "Call Setup Duration (ms)": "call_setup_duration",
            "Transaction Duration (ms)": "transaction_duration",
            "ETCS Baseline": "etcs_baseline", "System Version": "system_version",
            "NID_ENGINE": "nid_engine", "NID_OPERATIONAL": "nid_operational",
            "IMSI": "imsi", "MSISDN": "msisdn", "IMEI": "imei",
            "Calling Number": "calling_number", "Called Number": "called_number",
            "GSM-R Connected": "gsmr_connected", "ETCS Connected": "etcs_connected",
            "Start NID_C": "start_nid_c", "Start NID_BG": "start_nid_bg",
            "Stop NID_C": "stop_nid_c", "Stop NID_BG": "stop_nid_bg",
            "Stop D_LRBG": "stop_d_lrbg", "Root failure": "root_failure",
            "End Domain": "end_domain", "Protocol Layer": "protocol_layer",
            "End Event": "end_event", "End Cause": "end_cause",
            "ISDN Port Probe": "isdn_port_probe",
        }
    
    elif "transaction" in name:
        column_map = {
            "Start Time": "start_time", "Stop Time": "stop_time",
            "Call Setup Duration (ms)": "call_setup_duration",
            "Establishment Delay (ms)": "establishment_delay",
            "Transaction Duration (ms)": "transaction_duration",
            "Start LAC": "start_lac", "Start CI": "start_ci",
            "Stop LAC": "stop_lac", "Stop CI": "stop_ci",
            "NID_ENGINE": "nid_engine", "NID_OPERATIONAL": "nid_operational",
            "TMSI": "tmsi", "Reallocated TMSI": "reallocated_tmsi",
            "IMSI": "imsi", "MSISDN": "msisdn", "IMEI": "imei",
            "Calling Number": "calling_number", "Called Number": "called_number",
            "Dest. Route Add.": "dest_route_address",
            "Functional Number": "functional_number",
            "Functional Number CT": "functional_number_ct",
            "Direction": "direction", "Transaction Type": "transaction_type",
            "Transaction Subtype": "transaction_subtype",
            "Application Type": "application_type",
            "GSM-R Connected": "gsmr_connected", "Priority": "priority",
            "Root failure": "root_failure", "Protocol Layer": "protocol_layer",
            "End Event": "end_event", "End Cause": "end_cause",
            "gb_ciphering_algo": "gb_ciphering_algo",
        }

    elif "vgcs" in name and "vbs" in name:
        column_map = {
            "Start Time": "start_time", "Stop Time": "stop_time",
            "Application Type": "application_type",
            "Transaction Type": "transaction_type",
            "Transaction Subtype": "transaction_subtype",
            "Functional Number": "functional_number",
            "IMSI": "imsi", "TMSI": "tmsi", "MSISDN": "msisdn",
            "GID": "gid", "AREA": "area", "GCR": "gcr", "Priority": "priority",
            "Start LAC": "start_lac", "Start CI": "start_ci",
            "Establishment Delay (ms)": "establishment_delay",
            "SCCP Success Rate": "sccp_success_rate",
            "Cell Success Rate": "cell_success_rate",
            "Dispatcher Success Rate": "dispatcher_success_rate",
            "End User": "end_user", "VGCS Duration (ms)": "vgcs_duration",
        }

    elif "ho" in name:
        column_map = {
            "Start Time": "start_time", "Stop Time": "stop_time",
            "HO Start Time": "ho_start_time", "HO Duration": "ho_duration",
            "Src LAC": "src_lac", "Src CI": "src_ci",
            "Trg LAC": "trg_lac", "Trg CI": "trg_ci",
            "IMSI": "imsi", "MSISDN": "msisdn", "IMEI": "imei",
            "MS Power": "ms_power", "Call Type": "call_type", "HO Type": "ho_type",
            "HO End Event": "ho_end_event", "HO End Cause": "ho_end_cause",
            "HO Cause": "ho_cause",
        }
    
    elif "subscriber" in name and "matrix" in name:
        column_map = {
            "IMSI": "imsi", "Last IMSI Time": "last_imsi_time",
            "MSISDN": "msisdn", "Last MSISDN Time": "last_msisdn_time",
            "NID_ENGINE": "nid_engine", "Last NID_ENGINE Time": "last_nid_engine_time",
            "FN CT3": "fn_ct3", "Last FN CT3 Time": "last_fn_ct3_time",
            "FN CT4": "fn_ct4", "Last FN CT4 Time": "last_fn_ct4_time",
        }

    elif "hdlc" in name and "frame" in name:
        column_map = {
            "Start Time": "start_time", "Stop Time": "stop_time",
            "Frame Error Time": "frame_error_time", "ISDN Port Probe": "isdn_port_probe",
            "NID_ENGINE": "nid_engine", "NID_OPERATIONAL": "nid_operational",
            "Calling Number": "calling_number", "Called Number": "called_number",
            "Last NID_C": "last_nid_c", "Last NID_BG": "last_nid_bg",
            "M Level": "m_level", "M_MODE": "m_mode", "Direction": "direction",
            "Frame Error": "frame_error",
            "Frame Error Retransmission Count": "frame_error_retransmission_count",
        }
    
    df = df.rename(columns=lambda x: column_map.get(x.strip(), x.strip()))
    return df

def _correct_float_columns(df: pd.DataFrame) -> pd.DataFrame:
    df = df.copy()
    for col in df.columns:
        if df[col].dtype == "float64":
            non_null = df[col].dropna()
            if len(non_null) == 0 or (non_null % 1 == 0).all():
                df[col] = df[col].astype("Int64")
    return df

def run(batch_id:str, source_file: str = "expandium") -> ExpandiumStats:
    auto_data_scraping()
    stats = ExpandiumStats()
    files = list_files(SOURCE_FOLDER)

    for path in files:
        file_hash = compute_hash_file(path)
        stats.files_total += 1

        if already_ingested(file_hash):
            record_file_result(
                file_id=file_hash,
                batch_id=batch_id,
                source_system=source_file,
                filename=str(path.name),
                row_count=0,
                status="SKIPPED",
                error_message=None
            )
            stats.files_skipped +=1
            continue

        try:
            with open(path, "r", encoding="utf-8-sig") as f:
                first_line = f.readline().strip()

            skiprows = 1 if "sep=" in first_line else 0
            df = pd.read_csv(path, sep=";", skiprows=skiprows, encoding="utf-8-sig")

            df = _rename_columns(df,path)
            df = _correct_float_columns(df)
            df = _add_metadata(df, batch_id=batch_id, source_file=path.name, file_hash=file_hash)
            data_load = _which_data_load(path)
            rows = data_load(df)

            record_file_result(
                file_id=file_hash,
                batch_id=batch_id,
                source_system=source_file,
                filename=str(path.name),
                row_count=rows,
                status="SUCCESS",
                error_message=None
            )

            stats.files_success += 1
            stats.rows_loaded += rows

        except Exception as e:
            print(f"error on {path.name}: {type(e).__name__}: {e}")
            record_file_result(
                file_id=file_hash,
                batch_id=batch_id,
                source_system=source_file,
                filename=str(path.name),
                row_count=0,
                status="FAILED",
                error_message=str(e)
            )
            stats.files_failed += 1

    return stats

if __name__ == "__main__":
    from common import create_batch_id, record_run_start, record_run_end
    bid = create_batch_id()
    record_run_start(bid)
    s = run(bid)

    status = "SUCCESS"
    if s.files_failed > 0 and s.files_success > 0:
        status = "PARTIAL"
    elif s.files_failed > 0 and s.files_success == 0:
        status = "FAILED"

    record_run_end(
        batch_id=bid,
        status=status,
        total_files=s.files_total,
        total_rows=s.rows_loaded,
        error_message=None,
    )

    print(
        f"[{bid}] status={status} total={s.files_total} "
        f"success={s.files_success} failed={s.files_failed} "
        f"skipped={s.files_skipped} rows={s.rows_loaded}"
    )
