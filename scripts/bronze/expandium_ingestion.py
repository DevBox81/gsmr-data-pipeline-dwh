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

    #clicking the tracking tab
    wait.until(EC.element_to_be_clickable(
        (By.CSS_SELECTOR, "[title='Tracing']")
    )).click()

    def click_tab_and_download(tab_title: str) -> None:
        #this clicks a tab like etcs call tracing and waits for the loading then click the download csv
        wait.until(EC.element_to_be_clickable(
            (By.CSS_SELECTOR, f"[title='{tab_title}']")
        )).click()
        wait.until(EC.invisibility_of_element_located(
            (By.CSS_SELECTOR, "div.table-component-loader")
        ))
        wait.until(EC.element_to_be_clickable(
            (By.CSS_SELECTOR, "[title='Download in CSV format']")
        )).click()
 
    click_tab_and_download('ETCS Call tracing')
    click_tab_and_download("Transaction tracing")
    click_tab_and_download("VGCS / VBS / REC tracing")
    click_tab_and_download("HO tracing")
    click_tab_and_download("Subscriber Matrix")
    click_tab_and_download("HDLC Frame Error tracing")


    time.sleep(5)
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

    df.columns = df.columns.str.strip().str.lower()

    if "etcs" in name and "call" in name:
        column_map = {
            "start time": "start_time", "stop time": "stop_time",
            "call setup duration (ms)": "call_setup_duration",
            "transaction duration (ms)": "transaction_duration",
            "etcs baseline": "etcs_baseline", "system version": "system_version",
            "nid_engine": "nid_engine", "nid_operational": "nid_operational",
            "imsi": "imsi", "msisdn": "msisdn", "imei": "imei",
            "calling number": "calling_number", "called number": "called_number",
            "gsm-r connected": "gsmr_connected", "etcs connected": "etcs_connected",
            "start nid_c": "start_nid_c", "start nid_bg": "start_nid_bg",
            "stop nid_c": "stop_nid_c", "stop nid_bg": "stop_nid_bg",
            "stop d_lrbg": "stop_d_lrbg", "root failure": "root_failure",
            "end domain": "end_domain", "protocol layer": "protocol_layer",
            "end event": "end_event", "end cause": "end_cause",
            "isdn port probe": "isdn_port_probe",
        }

    elif "transaction" in name:
        column_map = {
            "start time": "start_time", "stop time": "stop_time",
            "call setup duration (ms)": "call_setup_duration",
            "establishment delay (ms)": "establishment_delay",
            "transaction duration (ms)": "transaction_duration",
            "start lac": "start_lac", "start ci": "start_ci",
            "stop lac": "stop_lac", "stop ci": "stop_ci",
            "nid_engine": "nid_engine", "nid_operational": "nid_operational",
            "tmsi": "tmsi", "reallocated tmsi": "reallocated_tmsi",
            "imsi": "imsi", "msisdn": "msisdn", "imei": "imei",
            "calling number": "calling_number", "called number": "called_number",
            "dest. route add.": "dest_route_address",
            "functional number": "functional_number",
            "functional number ct": "functional_number_ct",
            "direction": "direction", "transaction type": "transaction_type",
            "transaction subtype": "transaction_subtype",
            "application type": "application_type",
            "gsm-r connected": "gsmr_connected", "priority": "priority",
            "root failure": "root_failure", "protocol layer": "protocol_layer",
            "end event": "end_event", "end cause": "end_cause",
            "gb ciphering algo": "gb_ciphering_algo",
        }

    elif "vgcs" in name and "vbs" in name:
        column_map = {
            "start time": "start_time", "stop time": "stop_time",
            "application type": "application_type",
            "transaction type": "transaction_type",
            "transaction subtype": "transaction_subtype",
            "functional number": "functional_number",
            "imsi": "imsi", "tmsi": "tmsi", "msisdn": "msisdn",
            "gid": "gid", "area": "area", "gcr": "gcr", "priority": "priority",
            "start lac": "start_lac", "start ci": "start_ci",
            "establishment delay (ms)": "establishment_delay",
            "sccp success rate": "sccp_success_rate",
            "cell success rate": "cell_success_rate",
            "dispatcher success rate": "dispatcher_success_rate",
            "end user": "end_user", "vgcs duration (ms)": "vgcs_duration",
        }

    elif "ho" in name:
        column_map = {
            "start time": "start_time", "stop time": "stop_time",
            "ho start time": "ho_start_time", "ho duration": "ho_duration",
            "src lac": "src_lac", "src ci": "src_ci",
            "trg lac": "trg_lac", "trg ci": "trg_ci",
            "imsi": "imsi", "msisdn": "msisdn", "imei": "imei",
            "ms power": "ms_power", "call type": "call_type", "ho type": "ho_type",
            "ho end event": "ho_end_event", "ho end cause": "ho_end_cause",
            "ho cause": "ho_cause",
        }

    elif "subscriber" in name and "matrix" in name:
        column_map = {
            "imsi": "imsi", "last imsi time": "last_imsi_time",
            "msisdn": "msisdn", "last msisdn time": "last_msisdn_time",
            "nid_engine": "nid_engine", "last nid_engine time": "last_nid_engine_time",
            "fn ct3": "fn_ct3", "last fn ct3 time": "last_fn_ct3_time",
            "fn ct4": "fn_ct4", "last fn ct4 time": "last_fn_ct4_time",
        }

    elif "hdlc" in name and "frame" in name:
        column_map = {
            "start time": "start_time", "stop time": "stop_time",
            "frame error time": "frame_error_time", "isdn port probe": "isdn_port_probe",
            "nid_engine": "nid_engine", "nid_operational": "nid_operational",
            "calling number": "calling_number", "called number": "called_number",
            "last nid_c": "last_nid_c", "last nid_bg": "last_nid_bg",
            "m level": "m_level", "m_mode": "m_mode", "direction": "direction",
            "frame error": "frame_error",
            "frame error retransmission count": "frame_error_retransmission_count",
        }
    
    df = df.rename(columns=lambda x: column_map.get(x, x))
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
