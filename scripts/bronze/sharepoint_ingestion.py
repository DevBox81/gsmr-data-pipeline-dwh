/"""
this code is for the ingestions of sharepoint data into the database, it hashs a filem check if it's already ingested, then it reads the excel file, renames the
columns to much the ones on the database, adds the metadata then it loads it and it records the file results, while recording the stats of the ingestion,
the total files, the successed files, skipped and failed files, also how many rows are loaded, and if there's an error (an exception) it records it as
in "FAILED" status and you can see the error message in the ingest_files table in the error message column
"""/

from pathlib import Path
from dataclasses import dataclass
import pandas as pd
from get_db_engine import get_engine
from common import list_files, compute_hash_file, already_ingested, record_file_result
import re

SOURCE_FOLDER = Path(__file__).resolve().parent.parent.parent / "datasets" / "sharepoint"
SP_ERTMS_DECONNIXIONS_TABLE = "sp_ertms_disconnects"

@dataclass
class SharepointStats:
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

def _rename_columns(df: pd.DataFrame) -> pd.DataFrame:
    df = df.copy()

    column_map = {
        "N° ordre": "nombre_ordre",
        "7 derniers jours?": "derniere_7_jours",
        "Date": "date",
        "Heure": "heure",
        "N° Train": "numero_train",
        "Rame": "rame",
        "Motrice / Cab": "motrice_cab",
        "MRM": "mrm",
        "IMEI": "imei",
        "Sens": "sens",
        "Km": "km",
        "Intervalle": "intervalle",
        "Niveau ETCS": "niveau_etcs",
        "Evénement": "evenement",
        "Cause_racine": "cause_racine",
        "Analyse SMMRGV": "analyse_smmrgv",
        "Sous Système mis en cause": "sous_systeme_mis_en_cause",
        "Action": "action",
        "Execution HO": "execution_ho",
        "RxQual": "rxqual",
        "RxLev": "rxlev",
        "Voisinage (10sec)": "voisinage_10sec",
        "Com DTE-DCE": "com_dte_dce",
        "Ecart": "ecart",
        "Retransmission trames HDLC/T.70": "retransmission_trames_hdlc_t70",
        "Alarmes BTS": "alarmes_bts",
        "Traité par": "traite_par",
        "CIRE": "cire",
        "CIERS": "ciers",
        "Acquittement AUC": "acquittement_auc",
        "BUG RBC": "bug_rbc",
        "Liens PAI": "liens_pai",
        "Pilote": "pilote",
        "Échéance": "echeance",
    }

    df.rename(columns=column_map, inplace=True)
    return df

def _data_load(df: pd.DataFrame) -> int:
    engine = get_engine("bronze")
    df.to_sql(SP_ERTMS_DECONNIXIONS_TABLE, engine, if_exists="append", index=False, method="multi")
    return int(len(df))

def run(batch_id:str, source_file: str = "sharepoint") -> SharepointStats:
    stats = SharepointStats()
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
            df = pd.read_excel(path, sheet_name='Data')
            df = _rename_columns(df)
            df = _add_metadata(df, batch_id=batch_id, source_file=path.name, file_hash=file_hash)
            rows = _data_load(df)

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
