/"""
this file define the functions used commenly in the sharepoint and expandium ingestions
"""/

from datetime import datetime
import secrets
from get_db_engine import get_engine
from typing import Optional
from sqlalchemy import text
from pathlib import Path
import hashlib

def create_batch_id(prefix: str = "run") -> str:
    ts = datetime.now().strftime("%Y-%m-%d_%H:%M:%S")
    rand = secrets.token_hex(3)
    return f"{prefix}_{ts}_{rand}"
    '''
    format exemple : run_2026-04-04_4:21:21
    '''

def record_run_start(batch_id: str) -> None:
    engine = get_engine("bronze")
    sql = text (
        """
        INSERT INTO ingest_runs(batch_id,start_time,status,total_files,total_rows,error_message)
        VALUES (:batch_id, NOW(),'RUNNING',0,0,NULL)
        """
    )
    with engine.begin() as conn:
        conn.execute(sql, {"batch_id":batch_id})

def record_run_end(batch_id: str, status: str, total_files:int, total_rows:int, error_message:Optional[str]=None) -> None:
    engine = get_engine("bronze")
    sql = text (
        """
        UPDATE ingest_runs
        SET end_time = NOW(),
        status = :status,
        total_files = :total_files,
        total_rows = :total_rows,
        error_message = :error_message
        WHERE batch_id = :batch_id
        """
    )
    with engine.begin() as conn:
        conn.execute(sql, {"batch_id":batch_id,
                           "status":status,
                           "total_files":int(total_files),
                           "total_rows":int(total_rows),
                           "error_message":error_message})

def record_file_result(*, file_id: str, batch_id: str, source_system: str, filename: str, row_count: int, status: str, error_message: Optional[str]=None) ->None:
    engine = get_engine("bronze")

    sql = text (
        """
        INSERT INTO ingest_files(file_id,batch_id,source_system,filename,processed_at,row_count,status,error_message)
        VALUES (:file_id, :batch_id, :source_system, :filename, NOW(), :row_count, :status, :error_message)
        ON CONFLICT (file_id)
        DO UPDATE SET
        batch_id = EXCLUDED.batch_id,
        source_system = EXCLUDED.source_system,
        filename = EXCLUDED.filename,
        processed_at = EXCLUDED.processed_at,
        row_count = EXCLUDED.row_count,
        status = EXCLUDED.status,
        error_message = EXCLUDED.error_message
        """
    )
    with engine.begin() as conn:
        conn.execute(sql, {"file_id":file_id,
                           "batch_id":batch_id,
                           "source_system":source_system,
                           "filename":filename,
                           "row_count":int(row_count),
                           "status":status,
                           "error_message":error_message})

def list_files(folder: str | Path) -> list[Path]:
    folder_path = Path(folder)
    
    if not folder_path.exists():
        return []
    
    files = [p for p in folder_path.iterdir() if p.is_file()]
    files.sort(key=lambda p: p.name.lower())

    return files

def compute_hash_file(path: str | Path, algo: str="sha256"):
    p = Path(path)
    h = hashlib.new(algo)
    
    with p.open("rb") as file:
        for chunk in iter(lambda: file.read(1024*1024), b""):
            h.update(chunk)
    return h.hexdigest()

def already_ingested(file_id:str) -> bool:
    engine = get_engine("bronze")
    sql = text (
        """
        SELECT 1 FROM bronze.ingest_files
        WHERE file_id = :file_id
        AND status = 'SUCCESS'
        LIMIT 1
        """
    )
    with engine.connect() as conn:
        row = conn.execute(sql, {"file_id":file_id}).fetchone()
    return row is not None
