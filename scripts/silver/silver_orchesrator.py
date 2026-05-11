import sys
from pathlib import Path
sys.path.insert(0, str(Path(__file__).resolve().parent.parent.parent))
from sqlalchemy import text
import secrets
from datetime import datetime
from scripts.bronze.get_db_engine import get_engine
from typing import Optional

SILVER_TABLES = [
    "exp_etcs_call",
    "exp_hdlc_frame_errors",
    "exp_subscriber_matrix",
    "exp_ho_tracing",
    "exp_vgcs_vbs_rec_tracing",
    "exp_transaction_tracing",
    "sp_ertms_disconnects",
]

def _create_batch_id(prefix: str = "silver_run") -> str:
    ts= datetime.now().strftime("%Y-%m-%d_%H:%M%S")
    rand= secrets.token_hex(3)
    return f"{prefix}_{ts}_{rand}"

def _record_run_start(batch_id: str) -> None:
    engine = get_engine("bronze")
    sql = text (
        """
        INSERT INTO ingest_runs(batch_id,start_time,status,total_rows,error_message)
        VALUES (:batch_id, NOW(),'RUNNING',0,NULL)
        """
    )
    with engine.begin() as conn:
        conn.execute(sql, {"batch_id":batch_id})

def _record_run_end(batch_id: str, status: str, total_rows:int, error_message:Optional[str]=None) -> None:
    engine = get_engine("bronze")
    sql = text (
        """
        UPDATE ingest_runs
        SET end_time = NOW(),
        status = :status,
        total_rows = :total_rows,
        error_message = :error_message
        WHERE batch_id = :batch_id
        """
    )
    with engine.begin() as conn:
        conn.execute(sql, {"batch_id":batch_id,
                           "status":status,
                           "total_rows":int(total_rows),
                           "error_message":error_message})
        
def _count_silver_rows() -> int:
    engine = get_engine("silver")
    total = 0
    with engine.connect() as conn:
        for table in SILVER_TABLES:
            row = conn.execute(
                text(f"SELECT COUNT(*) FROM {table}")
            ).fetchone()
            count = row[0] if row else 0
            print(f"  silver.{table:<35} is {count:>7} rows")
            total += count
    return total

def run_silver() -> None:
    batch_id = _create_batch_id()

    print(f"\n{'='*60}")
    print(f"Silver load started  | batch_id = {batch_id}")
    print(f"{'='*60}")

    _record_run_start(batch_id)
    status = "FAILED"
    total_rows = 0
    error_message = None

    try:
        engine = get_engine("silver")
        with engine.begin() as conn:
            conn.execute(text("CALL silver.load_silver()"))

        print("\nRow counts after load:")
        total_rows = _count_silver_rows()
        status = "SUCCESS"

    except Exception as e:
        error_message = f"{type(e).__name__}: {e}"
        print(f"\n[ERROR] {error_message}")

    finally:
        _record_run_end(
            batch_id=batch_id,
            status=status,
            total_rows=total_rows,
            error_message=error_message
        )

    print(f"\n{'='*60}")
    print(
        f"Silver load finished | status={status} "
        f"| total_rows={total_rows}"
    )
    print(f"{'='*60}\n")

if __name__ == "__main__":
    run_silver()
