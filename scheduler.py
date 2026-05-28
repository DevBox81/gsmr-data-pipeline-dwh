from pathlib import Path
import sys
import logging
from datetime import datetime
import argparse
import subprocess


ROOT = Path(__file__).resolve().parent

BRONZE_DIR = ROOT / "scripts" / "bronze"
SILVER_DIR = ROOT / "scripts" / "silver"

INGESTION_SCRIPT = BRONZE_DIR / "ingestion_run.py"
SILVER_SCRIPT = SILVER_DIR / "silver_orchesrator.py"

for p in (ROOT, BRONZE_DIR, SILVER_DIR):
    sys.path.insert(0, str(p))

LOG_DIR = ROOT / "logs"
LOG_DIR.mkdir(exist_ok=True)

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)-8s] %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
    handlers=[
        logging.FileHandler(LOG_DIR / "scheduler.log", encoding="utf-8"),
        logging.StreamHandler(sys.stdout),
    ],
)

log = logging.getLogger("gsmr_scheduler")

def run_script(script_path: Path, working_dir: Path, step_name: str) -> bool:
    """
    Runs a Python script and returns True if success.
    """

    log.info(f"{step_name} STARTED")

    try:
        subprocess.run(
            [sys.executable, str(script_path)],
            cwd=str(working_dir),
            timeout=3600,  # 1 hour timeout
            check=True,
        )

        log.info(f"{step_name} SUCCESS")
        return True

    except subprocess.TimeoutExpired:
        log.error(f"{step_name} TIMEOUT (>1h)")
        return False

    except subprocess.CalledProcessError as exc:
        log.error(f"{step_name} FAILED (exit code {exc.returncode})")
        return False

    except Exception as exc:
        log.error(f"{step_name} ERROR: {type(exc).__name__} - {exc}")
        return False

def run_pipeline():

    cycle_start = datetime.now()

    sep = "=" * 70

    log.info(sep)
    log.info(f"PIPELINE STARTED : {cycle_start}")
    log.info(sep)


    bronze_ok = run_script(
        INGESTION_SCRIPT,
        BRONZE_DIR,
        "[1/2] BRONZE INGESTION"
    )

    if bronze_ok:

        silver_ok = run_script(
            SILVER_SCRIPT,
            SILVER_DIR,
            "[2/2] SILVER LOAD"
        )

        if silver_ok:
            log.info("FULL PIPELINE SUCCESS")

        else:
            log.warning("PIPELINE PARTIALLY FAILED (Silver failed)")

    else:
        log.warning("Skipping Silver because Bronze failed")

    elapsed = (datetime.now() - cycle_start).total_seconds()

    log.info(f"PIPELINE FINISHED in {elapsed:.1f}s")
    log.info(sep)

import time

def countdown(seconds):

    while seconds > 0:

        mins, secs = divmod(seconds, 60)

        timer = f"{mins:02d}:{secs:02d}"

        print(
            f"\rNext pipeline run in: {timer}",
            end="",
            flush=True
        )

        time.sleep(1)

        seconds -= 1

    print()

if __name__ == "__main__":

    parser = argparse.ArgumentParser(
        description="GSM-R Pipeline Scheduler"
    )

    parser.add_argument(
        "--interval-minutes",
        type=int,
        default=1
    )

    parser.add_argument(
        "--now",
        action="store_true"
    )

    args = parser.parse_args()

    interval_seconds = args.interval_minutes * 60

    log.info(
        f"Scheduler started - every {args.interval_minutes} minutes"
    )

    try:

        if args.now:
            run_pipeline()

        while True:

            countdown(interval_seconds)

            run_pipeline()

    except KeyboardInterrupt:

        log.info("Scheduler stopped by user")