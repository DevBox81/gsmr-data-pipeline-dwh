from sharepoint_ingestion import run as sharepoint_run
from expandium_ingestion import run as expandium_run
from common import record_run_start, record_run_end, create_batch_id

def run_ingestion() -> None:
    batch_id = create_batch_id()
    record_run_start(batch_id)
    top_error = None

    try:
        ExpandiumStats = expandium_run(batch_id)
        SharepointStats = sharepoint_run(batch_id)

        files_total = ExpandiumStats.files_total + SharepointStats.files_total
        total_success = ExpandiumStats.files_success + SharepointStats.files_success
        total_skipped = ExpandiumStats.files_skipped + SharepointStats.files_skipped
        total_failed = ExpandiumStats.files_failed + SharepointStats.files_failed
        rows_loaded = ExpandiumStats.rows_loaded + SharepointStats.rows_loaded

        if total_failed == 0:
            status = "SUCCESS"
        elif total_success > 0:
            status = "PARTIAL"
        else:
            status = "FAILED"

    except Exception as e:
        status = "FAILED"
        files_total = 0
        rows_loaded = 0
        top_error = str(e)

    record_run_end(
        batch_id=batch_id,
        status=status,
        total_files=files_total,
        total_rows=rows_loaded,
        error_message=top_error
    )

    print(
        f"the file {batch_id}, status: {status}, \n"
        f"total files = {files_total}, total rows = {rows_loaded}, \n"
        f"total success = {total_success}, total failed = {total_failed}, total skipped = {total_skipped}"
    )


if __name__ == "__main__":
    run_ingestion()
        

