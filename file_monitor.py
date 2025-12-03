# A.py  (repeat pattern for B.py, C.py, etc, changing only the domain specific parts)

import os
import time
import json
import argparse
from datetime import datetime
from concurrent.futures import ThreadPoolExecutor
import pandas as pd

POLL_SECONDS_DEFAULT = 5


# ------------- time helpers -------------

def to_unix(date_str, time_str):
    """Convert 'YYYY-MM-DD' and 'HH:MM:SS' into a unix timestamp."""
    ts = f"{date_str} {time_str}"
    return time.mktime(datetime.strptime(ts, "%Y-%m-%d %H:%M:%S").timetuple())


def split_ts(epoch):
    """Split an epoch seconds value into date and time strings."""
    dt = datetime.fromtimestamp(epoch)
    return dt.strftime("%Y-%m-%d"), dt.strftime("%H:%M:%S")


# ------------- domain specific for monitor A -------------

def get_monitor_files(project):
    """
    Return files to monitor for this project.

    Example: monitor *.log in data_raw/<project>/A.
    Change this to whatever A.py should watch.
    """
    import glob
    base = os.path.join("data_raw", project, "A")
    os.makedirs(base, exist_ok=True)
    pattern = os.path.join(base, "*.log")
    return glob.glob(pattern)


def parse_job_stage(file_path):
    """
    Derive job + stage from filename for A.

    Example filename: job42.stage3.log -> job='job42', stage='stage3'
    Adjust to your actual naming convention.
    """
    base = os.path.basename(file_path)
    parts = base.split(".")
    job = parts[0] if len(parts) > 0 else "job_unknown"
    stage = parts[1] if len(parts) > 1 else "stage0"
    return job, stage


def db_exists(job, stage):
    """
    Return True if DB has data for this (job, stage).

    Replace stub with real DB check.
    """
    return True


def data_extraction(file_path):
    """
    Slow extraction work.

    Runs in background worker threads.
    Replace this stub with real extraction.
    """
    time.sleep(10)


def get_data_record(file_path):
    """
    Fast data collection.

    Does not store file_path. Uses job + stage.
    Splits created and modified into date and time fields.
    """
    st = os.stat(file_path)
    job, stage = parse_job_stage(file_path)

    created_date, created_time = split_ts(st.st_ctime)
    modified_date, modified_time = split_ts(st.st_mtime)

    return {
        "job": job,
        "stage": stage,
        "created_date": created_date,
        "created_time": created_time,
        "modified_date": modified_date,
        "modified_time": modified_time,
        "size": st.st_size,
        "user": st.st_uid,
    }


# ------------- state and CSV helpers -------------

def make_key(job, stage):
    """Stable key used in state JSON and future map."""
    return f"{job}::{stage}"


def load_state(state_path):
    """Load JSON state so monitor can resume."""
    try:
        with open(state_path, "r") as f:
            return json.load(f)
    except Exception:
        return {}


def save_state(state, state_path):
    """Atomic pretty write of JSON state."""
    tmp = state_path + ".tmp"
    with open(tmp, "w") as f:
        json.dump(state, f, indent=2)
    os.replace(tmp, state_path)


def write_sorted_csv(df, csv_path):
    """Write CSV sorted by job, stage, modified_date, modified_time."""
    if df.empty:
        return
    tmp = csv_path + ".tmp"
    df.sort_values(
        ["job", "stage", "modified_date", "modified_time"],
        ascending=[True, True, True, True],
    ).to_csv(tmp, index=False)
    os.replace(tmp, csv_path)


# ------------- status and rerun logic -------------

def get_status_and_update_info(job, stage, rec, info, now_unix, is_extracting):
    """
    Compute status and update internal info:

    Status values:
      await extraction
      file running
      extracting
      file failed
      complete

    Rerun:
      New (job, stage) -> rerun = 0
      After status complete, if file modified and re extraction needed, rerun++
    """
    modified_unix = to_unix(rec["modified_date"], rec["modified_time"])
    size = rec["size"]

    last_seen_mtime = info.get("last_seen_mtime")
    last_seen_size = info.get("last_seen_size")
    last_change_time = info.get("last_change_time")
    last_extracted_mtime = info.get("last_extracted_mtime")
    last_status = info.get("last_status")
    rerun = info.get("rerun", 0)

    # track when file last changed
    if last_seen_mtime is None or modified_unix != last_seen_mtime or size != last_seen_size:
        last_change_time = now_unix

    exists = db_exists(job, stage)

    if is_extracting:
        status = "extracting"
    else:
        if exists:
            never_extracted = last_extracted_mtime is None
            changed_after_extract = (
                last_extracted_mtime is not None and modified_unix > last_extracted_mtime
            )

            if never_extracted:
                status = "await extraction"
            elif changed_after_extract:
                if last_status == "complete":
                    rerun += 1
                status = "await extraction"
            else:
                status = "complete"
        else:
            age = now_unix - (last_change_time if last_change_time is not None else now_unix)
            status = "file running" if age <= 15 * 60 else "file failed"

    info["last_seen_mtime"] = modified_unix
    info["last_seen_size"] = size
    info["last_change_time"] = last_change_time
    info["rerun"] = rerun

    return status, info


# ------------- main monitor loop -------------

def monitor_forever(project, csv_path, state_path, poll_seconds):
    """
    Monitor for one project and one monitor type (A).

    - reads and writes per project CSV and JSON
    - spawns slow extraction in background threads
    - updates rerun and status
    """
    state = load_state(state_path)
    df = pd.DataFrame()
    future_to_key = {}

    with ThreadPoolExecutor(max_workers=4) as pool:
        while True:
            now_unix = time.time()
            files = get_monitor_files(project)

            dirty_csv = False
            new_keys = 0
            seen_keys = set()

            # pass 1: fast data collection
            for file_path in files:
                rec = get_data_record(file_path)
                job = rec["job"]
                stage = rec["stage"]
                key = make_key(job, stage)
                seen_keys.add(key)

                info = state.get(key, {})
                if key not in state:
                    new_keys += 1

                is_extracting = any(
                    (k == key) and (not fut.done())
                    for fut, k in future_to_key.items()
                )

                prev_status = info.get("last_status")
                prev_mtime = info.get("last_seen_mtime")
                prev_rerun = info.get("rerun", 0)

                status, info = get_status_and_update_info(
                    job, stage, rec, info, now_unix, is_extracting
                )

                # start slow extraction if needed
                if status == "await extraction" and not is_extracting:
                    fut = pool.submit(data_extraction, file_path)
                    future_to_key[fut] = key
                    status = "extracting"

                info["last_status"] = status
                state[key] = info

                rerun = info["rerun"]
                rec["status"] = status
                rec["rerun"] = rerun

                modified_unix = to_unix(rec["modified_date"], rec["modified_time"])
                new_key_flag = prev_mtime is None
                modified_changed = prev_mtime is not None and modified_unix != prev_mtime
                status_changed = prev_status != status
                rerun_changed = prev_rerun != rerun

                if new_key_flag or modified_changed or status_changed or rerun_changed:
                    dirty_csv = True

                # upsert in df
                if df.empty:
                    df = pd.DataFrame([rec])
                else:
                    mask = (df["job"] == job) & (df["stage"] == stage)
                    if mask.any():
                        for col, val in rec.items():
                            df.loc[mask, col] = val
                    else:
                        df = pd.concat([df, pd.DataFrame([rec])], ignore_index=True)

            # pass 2: handle completed extractions
            finished = [fut for fut in list(future_to_key.keys()) if fut.done()]
            for fut in finished:
                key = future_to_key.pop(fut)
                info = state.get(key, {})
                try:
                    fut.result()
                    info["last_status"] = "complete"
                    info["last_extracted_mtime"] = info.get("last_seen_mtime")
                    new_status = "complete"
                except Exception:
                    info["last_status"] = "file failed"
                    new_status = "file failed"

                state[key] = info

                job, stage = key.split("::", 1)
                if not df.empty:
                    mask = (df["job"] == job) & (df["stage"] == stage)
                    if mask.any():
                        df.loc[mask, "status"] = new_status
                        dirty_csv = True

            if dirty_csv and not df.empty:
                write_sorted_csv(df, csv_path)

            print(
                f"[A][{project}] job_stage_monitored={len(seen_keys)} "
                f"new={new_keys} "
                f"extractions_running={len(future_to_key)}"
            )

            save_state(state, state_path)
            time.sleep(poll_seconds)


# ------------- args and entry point -------------

def parse_args():
    p = argparse.ArgumentParser()
    p.add_argument("--project", required=True)
    p.add_argument("--port", type=int, required=True)
    p.add_argument("--reset", action="store_true")
    p.add_argument("--poll-seconds", type=int, default=POLL_SECONDS_DEFAULT)
    return p.parse_args()


def main():
    args = parse_args()

    # per project, per monitor data folder
    base = os.path.join("data", args.project, "A")
    os.makedirs(base, exist_ok=True)
    csv_path = os.path.join(base, "monitor.csv")
    state_path = os.path.join(base, "monitor_state.json")

    # if reset is requested on startup, clear previous state
    if args.reset:
        for path in (csv_path, state_path):
            if os.path.exists(path):
                os.remove(path)

    monitor_forever(
        project=args.project,
        csv_path=csv_path,
        state_path=state_path,
        poll_seconds=args.poll_seconds,
    )


if __name__ == "__main__":
    main()
