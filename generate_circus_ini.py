import json
import os

PROJECTS_CONFIG = "projects.json"
CIRCUS_INI = "circus.ini"
WORKDIR = os.path.abspath(".")  # folder where file_monitor.py lives
PYTHON_BIN = "python"           # or full path, for example "/usr/bin/python3"


def main():
    with open(PROJECTS_CONFIG) as f:
        projects = json.load(f)

    lines = []
    lines.append("[circus]")
    lines.append("check_delay = 5")
    lines.append("endpoint = tcp://127.0.0.1:5555")
    lines.append("pubsub_endpoint = tcp://127.0.0.1:5556")
    lines.append("")

    # single monitor type: file_monitor
    for project, cfg in projects.items():
        port = cfg.get("port", 0)
        watcher_name = f"file_monitor_{project}"

        lines.append(f"[watcher:{watcher_name}]")
        # --reset ensures a clean run every time Circus (re)starts this watcher
        lines.append(
            f"cmd = {PYTHON_BIN} file_monitor.py --project {project} --port {port} --reset"
        )
        lines.append(f"working_dir = {WORKDIR}")
        lines.append("numprocesses = 1")
        lines.append("autostart = true")
        lines.append("restart = true")
        lines.append("max_retry = 5")

        # logs
        log_dir = os.path.join(WORKDIR, "logs")
        os.makedirs(log_dir, exist_ok=True)
        out_log = os.path.join(log_dir, f"{watcher_name}.out")
        err_log = os.path.join(log_dir, f"{watcher_name}.err")

        lines.append("stdout_stream.class = FileStream")
        lines.append(f"stdout_stream.filename = {out_log}")
        lines.append("stderr_stream.class = FileStream")
        lines.append(f"stderr_stream.filename = {err_log}")
        lines.append("")

    with open(CIRCUS_INI, "w") as f:
        f.write("\n".join(lines))

    print(f"Generated {CIRCUS_INI} for {len(projects)} projects in {WORKDIR}")


if __name__ == "__main__":
    main()
