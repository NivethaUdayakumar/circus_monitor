# generate_circus_ini.py

import json
import os

PROJECTS_CONFIG = "projects.json"
CIRCUS_INI = "circus.ini"
WORKDIR = os.path.abspath(".")  # folder where A.py, B.py, ... live

# You will run "module load python3/3.11.1" in your shell before circusd,
# so "python" here will already be Python 3.11.1.
PYTHON_BIN = "python"

# (name_prefix, script_filename)
MONITOR_TYPES = [
    ("A_monitor", "A.py"),
    ("B_monitor", "B.py"),
    ("C_monitor", "C.py"),
    ("D_monitor", "D.py"),
    ("E_monitor", "E.py"),
]


def main():
    with open(PROJECTS_CONFIG) as f:
        projects = json.load(f)

    lines = []
    lines.append("[circus]")
    lines.append("check_delay = 5")
    lines.append("endpoint = tcp://127.0.0.1:5555")
    lines.append("pubsub_endpoint = tcp://127.0.0.1:5556")
    lines.append("")

    log_dir = os.path.join(WORKDIR, "logs")
    os.makedirs(log_dir, exist_ok=True)

    for project, cfg in projects.items():
        port = cfg.get("port", 0)

        for mon_name, script in MONITOR_TYPES:
            watcher_name = f"{mon_name}_{project}"

            lines.append(f"[watcher:{watcher_name}]")
            # No auto respawn, user restarts manually via web or circusctl.
            lines.append(
                f"cmd = {PYTHON_BIN} {script} --project {project} --port {port} --reset"
            )
            lines.append(f"working_dir = {WORKDIR}")
            lines.append("numprocesses = 1")
            lines.append("autostart = true")
            # Critical part: no automatic respawn after exit
            lines.append("respawn = False")

            out_log = os.path.join(log_dir, f"{watcher_name}.out")
            err_log = os.path.join(log_dir, f"{watcher_name}.err")

            lines.append("stdout_stream.class = FileStream")
            lines.append(f"stdout_stream.filename = {out_log}")
            lines.append("stderr_stream.class = FileStream")
            lines.append(f"stderr_stream.filename = {err_log}")
            lines.append("")

    with open(CIRCUS_INI, "w") as f:
        f.write("\n".join(lines))

    print(
        f"Generated {CIRCUS_INI} with "
        f"{len(projects) * len(MONITOR_TYPES)} watchers "
        f"for {len(projects)} projects in {WORKDIR}"
    )


if __name__ == "__main__":
    main()
