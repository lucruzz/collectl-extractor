import csv
from pathlib import Path

def write_jobs(path, rows):
    Path(path).parent.mkdir(parents=True, exist_ok=True)

    with open(path, "w", newline="") as f:
        writer = csv.writer(f)
        writer.writerow(
            ["jobid", "nodelist", "jobstart", "jobend", "status"]
        )
        writer.writerows(rows)