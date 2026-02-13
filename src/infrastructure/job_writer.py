import csv
from pathlib import Path
from argparse import Namespace


def write_jobs(datajobs: list, prefix: str, outputpath: str = './outputs/slurm'):
    Path(outputpath).parent.mkdir(parents=True, exist_ok=True)
    fullpath = outputpath + "/" + prefix + "_jobs_with_io.csv"

    with open(fullpath, "w", newline="") as f:
        writer = csv.writer(f)
        writer.writerow(
            ["jobid", "nodelist", "jobstart", "jobend", "possibleio"]
        )
        writer.writerows(datajobs)

def run_save_pipeline(config: Namespace, jobs_with_io: list):
    write_jobs(jobs_with_io, config.schema_db, outputpath = config.output)