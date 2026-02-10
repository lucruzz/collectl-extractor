import csv
from pathlib import Path

def write_jobs(datajobs: list, inputfile: str = 'slurm_jobs.csv', outputpath: str = './outputs/slurm'):
    Path(outputpath).parent.mkdir(parents=True, exist_ok=True)
    fullpath = outputpath + "/" + inputfile

    with open(fullpath, "w", newline="") as f:
        writer = csv.writer(f)
        writer.writerow(
            ["jobid", "nodelist", "jobstart", "jobend"]
        )
        writer.writerows(datajobs)

def run_save_pipeline(config, not_overlaped_jobs: list, overlaped_jobs: list):
    write_jobs(not_overlaped_jobs, inputfile = 'not_overlaped_jobs',  outputpath = config.output)
    write_jobs(overlaped_jobs,  inputfile = 'overlaped_jobs', outputpath = config.output)