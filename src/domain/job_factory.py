from .job import Job


def make_job(fields: list) -> Job:
    jobid       = fields[0]
    jobname     = fields[1] if '.' not in jobid else None
    account     = fields[2]
    partition   = fields[3]
    nnodes      = int(fields[4])
    alloccpus   = int(fields[5])
    start       = fields[6] if fields[6] != 'Unknown' else None
    end         = fields[7] if fields[7] != 'Unknown' else None
    elapsed     = fields[8]
    status      = fields[9].split()[0]
    nodelist    = fields[10]  # [node.replace('sdumont', '') for node in fields[10]]
    username    = fields[11]
    reqtres     = fields[12]
    task        = fields[1] if len(jobid.split('.')) == 2 else None

    return Job(
        jobid,
        jobname,
        account,
        partition,
        nnodes,
        alloccpus,
        start,
        end,
        elapsed,
        status,
        nodelist,
        task,
        username,
        reqtres
    )