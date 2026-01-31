from dataclasses import dataclass, asdict
from typing import List

@dataclass
class Job:
    jobid:      str
    jobname:    str
    account:    str
    partition:  str
    nnodes:     int
    alloccpus:  int
    start:      str
    end:        str
    elapsed:    str
    status:     str
    nodelist:   List[str]
    task:       str
    username:   str
    reqtres:    str

    def set_task(self, task):
        self.task = task
    
    def to_dict(self) -> dict:
        return asdict(self)