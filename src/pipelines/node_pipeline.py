from typing import List
from .node_parser import expand_nodelist_for_all
from .node_parser import verify_node_overlap


def run_node_pipeline(datajobs: List[tuple]):
    datajobs = expand_nodelist_for_all(datajobs)
    # not_overlaped_jobs, overlaped_jobs = verify_node_overlap(datajobs)
    
    return verify_node_overlap(datajobs)