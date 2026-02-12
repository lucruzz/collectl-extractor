from typing import List
from .node_parser import expand_nodelist_for_all
from .node_parser import verify_node_overlap
from .node_parser import get_nodes_with_io_registered
from .node_parser import verify_io_for_jobs


def run_node_pipeline(datajobs: List[tuple], config) -> list:
    datajobs = expand_nodelist_for_all(datajobs)
    not_overlaped_jobs, _ = verify_node_overlap(datajobs)
    get_nodes_with_io_registered(config.year, config.month_start, config.month_end, config.schema_db, config.inputs_collectl, config.outputs_collectl)
    jobs_with_io, _ = verify_io_for_jobs(not_overlaped_jobs, config.outputs_collectl, config.year)
    return jobs_with_io