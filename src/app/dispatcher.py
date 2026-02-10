from pipelines.job_pipeline import run_job_pipeline
from pipelines.node_pipeline import run_node_pipeline
from utils.logging import log_info
from infrastructure.job_writer import run_save_pipeline


def dispatch(config) -> None:

    match config.type:
        case 'io':
            log_info('Os dados de I/O serão processados!')
        case 'job':
            log_info('Dispatcher: Job datas will be processed!')
            datajobs = run_job_pipeline(config)
            not_overlaped_jobs, overlaped_jobs = run_node_pipeline(datajobs)
            run_save_pipeline(config, not_overlaped_jobs, overlaped_jobs)
        case _:
            raise ValueError('Erro de dispatcher.py: não definido!')
