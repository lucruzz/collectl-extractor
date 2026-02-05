from pipelines.job_pipeline import run_job_pipeline
from utils.logging import log_info


def dispatch(config) -> None:

    match config.type:
        case 'io':
            log_info('Os dados de I/O serão processados!')
        case 'job':
            log_info('Dispatcher: Os dados de Jobs serão processados!')
            run_job_pipeline(config)
        case _:
            raise ValueError('Erro de dispatcher.py: não definido!')
