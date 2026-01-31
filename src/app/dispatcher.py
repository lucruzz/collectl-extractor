from pipelines.job_pipeline import run_job_pipeline


def dispatch(config) -> None:

    match config.type:
        case 'io':
            print('Os dados de I/O serão processados!')
        case 'job':
            print('Dispatcher: Os dados de Jobs serão processados!')
            run_job_pipeline(config)
        case _:
            raise ValueError('Erro de dispatcher.py: não definido!')
