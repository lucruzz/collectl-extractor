from infrastructure.file_reader import read_job_file
from infrastructure.job_repository import JobRepository
from argparse import Namespace
from .job_parser import JobParser
from typing import List
from pipelines.node_pipeline import run_node_pipeline
from infrastructure.job_writer import run_save_pipeline
from utils.logging import log_info


def job_populate_db_wrapper(config: Namespace) -> None:
    # se for para popular ou atualizar o banco
    if config.populate_db or config.update_db:
        # leio o arquivo de entrada PSV que contém os dados do slurm
        lines = read_job_file(config.input, config.input_file)
        # crio um objeto parser
        parser = JobParser()
        # parseio as linhas pegando os jobs válidos
        jobs = parser.parse(lines)
        # crio um objeto repo para se comunicar com o banco
        repo = JobRepository(config)
        # insiro os jobs válidos no BD
        repo.insert_jobs(jobs)

def job_fetch_db_wrapper(config: Namespace) -> List[tuple]:
    # crio um objeto repo para se comunicar com o banco
    repo = JobRepository(config)
    datajobs = repo.fetch_jobs(config.year, config.month_start, config.month_end)
    jobs_with_io = run_node_pipeline(datajobs, config)
    run_save_pipeline(config, jobs_with_io)

def run_job_pipeline(config: Namespace) -> (List[tuple] | None):

    match config.action:
        case 'populate':
            log_info('JobPipeline: Database will be populated!')
            job_populate_db_wrapper(config)
        case 'update':
            pass
        case 'extract':
            return job_fetch_db_wrapper(config)
        case _:
            raise ValueError('Erro de job_pipeline.py: ação não definida!')


# print(f'[!] Number of valid jobs    : {parser.get_num_valid_jobs()}')
# print(f'[!] Number of invalid jobs  : {parser.get_num_invalid_jobs()}')
# print(f'[!] Total number of jobs    : {parser.get_num_invalid_jobs() + parser.get_num_valid_jobs()}')