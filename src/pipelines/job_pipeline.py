from infrastructure.file_reader import read_job_file
from infrastructure.job_repository import JobRepository
from argparse import Namespace
from .job_parser import JobParser
from typing import List


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
        # insiro os jobs válidos
        repo.insert_jobs(jobs)

def job_fetch_db_wrapper(config: Namespace) -> List[tuple]:
    # crio um objeto repo para se comunicar com o banco
    repo = JobRepository(config)
    return repo.fetch_jobs()

def run_job_pipeline(config: Namespace) -> (List[tuple] | None):

    match config.action:
        case 'populate':
            job_populate_db_wrapper(config)
        case 'update':
            pass
        case 'export':
            return job_fetch_db_wrapper(config)
        case _:
            raise ValueError('Erro de job_pipeline.py: ação não definida!')
        
    
    # if config.output_file:
    #     writer = JobWriter(config.output_file)
    #     writer.write(jobs)

    # aqui entra:
    # salvar no banco
    # validação extra
    # estatísticas

    # print(f'[!] Number of valid jobs    : {parser.get_num_valid_jobs()}')
    # print(f'[!] Number of invalid jobs  : {parser.get_num_invalid_jobs()}')
    # print(f'[!] Total number of jobs    : {parser.get_num_invalid_jobs() + parser.get_num_valid_jobs()}')



# def export_jobs_pipeline(config):
#     repo = JobRepository(db)
#     jobs = repo.fetch_jobs()

    

#     db.close_db()
