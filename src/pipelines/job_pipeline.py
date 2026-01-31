from infrastructure.file_reader import read_job_file
from infrastructure.job_repository import JobRepository
from argparse import Namespace
from .job_parser import JobParser

def run_job_pipeline(config: Namespace):
    lines = read_job_file(config.input, config.input_file)

    parser = JobParser()
    jobs = parser.parse(lines)

    # aqui entra:
    # salvar no banco
    # validação extra
    # estatísticas

    # print(f'[!] Number of valid jobs    : {parser.get_num_valid_jobs()}')
    # print(f'[!] Number of invalid jobs  : {parser.get_num_invalid_jobs()}')
    # print(f'[!] Total number of jobs    : {parser.get_num_invalid_jobs() + parser.get_num_valid_jobs()}')
    # if config.populate_db or config.update_db:
    repo = JobRepository(config)
    # repo.insert_jobs(jobs)
    print(repo.get_schema())

    # if config.output_file:
    #     writer = JobWriter(config.output_file)
    #     writer.write(jobs)



