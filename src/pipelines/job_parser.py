from domain.job import Job
import domain.job_factory as jobfactory

class JobParser:
    def __init__(self) -> None:
        self.__jobs: list[Job] = []
        self.__cancelled_jobs: set[str] = set()
        self.__job_index: dict[str, Job] = {}
        self.__num_valid_jobs = 0
        self.__num_invalid_jobs = 0

    def parse(self, lines: list[str]) -> list[Job]:
        self.__jobs.clear()
        self.__cancelled_jobs.clear()
        self.__job_index.clear()

        if 'JobID|JobName|Account|Partition|NNodes|AllocCPUS|Start|End|Elapsed|State|NodeList|User|ReqTRES' in lines[0]:
            lines.pop(0)

        self.__get_invalid_jobs(lines)
        self.__get_valid_jobs(lines)

        return self.__jobs
    
    def get_num_valid_jobs(self):
        return self.__num_valid_jobs
    
    def get_num_invalid_jobs(self):
        return self.__num_invalid_jobs
    

    def __get_invalid_jobs(self, lines: list) -> None:
        for line in lines:
            fields = line.strip().split("|")
            if len(fields) < 13:
                continue

            jobid = fields[0].split('.')[0]
            status = fields[9].split()[0]

            # pego também os jobid dos jobs com "falso-positivo" em seu status
            # Existem subtasks que falharam, mas as tasks principais tem status COMPLETED
            # if '.' in jobid and ('CANCELLED' in status or 'FAILED' in status or 'TIMEOUT' in status):
            if ('CANCELLED' in status or
                    'FAILED' in status or
                    'TIMEOUT' in status or
                    'NODE_FAIL' in status or
                    'OUT_OF_MEMORY' in status or
                    'PENDING' in status or
                    'REQUEUED' in status or
                    'RUNNING' in status):
                self.__cancelled_jobs.add(jobid)

            # se for array job
            if '_' in jobid or '+' in jobid:
                self.__cancelled_jobs.add(jobid)
        
        self.__set_num_invalid_jobs()

    def __get_valid_jobs(self, lines: list) -> None:
        for line in lines:

            fields = line.strip().split("|")
            if len(fields) < 13 or len(fields) > 13:
                continue

            if fields[0].split('.')[0] in self.__cancelled_jobs:
                continue

            if '.' in fields[0]:
                # main_jobid = next((job for job in self.__jobs if job.jobid == fields[0].split('.')[0]), None)
                main_jobid = self.__job_index.get(fields[0].split('.')[0]) # tento recuperar do dicionario o objeto do job principal com o id, que é a chave do dict
                # se o jobid for encontrado
                if main_jobid is not None:
                    main_jobid.task = fields[1] if fields[1] != 'batch' else 'NULL'
                    continue
            
            # se passar direto do if acima, então não é uma subtask do job então posso adicionar os valores normalmente
            job = jobfactory.make_job(fields) # crio um objeto Job
            self.__jobs.append(job) # adiciono esse objeto na lista de jobs
            self.__job_index[job.jobid] = job # uso o jobid como chave do mapa de dicionario de jobs e adiciono o objeto job como conteudo
        
        self.__set_num_valid_jobs()

    def __set_num_valid_jobs(self):
        self.__num_valid_jobs = len(self.__jobs)

    def __set_num_invalid_jobs(self):
        self.__num_invalid_jobs = len(self.__cancelled_jobs)
