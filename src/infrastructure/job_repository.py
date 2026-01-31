# import psycopg2
# from psycopg2 import sql
from .database import Database
# from domain import Job


class JobRepository:

    def __init__(self, config):
        self.db = Database(config)
        self._schema = config.schema_db
    
    def get_schema(self):
        return self._schema

    # def insert_jobs(self, jobs: list[Job]):
    #     self.db.connect_db()

    #     for job in jobs:
    #         self._insert_job(job)

    #     self.db.close_db()

    # def insert_job():
    #     q = f"""
    #         INSERT INTO {self.db.sch}.{table_name} 
    #         (JobID, JobName, Account, Partition, NNodes, AllocCPUs, JobStart, JobEnd, Elapsed, Status, 
    #         Nodelist, Task, Username, ReqTRES) 
    #         VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s) 
    #         ON CONFLICT (JobID) DO NOTHING;
    #     """
    #     values = (jobid, jobname, account, partition, nnodes, alloccpus, start, end, elapsed, status,
    #                 str(nodelist), task, username, reqtres)

    #     self._cursor.execute(q, values)
    #     self._connection.commit()


    # def insert_job_db(self,
    #                   table_name: str,
    #                   jobid: int,
    #                   jobname: str,
    #                   account: str,
    #                   partition: str,
    #                   nnodes: int,
    #                   alloccpus: int,
    #                   start: str,
    #                   end: str,
    #                   elapsed: str,
    #                   status: str,
    #                   nodelist: list,
    #                   task: str,
    #                   username: str,
    #                   reqtres: str) -> None:

    #     if '.' in jobid:
    #         with open('excluded-jobs.out', "w") as f:
    #             f.write(f"{jobid} {jobname} {account} {partition} {nnodes} {alloccpus} {start} {end} {elapsed} "
    #                     f"{status} {nodelist} {task} {username} {reqtres}\n")
    #     else:

    #         q = f"""
    #                 INSERT INTO {table_name} 
    #                 (JobID, JobName, Account, Partition, NNodes, AllocCPUs, JobStart, JobEnd, Elapsed, Status, 
    #                 Nodelist, Task, Username, ReqTRES) 
    #                 VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s) 
    #                 ON CONFLICT (JobID) DO NOTHING;
    #              """
    #         values = (jobid, jobname, account, partition, nnodes, alloccpus, start, end, elapsed, status,
    #                   str(nodelist), task, username, reqtres)

    #         self._cursor.execute(q, values)
    #         self._connection.commit()


    # def update_job_db(self, column: str, job: dict, value: str) -> None:
    #     """
    #     Atualiza o valor de uma coluna específica em um job no banco de dados.
    #     Se o job não existir, insere um novo.

    #     :param str column: Nome da coluna.
    #     :param int jobid: ID do job a ser atualizado.
    #     :param str value: Novo valor para a coluna.
    #     """

    #     # Evita SQL injection no nome da coluna
    #     allowed_columns = ["username", "reqtres"]

    #     if column not in allowed_columns:
    #         raise ValueError(f"[!] Column '{column}' not allowed to be updated.")

    #     q_update = f"""
    #             UPDATE job
    #             SET {column} = %s
    #             WHERE JobID = %s
    #             RETURNING JobID;
    #         """
    #     self._cursor.execute(q_update, (value, job.get('jobid')))
    #     updated = self._cursor.fetchone()

    #     if not updated:
    #         self.insert_job_db(
    #             "job",
    #             job.get('jobid'),
    #             job.get('jobname'),
    #             job.get('account'),
    #             job.get('partition'),
    #             job.get('nnodes'),
    #             job.get('alloccpus'),
    #             job.get('start'),
    #             job.get('end'),
    #             job.get('elapsed'),
    #             job.get('status'),
    #             job.get('nodelist'),
    #             job.get('task'),
    #             job.get('username'),
    #             job.get('reqtres')
    #         )
    #     else:
    #         self._connection.commit()