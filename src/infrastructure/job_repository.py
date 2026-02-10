# import psycopg2
from psycopg2 import sql
from .database import Database
from domain.job import Job
from argparse import Namespace
from typing import List


class JobRepository:

    def __init__(self, config) -> Namespace:
        self.db = Database(config)
        self._schema = config.schema_db
        self._table = config.type
    
    def get_schema(self)-> str:
        return self._schema
    
    def get_table(self) -> str:
        return self._table

    def insert_jobs(self, jobs: list[Job])-> None:
        self.db.connect_db()
        conn = self.db.get_connection()

        with conn.cursor() as cursor:
            for job in jobs:
                self._insert_job(cursor, job)

        self.db.commit()
        self.db.close_db()

    def _insert_job(self, cursor, job: Job) -> None:
        query = f"""
            INSERT INTO {self.get_schema()}.{self.get_table()} 
            (JobID, JobName, Account, Partition, NNodes, AllocCPUs, JobStart, JobEnd, Elapsed, Status, 
            Nodelist, Task, Username, ReqTRES) 
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s) 
            ON CONFLICT (JobID) DO NOTHING;
        """
        values = (
            job.jobid, 
            job.jobname, 
            job.account, 
            job.partition, 
            job.nnodes, 
            job.alloccpus, 
            job.start, 
            job.end, 
            job.elapsed, 
            job.status,
            str(job.nodelist), 
            job.task, 
            job.username, 
            job.reqtres
        )

        cursor.execute(query, values)

    def fetch_jobs(self) -> List[tuple]:
        query = f"""
            SELECT jobid, status, nodelist, jobstart, jobend FROM {self.get_schema()}.job
        """

        self.db.connect_db()
        conn = self.db.get_connection()

        cursor = conn.cursor()
        cursor.execute(query)
        rows = cursor.fetchall()
        self.db.close_db()

        return rows
