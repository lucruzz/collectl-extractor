import sys
import psycopg2
from argparse import Namespace
from utils.logging import log_error, log_info

class Database:
    def __init__(self, config: Namespace):
        self.__user = config.user
        self.__password = config.password
        self.__host = config.host
        self.__port = config.port
        self.__dbname = config.dbname
        self._connection = None

    def connect_db(self) -> None:

        try:
            self._connection = psycopg2.connect(
                host=self.__host,
                dbname=self.__dbname,
                user=self.__user,
                password=self.__password,
                port=self.__port
            )

            if self._connection.status:
                log_info("Database: Database is connected.")

        except Exception as error:
            log_error("Error: Database connection failed.")
            sys.exit(1)

    def get_connection(self):
        return self._connection

    def commit(self):
        if self._connection:
            self._connection.commit()

    def close_db(self) -> None:
        if self._connection is not None:
            self._connection.close()
            log_info("Database: Database connection closed.")