# from idlelib import query
# import sys
# import psycopg2
from argparse import Namespace
# from psycopg2 import sql


class Database:
    def __init__(self, config: Namespace):
        self.__user = config.user
        self.__password = config.password
        self.__host = config.host
        self.__port = config.port
        self.__dbname = config.dbname
        self.__schema = config.schema_db
        self._connection = None
        self._cursor = None

    # def connect_db(self) -> None:

    #     try:
    #         self._connection = psycopg2.connect(
    #             host=self.__host,
    #             dbname=self.__dbname,
    #             user=self.__user,
    #             password=self.__password,
    #             port=self.__port
    #         )

    #         self._cursor = self._connection.cursor()

    #         if self._connection.status:
    #             print(f"\033[0;32m[+]\033[0m \033[1;37mDatabase is connected.\033[0m")

    #     except Exception as error:
    #         print('\033[31m[!] Error: Database connection failed.\033[0m')
    #         print(error)
    #         sys.exit(1)

    # def test_connection_db(self) -> bool:
    #     try:
    #         tmp_connection = psycopg2.connect(
    #             host=self.__host,
    #             dbname='postgres',
    #             user=self.__user,
    #             password=self.__password,
    #             port=self.__port
    #         )
    #         tmp_cursor = tmp_connection.cursor()

    #         tmp_cursor.close()
    #         tmp_connection.close()

    #         return True

    #     except (Exception, psycopg2.DatabaseError) as error:
    #         print(error)
    #         return False

    # def close_db(self) -> None:

    #     if self._cursor is not None:
    #         self._cursor.close()

    #     if self._connection is not None:
    #         self._connection.close()

    #     print('\033[0;32m[+]\033[0m \033[1;37mDatabase connection closed.\033[0m')

    # def _table_exists(self, table_name: str) -> bool:
    #     query = f"SELECT EXISTS (SELECT 1 FROM pg_catalog.pg_tables WHERE schemaname = '{self.__schema}' AND tablename = '{table_name}');"
    #     self._cursor.execute(query)
    #     return self._cursor.fetchone()[0]

    # def execute_query(self, query: str):
    #     self._cursor.execute(query)
    #     return  self._cursor.fetchall()

    # def remove_db(self) -> None:
    #     pass