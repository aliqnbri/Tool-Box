import psycopg2 as psql
from django.conf import settings
from psycopg2 import pool
from contextlib import contextmanager
from typing import Generator, Any, Dict


class PsqlConnector:
    _connection_pool = None

    @classmethod
    def initialize_pool(cls, db_config: Dict[str, Any], minconn: int = 1, maxconn: int = 10) -> None:
        cls._connection_pool = pool.SimpleConnectionPool(
            minconn, maxconn, **db_config)

    @classmethod
    @contextmanager
    def get_cursor(cls) -> Generator[psql.extensions.cursor, None, None]:
        connection = cls._connection_pool.getconn()
        try:
            cursor = connection.cursor()
            yield cursor
            print("\033[32;5mConnecting to PostgreSQL Database...\033[m")
            connection.commit()
        except Exception as e:
            connection.rollback()
            raise e
        finally:
            cursor.close()
            print("\033[31;5mConnection Closed\033[m")
            cls._connection_pool.putconn(connection)

    @classmethod
    def get_db_config(cls):
        try:
            db_settings = settings.DATABASES['default']
            db_config = {
                'dbname': db_settings['NAME'],
                'user': db_settings['USER'],
                'password': db_settings['PASSWORD'],
                'host': db_settings['HOST'],
                'port': db_settings['PORT']
            }
            return db_config
        except KeyError as error:
            raise Exception(f'Missing database setting: {error}')


PsqlConnector.initialize_pool(
    PsqlConnector.get_db_config(), minconn=1, maxconn=10)





# import psycopg2 as psql
# from django.conf import settings


# class PsqlConnector:
#     connection =None
#     cursor =None

#     @classmethod
#     def __enter__(cls):
#         cls.connection = None
#         params = cls.get_db_config()
#         print("\033[32;5mConnecting to PostgreSQL Database...\033[m")
#         cls.connection = psql.connect(**params)
#         cls.cursor = cls.connection.cursor()
#         return cls.cursor
    
#     @classmethod
#     def __exit__(cls, exc_type, exc_val, exc_tb):
#         if cls.cursor:
#             cls.connection.commit()
#             cls.cursor.close()
#         if cls.connection:
#             cls.connection.close()
#             print("\033[31;5mConnection Closed\033[m")


#     @classmethod
#     def get_db_config(cls):
#         try:
#             db_settings = settings.DATABASES['default']
#             db_config = {
#                 'dbname': db_settings['NAME'],
#                 'user': db_settings['USER'],
#                 'password': db_settings['PASSWORD'],
#                 'host': db_settings['HOST'],
#                 'port': db_settings['PORT']
#             }        
#             return db_config
#         except KeyError as error:
#             raise Exception(f'Missing database setting: {error}')