import psycopg2 as psql
from psycopg2 import pool, sql, extensions
from contextlib import contextmanager
from typing import Generator, Any, Dict, Optional
import logging
from dotenv import load_dotenv
import os


# Setup logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class PsqlConnector:
    _connection_pool: Optional[pool.SimpleConnectionPool] = None

    @classmethod
    def initialize_pool(cls, db_config: Dict[str, Any], minconn: int = 1, maxconn: int = 10) -> None:
        cls._connection_pool = pool.SimpleConnectionPool(
            minconn, maxconn, **db_config)
        
        """
        Initialize the PostgreSQL connection pool.

        :param db_config: Dictionary containing database configuration.
        :param minconn: Minimum number of connections in the pool.
        :param maxconn: Maximum number of connections in the pool.
        """
        if cls._connection_pool is None:
            cls._connection_pool = pool.SimpleConnectionPool(minconn, maxconn, **db_config)
            logger.info("PostgreSQL connection pool created")
        else:
            logger.warning("Connection pool already initialized")

    @classmethod
    @contextmanager
    def get_cursor(cls) -> Generator[psql.extensions.cursor, None, None]:
        """
        Context manager to get a cursor from the connection pool.

        :yield: PostgreSQL cursor object.
        """
        connection: Optional[extensions.connection] = None
        cursor: Optional[psql.extensions.cursor] = None

        if cls._connection_pool is None:
            raise Exception("Connection pool is not initialized")
       
        try:
            connection = cls._connection_pool.getconn()
            cursor = connection.cursor()
            yield cursor
            logger.info("Executing transaction")
            connection.commit()
        except Exception as e:
            if connection:
                connection.rollback()
            logger.error(f"Transaction failed: {e}")
            raise
        finally:
            if cursor:
                cursor.close()
            if connection:
                cls._connection_pool.putconn(connection)
            logger.info("Connection returned to the pool")

    @classmethod
    def get_db_config(cls):
        load_dotenv()

        db_config = {
            'dbname': os.getenv('DB_NAME'),
            'user': os.getenv('DB_USER'),
            'password': os.getenv('DB_PASSWORD'),
            'host': os.getenv('DB_HOST'),
            'port': os.getenv('DB_PORT')
        }

        if not all(db_config.values()):
            missing_vars = [k for k, v in db_config.items() if not v]
            raise Exception(f"Missing database configuration for: {', '.join(missing_vars)}")

        return db_config


PsqlConnector.initialize_pool(
    PsqlConnector.get_db_config(), minconn=1, maxconn=10)


