from DatabaseConnector import PsqlConnector # with my own Database Connector that i've wrote
# from tools.schema import ForeignKey
from typing import List, Type, Dict, Any, Optional, Generator
from functools import lru_cache
import logging
import asyncio

# Setup logging for debugging and error handling
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class ModelMeta(type):
    def __new__(cls, name, bases, dct):
        if name != 'Model':
            columns = {k: v for k, v in dct.items()}
            dct['_columns'] = columns
            dct['table_name'] = name.lower() + 's'

            def create(cls, **kwargs):
                instance = cls.__new__(cls)
                for key, value in kwargs.items():
                    setattr(instance, key, value)
                return instance

            dct['create'] = classmethod(create)

        return super().__new__(cls, name, bases, dct)


class Model(metaclass=ModelMeta):

    @classmethod
    async def _execute_query(cls, query: str, params: Optional[tuple] = ()) -> List[tuple]:
        try:
            async with PsqlConnector.get_cursor() as cursor:
                await cursor.execute(query, params)
                return await cursor.fetchall()
        except Exception as e:
            logger.error(f"Error executing query: {e}")
            return []

    @classmethod
    async def _fetch_as_dicts(cls, query: str, params: tuple, keys: List[tuple]) -> List[Dict[str, Any]]:
        rows = await cls._execute_query(query, params)
        return (dict(zip(keys, row)) for row in rows)

    @classmethod
    @lru_cache(maxsize=128)
    async def get_columns(cls) -> List[str]:
        query = "SELECT column_name FROM information_schema.columns WHERE table_name = %s;"
        params = (cls.table_name,)
        columns = await cls._execute_query(query, params)
        return [row[0] for row in columns]

    @classmethod
    async def get(cls, **kwargs) -> List[Dict[str, Any]]:
        columns = await cls.get_columns()
        condition = ' AND '.join([f"{k} = %s" for k in kwargs])
        values = tuple(kwargs.values())
        query = f'SELECT * FROM {cls.table_name} WHERE {condition};'
        return await cls._fetch_as_dicts(query, values, columns)

    @classmethod
    async def insert(cls, **kwargs) -> List[Dict[str, Any]]:
        columns = ', '.join(kwargs.keys())
        values = ', '.join(['%s'] * len(kwargs))
        query = f'''INSERT INTO {cls.table_name} ({columns}) VALUES ({values}) RETURNING *;'''
        params = tuple(kwargs.values())
        all_columns = await cls.get_columns()
        instance = await cls._fetch_as_dicts(query, params, all_columns)
        return instance

    @classmethod
    async def create_table(cls):
        columns_definitions = ', '.join(
            f"{name} {col}" for name, col in cls._columns.items())
        foreign_keys = [f"FOREIGN KEY ({name}) {col}" for name, col in cls._columns.items() if isinstance(col, ForeignKey)]
        unique_constraints = [name for name, col in cls._columns.items() if col.unique and not isinstance(col, ForeignKey)]
        check_constraints = [f"CHECK ({col.check})" for name, col in cls._columns.items() if col.check]

        query_parts = [columns_definitions] + foreign_keys
        if unique_constraints:
            query_parts.append(f"UNIQUE ({', '.join(unique_constraints)})")
        query_parts.extend(check_constraints)

        query = f'CREATE TABLE IF NOT EXISTS {cls.table_name} ({", ".join(query_parts)});'
        await cls._execute_query(query)

    @classmethod
    async def drop_table(cls):
        query = f'DROP TABLE IF EXISTS {cls.table_name};'
        await cls._execute_query(query)

    @classmethod
    async def all(cls, filter: Optional[Dict[str, Any]] = None, ordering: Optional[List[str]] = None) -> List[Dict[str, Any]]:
        columns = await cls.get_columns()
        query = f'SELECT * FROM {cls.table_name}'
        params = ()
        if filter:
            condition = ' AND '.join([f"{k} = %s" for k in filter.keys()])
            query += f' WHERE {condition}'
            params = tuple(filter.values())

        if ordering:
            ordering_clause = ', '.join(ordering)
            query += f' ORDER BY {ordering_clause}'    

        return await cls._fetch_as_dicts(query, params, columns)

    @classmethod
    async def update(cls, id: int, **kwargs) -> Optional[Dict[str, Any]]:
        columns = await cls.get_columns()
        updates = ', '.join(f"{k} = %s" for k in kwargs)
        values = (*kwargs.values(), id)
        query = f"UPDATE {cls.table_name} SET {updates} WHERE id = %s RETURNING *;"
        row_dict = await cls._fetch_as_dicts(query, values, columns)
        return row_dict[0] if row_dict else None

    @classmethod
    async def destroy(cls, **kwargs) -> None:
        query = f'DELETE FROM {cls.table_name} WHERE {" AND ".join([f"{key} = %s" for key in kwargs.keys()])};'
        params = tuple(kwargs.values())
        try:
            async with PsqlConnector.get_cursor() as cursor:
                await cursor.execute(query, params)
        except Exception as e:
            logger.error(f"Error deleting data: {e}")
            raise
