from contextlib import contextmanager
import logging
import os
import sqlite3
import yaml

log = logging.getLogger(__name__)

def __create_table(cursor, table):
    log.info("Creating table:%s" % table)
    column_list = table['columns']
    table_name = table['name']
    query = 'CREATE TABLE '
    query += table_name + '('
    query += ', '.join(column for column in column_list)
    query += ')'
    try:
        cursor.execute(query)
        log.debug("Created table:%s" % table_name)
    except sqlite3.OperationalError:
        # Assume table exists
        log.debug("Table %s already exists" % table_name)

def create_tables(cursor, tables_template):
    with open(os.path.abspath(tables_template)) as f:
        data = yaml.load(f)
        tables = data['tables']
        for table in tables:
            __create_table(cursor, table)

@contextmanager
def connect_sql(database_file):
    with sqlite3.connect(database_file) as conn:
        try:
            yield conn
        finally:
            conn.commit()
