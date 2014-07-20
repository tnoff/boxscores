from contextlib import contextmanager
import os
import sqlite3
import yaml

def __create_table(cursor, table):
    #Table meta
    # - table_name :
    # - 'team VARCHAR(256)'
    # - 'date DATETIME'
    # - 'link VARCHAR(1023) PRIMARY KEY'
    query = 'CREATE TABLE '
    for table_name, table_list in table.iteritems():
        query += table_name + '('
        query += ', '.join(k for k in table_list)
    query += ')'
    try:
        cursor.execute(query)
    except sqlite3.OperationalError:
        #Assume table exists
        pass

def create_tables(cursor, tables_template):
    with open(os.path.abspath(tables_template)) as f:
        data = yaml.load(f)
        for table in data:
            __create_table(cursor, table)

@contextmanager
def connect_sql(database_file):
    with sqlite3.connect(database_file) as conn:
        try:
            yield conn
        finally:
            conn.commit()
