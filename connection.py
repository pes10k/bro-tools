import config
from collections import namedtuple
import psycopg2

__state = {'con': None}

def db():
    if not __state['con']:
        __state['con'] = psycopg2.connect(**config.db_params)
        __state['con'].autocommit = True
    return __state['con']

def bro_records(handle):
    seperator = None
    record_type = None
    for raw_row in handle:
        row = raw_row[:-1] # Strip off line end
        if not seperator and row[0:10] == "#separator":
            seperator = row[11:].decode('unicode_escape')
        if not record_type and row[0:7] == "#fields":
            record_type = namedtuple('BroRecord', [a.replace(".", "_") for a in row.split(seperator)[1:]])
        elif row[0] != "#":
            row_values = row.split(seperator)
            yield record_type._make([a if a != "-" else "" for a in row_values])
