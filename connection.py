import config
import psycopg2

__state = {'con': None}

def db():
    if not __state['con']:
        __state['con'] = psycopg2.connect(**config.db_params)
        __state['con'].autocommit = True
    return __state['con']
