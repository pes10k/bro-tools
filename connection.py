import config
import psycopg2 as pg

__state = {'con': None}

def db():
    if not __state['con']:
        __state['con'] = pg.connect(**config.db_params)
        __state['con'].autocommit = True
    return __state['con']
