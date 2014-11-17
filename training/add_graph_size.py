#!/usr/bin/env python
"""
One timer to update permanant store with graph sizes.
"""

import sys
import os.path

CUR_PATH = os.path.dirname(os.path.abspath(__file__))
PARENT_PATH = os.path.join(CUR_PATH, '..')
DEFAULT_DB_URL = os.path.join(PARENT_PATH, "contrib", "data.db")
DEFAULT_DBURI = sys.path.append("sqlite:///{0}".format(DEFAULT_DB_URL))

import datetime
import training.sqltypes
import training.features
from brotools.reports import default_cli_parser, parse_default_cli_args
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker

parser = default_cli_parser(sys.modules[__name__].__doc__)
parser.add_argument('-u', '--dburi', default=DEFAULT_DBURI,
                    help="A valid DB URI used for connecting to a SQL " +
                         "database to persist information in.  See " +
                         "for examples: " +
                         "http://docs.sqlalchemy.org/en/rel_0_9/core/engines.html#database-urls")
num_inputs, inputs, output_h, debug, args = parse_default_cli_args(parser)


engine = create_engine(args.dburi, echo=False)

# Create any needed, missing tables in the given database
training.sqltypes.Base.metadata.create_all(engine)
session = sessionmaker(bind=engine)()

for path, graph in inputs():
    graph_rec = training.sqltypes.get_set(graph, session)
    graph_rec.graph_size = len(graph)
    session.commit()
