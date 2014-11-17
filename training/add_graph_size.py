#!/usr/bin/env python
"""
One timer to update permanant store with graph sizes.
"""

import sys
import os.path

CUR_PATH = os.path.dirname(os.path.abspath(__file__))
PARENT_PATH = os.path.join(CUR_PATH, '..')
DEFAULT_DB_PATH = os.path.join("..", "contrib", "data.db")
DEFAULT_DBURI = os.path.join("sqlite:///{0}".format(DEFAULT_DB_PATH))
sys.path.append(PARENT_PATH)

import datetime
import training.sqltypes
import training.features
from brotools.reports import default_cli_parser, parse_default_cli_args
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
from stuffing.amazon import AmazonAffiliateHistory


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
hit = 0
miss = 0
for path, graph in inputs():

    if "amazon.com" in graph._root.host:
        continue

    debug("Graph Root: {0}".format(graph._root.url))
    if args.verbose:
        if "www.lewnn.com" in graph._root.url:
            print graph
        raw_input("next...")

    if len(graph) == 1:
        debug("Don't bother labeling graphs with only one request in them")
        continue

    sets = AmazonAffiliateHistory.cookie_sets_in_graph(graph)
    if len(sets) == 0:
        debug("Skipping graph, no cookie sets found")
        continue

    # Try to find a cookie set that is not the root of the graph, since
    # these will not be intersting cases for training
    a_set = None
    for test_set in sets:
        if test_set is graph._root:
            continue
        a_set = test_set
        break

    if a_set is None:
        debug("Don't bother labeling graphs where cookie set is the root " +
              "of the tree")
        continue

    graph_rec = training.sqltypes.get_set(graph, session)
    if not graph_rec:
        miss += 1
        continue
    else:
        hit += 1
    graph_rec.graph_size = len(graph)
    session.commit()

print "hit: {0}\nmiss: {1}".format(hit, miss)
