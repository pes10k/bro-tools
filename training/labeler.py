#!/usr/bin/env python
"""
An interactive tool used for label amazon stuff data in graphs.
"""

import sys
import os.path
sys.path.append(os.path.join(os.path.dirname(os.path.abspath(__file__)), '..'))

import datetime
import training.sqltypes
import training.features
from brotools.reports import default_cli_parser, parse_default_cli_args
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
from stuffing.amazon import AmazonAffiliateHistory

parser = default_cli_parser(sys.modules[__name__].__doc__)
parser.add_argument('-u', '--dburi', required=True,
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

    # Check to see if we've already reviewed this graph
    if training.sqltypes.get_set(graph, session):
        debug("Skipping graph, already considered it")
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

    a_set_hash = graph.hash()
    a_set_file = path
    a_set_url = a_set.url
    a_set_reqest_time = datetime.datetime.fromtimestamp(int(a_set.ts))
    a_set_tag = training.features.affiliate_tag_for_cookie_set(graph)
    time_from_referrer = training.features.amazon_time_from_referrer(graph)
    time_after_set = training.features.amazon_time_after_cookie_set(graph)
    ref = graph.parent_of_node(a_set)

    print "-----------------------------------------------------------"
    print "Hash:               {0}".format(a_set_hash)
    print "File:               {0}".format(path)
    print "Set URL:            {0}".format(a_set_url)
    print "Referrer URL:       {0}".format(ref.url if ref else None)
    print "Tag:                {0}".format(a_set_tag)
    print "Time From Referrer: {0}".format(time_from_referrer)
    print "Time to bottom:     {0}".format(time_after_set)
    print graph.summary(detailed=False)
    print ""

    valid_responses = ("y", "n", "u")
    response = False
    while response not in valid_responses:
        response = raw_input("[Y]es/[N]o/[U]ncertain: ")
        response = response.lower()

    if response == "y":
        label = "valid"
    elif response == "n":
        label = "stuff"
    elif response == "u":
        label = "uncertain"

    if ref:
        referrer_id = training.sqltypes.get_referrer_id(ref, session)
    else:
        referrer_id = None

    new_set = training.sqltypes.CookieSet(
        id=a_set_hash, file=a_set_file, url=a_set_url,
        request_time=a_set_reqest_time, tag=a_set_tag,
        referrer_id=referrer_id,
        time_from_referrer=time_from_referrer,
        time_after_set=time_after_set, label=label
    )
    session.add(new_set)
    session.commit()
    print ""
    print ""
