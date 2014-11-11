"""
An interactive tool used for label amazon stuff data in graphs.
"""

import sys
import os.path
sys.path.append(os.path.join(os.path.dirname(os.path.abspath(__file__)), '..'))

import readline
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
count, ins, out, debug, args = parse_default_cli_args(parser)

engine = create_engine(args.dburi, echo=args.verbose)

# Create any needed, missing tables in the given database
training.sqltypes.Base.metadata.create_all(engine)
session = sessionmaker(bind=engine)

for path, graphs in ins():
    for g in graphs:
        # Check to see if we've already reviewed this graph
        if training.sqltypes.get_set(g):
            debug("Skipping graph, already considered it")
            continue

        sets = AmazonAffiliateHistory.cookie_sets_in_graph(g)
        if len(sets) == 0:
            debug("Skipping graph, no cookie sets found")
            continue

        a_set = sets[0]
        a_set_hash = g.hash()
        a_set_file = path
        a_set_url = a_set.url()
        a_set_reqest_time = datetime.datetime.fromtimestamp(int(a_set.ts))
        a_set_tag = training.features.affiliate_tag_for_cookie_set(g)
        time_from_referrer = training.features.amazon_time_from_referrer(g)
        time_after_set = training.features.amazon_time_after_cookie_set(g)

        print "-----------------------------------------------------------"
        print "Hash:    {0}".format(a_set_hash)
        print "File:    {0}".format(path)
        print "Set URL: {0}".format(a_set_url)
        print "Tag:     {0}".format(a_set_tag)
        print "Time From Referrer: {0}".format(time_from_referrer)
        print "Time to bottom:     {0}".format(time_to_bottom)
        print g.summary(detailed=False)
        print ""

        valid_responses = ("y", "n", "u")
        response = False
        while response not in valid_responses:
            response = raw_input("Yes/No/Uncertain").lower()

        if response == "y":
            label = "valid"
        elif response == "n":
            label = "stuff"
        elif response == "u":
            label = "uncertain"

        ref = g.parent_of_node(a_set)
        if ref:
            referrer_id = training.sqltypes.get_referrer_id(ref)
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
        print ""
        print ""
