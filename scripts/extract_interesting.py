#!/usr/bin/env python
"""Receive a large number of graphs, in pickle files, and write back only
those graphs that include domains that we care about."""

import sys
import os.path
sys.path.append(os.path.join(os.path.dirname(os.path.abspath(__file__)), '..'))

import os
import brotools.reports
import brotools.records
import datetime

try:
    import cPickle as pickle
except:
    import pickle

parser = brotools.reports.marketing_cli_parser(sys.modules[__name__].__doc__)
count, ins, out, debug, marketers, args = brotools.reports.parse_marketing_cli_args(parser)

index = 0
for path, g in ins():

    index += 1
    # debug("{0}. Found {1} graphs".format(index, len(graphs)))
    for marketer in marketers:
        if len(marketer.nodes_for_domains(g)) == 0:
            continue
        debug("{0}. Found graph for {1}".format(index, marketer.name()))
        dst_path = path + ".important"
        with open(dst_path, 'a') as h:
            pickle.dump(g, h)
        break
