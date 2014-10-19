#!/usr/bin/env python
"""Builds graphs from the lines in a gziped bro log in a temporary location,
echos out the number of records incorporated into graphs in the process.  This
script is really only useful as a sanatiy check to see how we'll we're doing,
and making sure that all expected files are being brough into a graph.

Paths to bro-log records are read in from stdin, and each resulting count
is written to stdout

"""
import sys
import os.path
sys.path.append(os.path.dirname(os.path.abspath(__file__)), os.path.join('..'))

import argparse
from brotools.graphs import graphs
import gzip

def record_filter(record):
    short_content_type = record.content_type[:9]
    return (short_content_type in ('text/plai', 'text/html') or
            record.status_code == "301")

for path in sys.stdin:
    count = 0
    with gzip.open(path.strip(), 'rb') as h:
        print sum([len(g) for g in graphs(h, record_filter=record_filter)])
