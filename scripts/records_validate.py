#!/usr/bin/env python
"""Parses records out of a gziped bro log file, and returns the number of
BroRecord objects parsed out of the file.
"""
import sys
import os.path
sys.path.append(os.path.join(os.path.dirname(os.path.abspath(__file__)), '..'))

import argparse
from brotools.records import bro_records
from brotools.reports import record_filter
import gzip

for path in sys.stdin:
    count = 0
    with gzip.open(path.strip(), 'rb') as h:
        for record in bro_records(h, record_filter=record_filter):
            count += 1
    print count
