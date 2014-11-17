#!/usr/bin/env python
"""
High level stats describing the status of the training set.
"""

import os
import sys
import pprint
import sqltypes
import pickle

tags = {}
features = {}
all_features = ("time_from_referrer", "time_after_set", "graph_size",
                "is_reachable", "page_rank", "alexa_rank", "is_registered",
                "years_registered", "registration_date", "is_ssl")

for f in all_features:
    features["{0}_min".format(f)] = None
    features["{0}_max".format(f)] = None
    features["{0}_count".format(f)] = 0


def add_f(label, row):
    min_key = "{0}_min".format(label)
    max_key = "{0}_max".format(label)
    count_key = "{0}_count".format(label)

    try:
        value = row[label]
    except KeyError:
        return

    if value is None:
        return

    if label in ("years_registered", "page_rank"):
        if value == 0:
            return

    if features[min_key] is None or features[min_key] > value:
        features[min_key] = value

    if features[max_key] is None or features[max_key] < value:
        features[max_key] = value
    features[count_key] += 1

for row in sqltypes.raw_records():
    try:
        tags[row['tag']] += 1
    except:
        tags[row['tag']] = 1

    for f in all_features:
        add_f(f, row)

tag_counts = sorted(tags.items(), key=lambda x: x[1])

h = open(os.path.join("..", "contrib", "tag_counts.pickle"), 'w')
pickle.dump(tags, h)
features['tag_count_min'] = tag_counts[0][1]
features['tag_count_max'] = tag_counts[-1][1]
pprint.pprint(features)
