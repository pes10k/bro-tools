#!/usr/bin/env python
"""Examines pickled graphs of bro record data and builds up counts of how often
different session cookies for different affilate marketers are found in the
data.

The script can look for a subset of supported marketers, or if no arguments
are provided will look for all marketers.
"""

import sys
import os.path
sys.path.append(os.path.join('..'))
import brotools.reports

parser = brotools.reports.marketing_cli_parser(sys.modules[__name__].__doc__)
count, ins, out, debug, marketers, args = brotools.reports.parse_marketing_cli_args(parser)

# This dictionary has keys of marketer names.  Values are dicts, and the keys
# of those dicts are session cookies found for the given marketer.  Values
# of this sub dict are the counts of the number of times the given cookie
# has been found.
cookies_by_marketer = {}
index = 0
debug("Preparing to start reading {0} pickled data".format(count))
for path, graphs in ins():
    index += 1
    debug("{0}. Considering {1}".format(index, path))
    debug("{0}. Found {1} graphs".format(index, len(graphs)))
    for g in graphs:
        for marketer in marketers:
            marketer_name = marketer.name()
            session_id = marketer.session_id_for_graph(g)
            if not session_id:
                continue

            debug("\tFound session id: {1}".format(index, session_id))
            if marketer_name not in cookies_by_marketer:
                cookies_by_marketer[marketer_name] = {}
            if session_id not in cookies_by_marketer[marketer_name]:
                cookies_by_marketer[marketer_name][session_id] = 1
            else:
                cookies_by_marketer[marketer_name][session_id] += 1

# Now, print the results, with the most common marketer appearing first
marketers_sorted = sorted(cookies_by_marketer.iteritems(), key=lambda x: len(x[1]), reverse=True)
for marketer_name, session_ids in marketers_sorted:
    out.write("Marketer: {0}\n".format(marketer_name))
    out.write("Num session IDs: {0}\n".format(len(session_ids)))
    out.write("----------\n")
    sorted_session_ids = sorted(session_ids.iteritems(), key=lambda x: x[1], reverse=True)
    for session_id, count in sorted_session_ids:
        out.write("\t{0}\t{1}\n".format(count, session_id))
    out.write("\n\n")
