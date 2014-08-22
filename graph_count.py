"""Count the number of graphs in one or more files."""

import sys
import brotools.reports
import brotools.records

parser = brotools.reports.default_cli_parser(sys.modules[__name__].__doc__)
count, ins, out, debug, args = brotools.reports.parse_default_cli_args(parser)

debug("Preparing to reading {0} sets of graphs".format(count))

last_path = None
current_count = 0
total_count = 0
for path, graph in ins():

    if last_path and last_path != path:
        out.write("{0}: {1}\n".format(last_path, current_count))

    last_path = path
    current_count += 1
    total_count += 1

out.write("Total: {0}\n".format(total_count))
