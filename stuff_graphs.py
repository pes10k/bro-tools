"""Generate images of graphs of all found graphs that contain a possible
cookie stuffing instance."""

import sys
import brotools.reports
import brotools.records
import stuffing.amazon

parser = brotools.reports.default_cli_parser(sys.modules[__name__].__doc__)
ins, out, debug, args = brotools.reports.parse_default_cli_args(parser)

debug("Preparing to start reading pickled data")
index = 0
for path, graphs in ins:
    index += 1
    debug("{0}. Considering {1}".format(index, path))
    debug("{0}. Found {1} graphs".format(index, len(graphs)))
    for g in graphs:
        if "amazon.com" in g._root.host:
            continue
        # Iterate over all nodes in the graph until we hit one that has
        # a cookie stuffing attempt in it.  But no need to iterate further
        # after we hit the first stuffing node
        stuffs = stuffing.amazon.stuffs_in_graph(g, time=5)
        if len(stuffs) > 0:
            debug(" * Found possible cookie stuffing at: {0}".format(
                stuffs[0].url))
            debug(" * Root node: {0}".format(g._root.url))
            out.write(str(g))
            out.write("\n\n")
