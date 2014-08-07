"""Generate images of graphs of all found graphs that contain a possible
cookie stuffing instance."""

import sys
import brotools.reports
import brotools.records

parser = brotools.reports.marketing_cli_parser(sys.modules[__name__].__doc__)
count, ins, out, debug, marketers, args = brotools.reports.parse_marketing_cli_args(parser)

debug("Preparing to start reading pickled data.")
index = 0
for path, graphs in ins():
    index += 1
    debug("{0}. Considering {1}".format(index, path))
    debug("{0}. Found {1} graphs".format(index, len(graphs)))
    for g in graphs:
        for marketer in marketers:
            # Iterate over all nodes in the graph until we hit one that has
            # a cookie stuffing attempt in it.  But no need to iterate further
            # after we hit the first stuffing node
            stuffs = marketer.stuffs_in_graph(g, time=2)
            if len(stuffs) > 0:
                debug(" * Found possible cookie stuffing at: {0}".format(
                    stuffs[0].url))
                debug(" * Root node: {0}".format(g._root.url))
                out.write("Marketer: {0}\n".format(marketer.name()))
                out.write(str(g))
                out.write("\n\n")
