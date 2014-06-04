"""Generate images of graphs of all found graphs that contain a possible
cookie stuffing instance."""

import sys
import brotools.reports
import brotools.records
from stuffing.amazon import is_cookie_set

parser = brotools.reports.default_cli_parser(sys.modules[__name__].__doc__)
parser.add_argument('--graphs', '-g', default="/tmp/bro-redirectors",
                    help="A path to write images of graphs that include " +
                    "cookie stuffing attempts to.")
ins, out, debug, args = brotools.reports.parse_default_cli_args(parser)

brotools.records.BroRecord.__str__ = lambda x: x.host + "\n" + x.uri

for path, graphs in ins:
    debug("Considering {0}".format(path))
    debug("Found {0} graphs".format(len(graphs)))
    index = 0
    for g in graphs:
        index += 1
        # Iterate over all nodes in the graph until we hit one that has
        # a cookie stuffing attempt in it.  But no need to iterate further
        # after we hit the first stuffing node
        for node in (n for n in g.nodes() if is_cookie_set(n) and n != g._root):
            debug(" * Found possible cookie stuffing at: {0}".format(node.url()))
            debug(" * Root node: {0}".format(g._root.url()))
            out.write(str(g))
            out.write("\n\n\n")
            break
