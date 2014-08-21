"""Checks a collection of bro log graphs to measure two values, 1) how many
graph heads have referrer heads, and 2) what hosts each of those referrer heads
are for."""

import sys
import brotools.reports
import brotools.records

parser = brotools.reports.default_cli_parser(sys.modules[__name__].__doc__)
parser.add_argument('--time', '-t', type=float, default=10,
                    help="The number of seconds that can pass between " +
                    "requests for them to still be counted in the same graph.")
count, ins, out, debug, args = brotools.reports.parse_default_cli_args(parser)

index = 0
debug("Preparing to reading {0} sets of graphs".format(count))

g_items = {
    "graphs_for_client": {},
    # Graphs sorted by latest child record timestamp, earliest value first
    "graphs_by_date": []
}

def insert_into_collection(candidate_graph):
    index = -1
    match = None
    for sorted_g in g_items['graphs_by_date']:
        index += 1
        if sorted_g.latest_ts > candidate_graph.latest_ts:
            match = True
            break
    if not match:
        return False
    hash_key = candidate_graph.ip + "|" + candidate_graph.user_agent
    g_items['graphs_for_client'][hash_key].append(candidate_graph)
    g_items['graphs_by_date'].insert(index, candidate_graph)
    return True

def prune_collection(most_recent_graph):
    latest_valid_time = most_recent_graph.latest_ts - args.time
    remove_count = 0
    for sorted_g in g_items['graphs_by_date']:
        if sorted_g.latest_ts >= latest_valid_time:
            break
        remove_count += 1
        hash_key = sorted_g.ip + "|" + sorted_g.user_agent
        g_items['graphs_for_client'][hash_key].remove(sorted_g)

    if remove_count > 0:
        g_items['graphs_by_date'] = g_items['graphs_by_date'][remove_count:]

count_merges = 0
count_graphs = 0
for path, graphs in ins():
    index += 1
    debug("{0}-{1}. Considering {2}".format(index, count, path))
    debug("{0}-{1}. Found {2} graphs".format(index, count, len(graphs)))
    for new_graph in graphs:
        count_graphs += 1
        prune_collection(new_graph)
        hash_key = new_graph.ip + "|" + new_graph.user_agent
        client_graphs = g_items['graphs_for_client'][hash_key]
        for client_graph in client_graphs:
            if client_graph.referrer_record(new_graph._root):
                debug(" * Found possible merge: {0}".format(new_graph._root.url))
                count_merges += 1
        insert_into_collection(new_graph)

out.write("Found graphs: {0}\n".format(count_graphs))
out.write("Possible merges in the collection: {0}\n".format(count_merges))

