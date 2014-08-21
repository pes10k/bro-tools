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

debug("Preparing to reading {0} sets of graphs".format(count))

g_items = {
    "graphs_for_client": {},
    # Graphs sorted by latest child record timestamp, earliest value first
    "graphs_by_date": []
}

def graph_hash(graph):
    return graph.ip + "|" + graph.user_agent

def insert_into_collection(candidate_graph):
    hash_key = graph_hash(candidate_graph)

    # Special case for considering the first graph
    if len(g_items['graphs_by_date']) == 0:
        g_items['graphs_for_client'][hash_key] = [candidate_graph]
        g_items['graphs_by_date'].append(candidate_graph)
        return True

    index = -1
    match = None
    for sorted_g in g_items['graphs_by_date']:
        index += 1
        if sorted_g.latest_ts > candidate_graph.latest_ts:
            match = True
            break

    if not match:
        return False
    try:
        g_items['graphs_for_client'][hash_key].append(candidate_graph)
    except KeyError:
        g_items['graphs_for_client'][hash_key] = [candidate_graph]
    g_items['graphs_by_date'].insert(index, candidate_graph)
    return True

def prune_collection(most_recent_graph):
    latest_valid_time = most_recent_graph.latest_ts - args.time
    remove_count = 0
    for sorted_g in g_items['graphs_by_date']:
        if sorted_g.latest_ts >= latest_valid_time:
            break
        remove_count += 1
        hash_key = graph_hash(sorted_g)
        g_items['graphs_for_client'][hash_key].remove(sorted_g)

    if remove_count > 0:
        g_items['graphs_by_date'] = g_items['graphs_by_date'][remove_count:]

count_merges = 0
count_graphs = 0
for path, graph in ins():
    count_graphs += 1
    prune_collection(graph)
    hash_key = graph_hash(graph)
    try:
        client_graphs = g_items['graphs_for_client'][hash_key]
        for client_graph in client_graphs:
            if client_graph.referrer_record(graph._root):
                debug(" * Found possible merge: {0}".format(graph._root.url))
                debug(" * * In: {0}".format(path))
                count_merges += 1
    except KeyError:
        pass
    insert_into_collection(graph)

out.write("Found graphs: {0}\n".format(count_graphs))
out.write("Possible merges in the collection: {0}\n".format(count_merges))

