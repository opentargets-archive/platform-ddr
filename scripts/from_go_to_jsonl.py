import networkx
import obonet
import json
import itertools

# good example https://github.com/dhimmel/obonet/blob/master/examples/go-obonet.ipynb

url = 'go-basic.obo'
graph = obonet.read_obo(url)

# remove all non biological process from nodes
nodes_to_remove = [n for n, data in graph.nodes(data=True) if data.get('namespace') != 'biological_process']
graph.remove_nodes_from(nodes_to_remove)

# remove all non is_a type of relations
edges_to_remove = [(ffrom, tto, relation) for ffrom, tto, relation in graph.edges(keys=True) if relation != 'is_a']
graph.remove_edges_from(edges_to_remove)

# id <-> name LUTs
id_to_name = {id_: data.get('name') for id_, data in graph.nodes(data=True)}
name_to_id = {data['name']: id_ for id_, data in graph.nodes(data=True) if 'name' in data}
target_bp_id = "GO:0008150"
target_bp = id_to_name[target_bp_id]

example_bp = 'positive regulation of inositol 1,4,5-trisphosphate-sensitive calcium-release channel activity'
example_bp_id = name_to_id[example_bp]
# only for biological processes
# paths = networkx.all_simple_paths(graph, source=example_bp_id, target=target_bp_id)

# print('\n', 'simple paths from', example_bp, 'up to the root', target_bp, '\n')

# for path in paths:
#     filtered_path = list(path)[:-3]
#     print('-', ' -> '.join(node for node in filtered_path))

for k, v in id_to_name.items():
    list_of_paths = []
    simple_paths = []
    try:
        simple_paths = networkx.all_simple_paths(graph, source=k, target=target_bp_id)
    finally:
        list_of_paths = [list(path)[:-3] for path in simple_paths]
        set_of_paths = set(itertools.chain.from_iterable(list_of_paths))

    obj = {'go_id': k, 'go_term': v, 'go_paths': list_of_paths, 'go_set': list(set_of_paths)}

    # print('for go term k,v', k, v, 'list_of_paths', len(list_of_paths))
    print(json.dumps(obj))
