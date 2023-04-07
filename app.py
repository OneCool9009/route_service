import pandas as pd
import numpy as np
from sknetwork.data import from_adjacency_list
from sknetwork.clustering import Louvain, get_modularity
from sknetwork.utils import get_neighbors, get_weights


def get_graph_from_csv(path):
    df = pd.read_csv(path, sep=';')

    adjacency_dict = dict()
    # home_region_list = df.home_region.unique()
    # for home_region in home_region_list:
    #     adjacency_dict[home_region] = list(df[df.home_region == home_region].carrier.values)

    carrier_list = df.carrier.unique()
    for carrier in carrier_list:
        adjacency_dict[carrier] = list(df[df.carrier == carrier].route.values)

    return [from_adjacency_list(adjacency_dict, directed=False, weighted=True, sum_duplicates=True), df]


def get_graph_from_request(data_dct):
    df = pd.DataFrame.from_dict(data_dct)

    adjacency_dict = dict()
    # home_region_list = df.home_region.unique()
    # for home_region in home_region_list:
    #     adjacency_dict[home_region] = list(df[df.home_region == home_region].carrier.values)

    carrier_list = df.carrier.unique()
    for carrier in carrier_list:
        adjacency_dict[carrier] = list(df[df.carrier == carrier].route.values)

    return [from_adjacency_list(adjacency_dict, directed=False, weighted=True, sum_duplicates=True), df]


def get_cluster_labels(adjacency, print_modularity=False):
    louvain = Louvain(modularity='dugue')
    louvain.fit(adjacency)

    if print_modularity:
        print(get_modularity(adjacency, louvain.labels_))

    return louvain.labels_


def get_result(graph, labels):
    df = graph[1]
    node_info_df = pd.DataFrame(columns=['node', 'node_type'])
    tmp_df = pd.DataFrame(columns=['node', 'node_type'])
    tmp_df['node'] = df.carrier.unique()
    tmp_df['node_type'] = 'carrier'
    node_info_df = pd.concat([node_info_df, tmp_df])

    tmp_df = pd.DataFrame(columns=['node', 'node_type'])
    tmp_df['node'] = df.route.unique()
    tmp_df['node_type'] = 'route'
    node_info_df = pd.concat([node_info_df, tmp_df])

    labels_df = pd.DataFrame(columns=['node_id', 'node', 'cluster_id', 'outside_persent'])
    labels_df.node_id = range(0, len(labels))
    labels_df.node = graph[0].names
    labels_df.cluster_id = labels
    labels_df = pd.merge(labels_df, node_info_df, on='node')

    for node_id in labels_df.node_id.values:
        target_label = labels_df[labels_df.node_id == node_id].cluster_id.values[0]
        node_type = node_info_df[node_info_df.node == labels_df[labels_df.node_id == node_id].node.values[0]].node_type.values[0]
        node_name = node_info_df[node_info_df.node == labels_df[labels_df.node_id == node_id].node.values[0]].node.values[0]
        neighbors = get_neighbors(graph[0].adjacency, node_id)

        outside_persent = 0.0
        sum_weight = 0
        for neighbor in neighbors:
            neighbor_name = node_info_df[node_info_df.node == labels_df[labels_df.node_id == neighbor].node.values[0]].node.values[0]
            neighbor_label = labels_df[labels_df.node_id == neighbor].cluster_id.values[0]
            neighbor_weight = 1
            if node_type == 'carrier':
                neighbor_weight = df[(df.carrier == node_name) & (df.route == neighbor_name)].shape[0]
            elif node_type == 'route':
                neighbor_weight = df[(df.carrier == neighbor_name) & (df.route == node_name)].shape[0]

            sum_weight += neighbor_weight
            if neighbor_label != target_label:
                outside_persent += neighbor_weight

        outside_persent /= sum_weight
        labels_df.loc[labels_df[labels_df.node_id == node_id].index, 'outside_persent'] = outside_persent

    return labels_df


def calc_clusters(request_dct):
    graph = get_graph_from_request(request_dct['data'])
    cluster_labels = get_cluster_labels(graph[0].adjacency, print_modularity=False)
    result_df = get_result(graph, cluster_labels)

    return {'data': result_df.to_dict()}


if __name__ == '__main__':
    graph = get_graph_from_csv('graph_data.csv')
    cluster_labels = get_cluster_labels(graph[0].adjacency, print_modularity=True)
    result_df = get_result(graph, cluster_labels)

    writer = pd.ExcelWriter('./debug_results/cluster_result.xlsx')
    result_df.to_excel(writer, sheet_name='res', index=False)
    writer.close()
