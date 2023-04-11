import pandas as pd
import json
import os
from sknetwork.data import from_adjacency_list
from sknetwork.clustering import Louvain, get_modularity
from sknetwork.utils import get_neighbors


BASE_DIR = os.path.dirname(os.path.realpath(__file__))


def get_graph():
    df = pd.read_excel(BASE_DIR + '/data_storage/graph_data.xlsx')
    df['ИНН'] = df['ИНН'].astype("string")
    df['carrier'] = df['Перевозчик'] + ' ' + df['ИНН']
    df['home_region'] = df['Регион перевозчика'].str.strip()
    df['uploading_region'] = df['Регион погрузки'].str.strip()
    df['unloading_region'] = df['Регион разгрузки'].str.strip()
    df = df.drop(['Перевозчик', 'ИНН', 'Регион перевозчика', 'Регион погрузки', 'Регион разгрузки'], axis=1).dropna()

    route_list = []
    for row in df.iterrows():
        sorted_values = sorted([row[1]['uploading_region'], row[1]['unloading_region']])
        route_list.append(sorted_values[0] + ' - ' + sorted_values[1])

    df['route'] = route_list

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


def get_nodes_outside_persent(labels_df, node_info_df, graph):
    for node_id in labels_df.node_id.values:
        target_label = labels_df[labels_df.node_id == node_id].cluster_id.values[0]
        node_type = node_info_df[node_info_df.node == labels_df[labels_df.node_id == node_id].node.values[0]].\
            node_type.values[0]
        node_name = node_info_df[node_info_df.node == labels_df[labels_df.node_id == node_id].node.values[0]].\
            node.values[0]
        neighbors = get_neighbors(graph[0].adjacency, node_id)

        outside_persent = 0.0
        sum_weight = 0
        for neighbor in neighbors:
            neighbor_name = node_info_df[node_info_df.node == labels_df[labels_df.node_id == neighbor].node.values[0]].\
                node.values[0]
            neighbor_label = labels_df[labels_df.node_id == neighbor].cluster_id.values[0]
            neighbor_weight = 1
            if node_type == 'carrier':
                neighbor_weight = graph[1][(graph[1].carrier == node_name) & (graph[1].route == neighbor_name)].shape[0]
            elif node_type == 'route':
                neighbor_weight = graph[1][(graph[1].carrier == neighbor_name) & (graph[1].route == node_name)].shape[0]

            sum_weight += neighbor_weight
            if neighbor_label != target_label:
                outside_persent += neighbor_weight

        outside_persent /= sum_weight
        labels_df.loc[labels_df[labels_df.node_id == node_id].index, 'outside_persent'] = outside_persent

    return labels_df


def get_result(graph, labels):
    df = graph[1].copy()
    node_info_df = pd.DataFrame(columns=['node', 'node_type'])

    for node_type in ['carrier', 'route']:
        tmp_df = pd.DataFrame(columns=['node', 'node_type'])
        tmp_df['node'] = df[node_type].unique()
        tmp_df['node_type'] = node_type

        node_info_df = pd.concat([node_info_df, tmp_df])

    labels_df = pd.DataFrame(columns=['node_id', 'node', 'cluster_id', 'outside_persent'])
    labels_df.node_id = range(0, len(labels))
    labels_df.node = graph[0].names
    labels_df.cluster_id = labels
    labels_df = pd.merge(labels_df, node_info_df, on='node')

    labels_df = get_nodes_outside_persent(labels_df, node_info_df, graph)
    labels_df = pd.merge(labels_df, df[['route', 'uploading_region', 'unloading_region']].
                         drop_duplicates(subset=['route']).
                         rename(columns={'route': 'node'}), on='node', how='left')

    return labels_df


def get_result_dict(source_df):
    clusters_lst = list()
    for cluster_id in source_df.cluster_id.unique():
        cluster_dct = dict()
        cluster_dct['id'] = int(cluster_id)

        destinations_lst = list()
        carriers_lst = list()
        for row in source_df[source_df.cluster_id == cluster_id].iterrows():
            if row[1]['node_type'] == 'route':
                destination_dct = dict()
                destination_dct['from'] = row[1]['uploading_region']
                destination_dct['to'] = row[1]['unloading_region']
                destination_dct['risk'] = row[1]['outside_persent']

                destinations_lst.append(destination_dct)
            elif row[1]['node_type'] == 'carrier':
                carriers_dct = dict()
                carriers_dct['name'] = row[1]['node']
                carriers_dct['risk'] = row[1]['outside_persent']

                carriers_lst.append(carriers_dct)

        cluster_dct['destinations'] = destinations_lst
        cluster_dct['carriers'] = carriers_lst

        clusters_lst.append(cluster_dct)

    result_dct = dict()
    result_dct['clusters'] = clusters_lst

    return result_dct


def calc_clusters():
    graph = get_graph()
    cluster_labels = get_cluster_labels(graph[0].adjacency, print_modularity=False)
    result_df = get_result(graph, cluster_labels)
    result_dct = get_result_dict(result_df)

    writer = pd.ExcelWriter(BASE_DIR + '/result_storage/cluster_result.xlsx')
    result_df.to_excel(writer, sheet_name='res', index=False)
    writer.close()

    with open(BASE_DIR + '/result_storage/cluster_result.json', 'w') as outfile:
        json.dump(result_dct, outfile)

    return result_dct


if __name__ == '__main__':
    graph = get_graph()
    cluster_labels = get_cluster_labels(graph[0].adjacency, print_modularity=True)
    result_df = get_result(graph, cluster_labels)
    result_dct = get_result_dict(result_df)

    writer = pd.ExcelWriter(BASE_DIR + '/result_storage/cluster_result.xlsx')
    result_df.to_excel(writer, sheet_name='res', index=False)
    writer.close()

    with open(BASE_DIR + '/result_storage/cluster_result.json', 'w') as outfile:
        json.dump(result_dct, outfile)
