import networkx as nx
from networkx.readwrite import json_graph
import json
import numpy as np


def dagSimilarity(G, H):
    """
    Run spectral graph embedding
    returns similarity score [0, inf), lower is better
    """
    l1 = laplacian_spectrum( G )
    l2 = laplacian_spectrum( H )

    s1 = np.linalg.svd( l1,  compute_uv=False )
    s2 = np.linalg.svd( l2,  compute_uv=False )
    
    k1 = np.linalg.matrix_rank( l1,  0.9 )
    k2 = np.linalg.matrix_rank( l2,  0.9 )    
    k = min(k1, k2)

    similarity = sum((s1[:k] - s2[:k])**2)
    return similarity
    
    print "similarity score: ",  similarity

def laplacian_spectrum( G ):
    L = nx.to_numpy_matrix( G )
    degrees = np.sum( L,  axis=1 )
    for i in range( 0, L.shape[0] ):
        L[i, i] = degrees[i]
    return L

def select_k(spectrum, minimum_energy = 0.9):
    running_total = 0.0
    total = np.sum(spectrum)
    if total == 0.0:
        return len(spectrum)
    for i in range(len(spectrum)):
        running_total += np.sum( spectrum[i] )
        print "running total: ",  running_total
        if running_total / total >= minimum_energy:
            return i + 1
    return len(spectrum)


if __name__ == '__main__':
    data1 = '{"directed": true, "graph": {}, "nodes": [{"name": "ParallelCollectionRDD", "id": 0}, {"name": "MapPartitionsRDD", "id": 1}],'
    data1 += ' "links": [{"source": 0, "target": 1}], "multigraph": false}'
    data2 = '{"directed": true, "graph": {}, "nodes": [{"name": "ParallelCollectionRDD", "id": 2}, {"name": "ParallelCollectionRDD", "id": 0}, {"name": "MapPartitionsRDD", "id": 1}, {"name": "MapPartitionsRDD", "id": 2}],'
    data2 +=' "links": [{"source": 0, "target": 1}, {"source": 0, "target": 2}], "multigraph": false}'
    
    dataDict1 = json.loads(data1)
    dataDict2 = json.loads(data2)
    
    G = json_graph.node_link_graph(dataDict1)
    H = json_graph.node_link_graph(dataDict2)

    dagSimilarity( G,  H )

