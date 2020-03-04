# -*- coding: utf-8 -*-
"""
@author: Thijs Werrij
"""

from neo4j import GraphDatabase, basic_auth
from time import time

#%%

driver = GraphDatabase.driver("bolt://localhost", auth=basic_auth("neo4j", "hunter2"))
session = driver.session()

#%% Functions

create_query = """CREATE (p:Person {name:'Eve'})"""
delete_query = """MATCH (p:Person {name: 'Eve'}) DELETE p"""

#%% Test matches

match_all = """
MATCH (n)
RETURN n, n.time_created
"""

match_connections = """
MATCH (n1)-[r]->(n2) RETURN r, n1, n2 LIMIT 25
"""

match_person = """
MATCH (user:Person)
RETURN user.name AS name
"""

results = session.run(match_all)
for record in results:
    print(record)
    
#%%

match_new_nodes = """
MATCH (n)
WHERE n.graph_status = "new"
RETURN n, labels(n), keys(n)
"""

match_new_edges = """
MATCH (a)-[e]->(b)
WHERE e.graph_status = "new"
RETURN e
"""

def checkForUpdates():
    node_results = session.run(match_new_nodes)
    edge_results = session.run(match_new_edges)
    
    edges = []
    
    if edge_results:
        print('New edges')
        for e in edge_results:
            edges.append(e[0])
    
    if node_results:
        print('New nodes')
        for record in node_results:
            createObject(record[0], record[1], record[2], edges)
        
def createObject(node, labels, keys, edges):
    node_id = node.id
    
    property_values = []
    
    for k in keys:
        property_values.append(node[k])
    
    print(node_id, labels, property_values)

checkForUpdates()

#%% Close session
#session.close()