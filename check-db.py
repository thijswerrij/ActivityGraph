# -*- coding: utf-8 -*-
"""
@author: Thijs Werrij
"""

from neo4j import GraphDatabase, basic_auth
from time import time

from activitypub.manager import Manager
from activitypub.database import *

#%%

driver = GraphDatabase.driver("bolt://localhost", auth=basic_auth("neo4j", "hunter2"))
session = driver.session()

db = MongoDatabase("mongodb://localhost:27017", "dsblank_localhost")
manager = Manager(database=db)

if manager.database.table_exists("activities"):
    manager.database.activities.clear()
else:
    manager.database.build_table("activities")
manager.database.activities.clear()
manager.database.actors.clear()
manager.database.objects.clear()

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

match_nodes = """
MATCH (n)
WHERE n.graph_status = "%s"
RETURN n, labels(n), keys(n)
"""

match_edges = """
MATCH (a)-[e]->(b)
WHERE e.graph_status = "%s"
RETURN e, type(e)
"""

def checkForUpdates():
    created_nodes = session.run(match_nodes % "new")
    created_edges = session.run(match_edges % "new")
    
    updated_nodes = session.run(match_nodes % "updated")
    updated_edges = session.run(match_edges % "updated")
    
    deleted_nodes = session.run(match_nodes % "delete")
    deleted_edges = session.run(match_edges % "delete")
    
    detached_nodes = session.run(match_nodes % "detach")
    
    if created_nodes.peek():
        print('New nodes')
        for record in created_nodes:
            createObject(record[0], record[1], record[2])
            
    if created_edges.peek():
        print('New edges')
        for e in created_edges:
            nodes = e[0].nodes
            print(e)
            createEdge(nodes[0],nodes[1],e[1])
    
    if updated_edges.peek():
        print('Updated edges')
        for e in updated_edges:
            #TODO?
            print(e)
    
    if updated_nodes.peek():
        print('Updated nodes')
        for record in updated_nodes:
            print(record)
            updateObject(record[0], record[1], record[2])
    
    if deleted_edges.peek():
        print('Deleted edges')
        for e in deleted_edges:
            nodes = e[0].nodes
            removeEdge(nodes[0],nodes[1],e[1])
        
    if deleted_nodes.peek():
        print('Deleted nodes')
        for record in deleted_nodes:
            deleteObject(record[0], record[1])
            
    if detached_nodes.peek():
        print('Deleted nodes')
        for record in deleted_nodes:
            deleteObject(record[0], record[1])
            
        to_detach_nodes = session.run("MATCH (n)-[e]->(d) WHERE id(d)= " + record[0].id + " RETURN n, type(e)")
        
        for d in to_detach_nodes:
            removeEdge(d[0],record[0],d[1])
        
def createObject(node, labels, keys):
    node_id = node.id
    
    property_values = []
    
    for k in keys:
        property_values.append(node[k])
    
    print(node_id, labels, keys, property_values)
    
    objDict = {"id" : str(node_id)}
    
    for i in range(len(keys)):
        objDict[keys[i]] = property_values[i]
    
    #print(objDict)
    if "Person" in labels:
        p = manager.Person(**objDict)
        db.actors.insert_one(p.to_dict())
    else:
        o = manager.Note(**objDict)
        db.objects.insert_one(o.to_dict())
        
def updateObject(node, labels, keys):
    node_id = node.id
    
    property_values = []
    
    for k in keys:
        property_values.append(node[k])
    
    print(node_id, labels, keys, property_values)
    
    objDict = {}
    
    for i in range(len(keys)):
        objDict[keys[i]] = property_values[i]
    
    if "Person" in labels:
        updatedNode = db.actors.find_one_and_update({"id": str(node_id)}, {"$set": objDict})
    else:
        updatedNode = db.objects.find_one_and_update({"id": str(node_id)}, {"$set": objDict})
    print(updatedNode)
    
def deleteObject(node, labels):
    node_id = node.id
    
    if "Person" in labels:
        db.actors.remove({"id": str(node_id)})
    else:
        db.objects.remove({"id": str(node_id)})
        
def createEdge(ingoingNode, outgoingNode, name):
    id1 = ingoingNode.id
    id2 = outgoingNode.id
    
    print(id1,id2, name)
    node2 = db.objects.find_one({"id": str(id2)})
    print(node2)
    node1 = db.objects.find_one_and_update({"id": str(id1)}, {"$set": {name: node2}})
    print(node1)

def updateEdge():
    #TODO ?
    print('todo updateEdge')

def removeEdge(ingoingNode, outgoingNode, name):
    id1 = ingoingNode.id
    id2 = outgoingNode.id
    
    print(id1,id2, name)
    node1 = db.objects.find_one({"id": str(id1)}).__delitem__(name)
    db.objects.remove({"id": str(id1)})
    db.actors.insert_one(node1)

checkForUpdates()

#%% Close session
#session.close()