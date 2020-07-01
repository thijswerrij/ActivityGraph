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

## Pick one:
#database = RedisDatabase("redis://localhost:6379/0")
db = MongoDatabase("mongodb://localhost:27017", "dsblank_localhost")
#database = ListDatabase()
#database = SQLDatabase("sqlite://")
#datavbase = SQLDatabase("sqlite:///sqlite.db")

manager = Manager(database=db)

if manager.database.table_exists("activities"):
    manager.database.activities.clear()
else:
    manager.database.build_table("activities")
manager.database.activities.clear()
manager.database.actors.clear()
manager.database.objects.clear()

def initializeGraph():
    initial_nodes = """
    MATCH (n)
    WHERE NOT EXISTS (n.graph_status)
    RETURN n, labels(n), keys(n)
    ORDER BY n.time
    """
    
    initial_edges = """
    MATCH (a)-[e]->(b)
    WHERE NOT EXISTS (e.graph_status)
    RETURN e, type(e), labels(a), labels(b)
    ORDER BY e.time
    """
    
    for record in session.run(initial_nodes):
        createObject(record[0], record[1], record[2])
            
    for e in session.run(initial_edges):
        nodes = e[0].nodes
        createEdge(nodes[0],nodes[1],e[1],e[2],e[3])
    
    checkForUpdates()
    
#%%

match_nodes = """
MATCH (n)
WHERE n.graph_status = "%s"
RETURN n, labels(n), keys(n)
ORDER BY n.time
"""

match_edges = """
MATCH (a)-[e]->(b)
WHERE e.graph_status = "%s"
RETURN e, type(e), labels(a), labels(b)
ORDER BY e.time
"""

def checkForUpdates(originalQuery=None):
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
            createEdge(nodes[0],nodes[1],e[1],e[2],e[3])
    
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
    
    if deleted_edges.peek() or deleted_nodes.peek():
        session.run(originalQuery)
        
        if deleted_edges.peek():
            print('Deleted edges')
        
            for e in deleted_edges:
                nodes = e[0].nodes
                removeEdge(nodes[0],nodes[1],e[1],e[2],e[3])
        
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
    
    #finalizeGraph()
        
def createObject(node, labels, keys):
    node_id = node.id
    
    property_values = []
    
    for k in keys:
        property_values.append(node[k])
    
    print(node_id, labels, keys, property_values)
    
    objDict = {"object_id" : str(node_id)}
    
    for i in range(len(keys)):
        objDict[keys[i]] = property_values[i]
    
    #print(objDict)
    if "Person" in labels:
        p = manager.Person(**objDict)
        db.actors.insert_one(p.to_dict())
    else:
        note = manager.Note(**objDict)
        
        create = manager.Create(**{
            'object': note.to_dict(),
            'published': '$NOW',
            }
        )
        
        message = manager.Create(
        **{
            'object_id': str(node_id),
            'activity': create.to_dict(),
            'box': 'outbox',
            'type': ['Create'],
            'meta': {'undo': False, 'deleted': False},
            })
        
        #db.objects.insert_one(o.to_dict())
        db.activities.insert_one(message.to_dict())
        
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
        updatedNode = db.actors.find_one_and_update({"object_id": str(node_id)}, {"$set": objDict})
    else:
        updatedNode = db.activities.find_one_and_update({"object_id": str(node_id)}, {"$set": objDict})
    print(updatedNode)
    
def deleteObject(node, labels):
    node_id = node.id
    
    if "Person" in labels:
        db.actors.remove({"object_id": str(node_id)})
    else:
        db.activities.remove({"object_id": str(node_id)})
        
def createEdge(ingoingNode, outgoingNode, name, labelsA, labelsB):
    id1 = ingoingNode.id
    id2 = outgoingNode.id
    
    print(id1,id2, name)
    if "Person" in labelsB:
        node2 = db.actors.find_one({"object_id": str(id2)})
    else:
        node2 = db.activities.find_one({"object_id": str(id2)})
        
    print(node2)
    
    if "Person" in labelsA:
        node1 = db.actors.find_one_and_update({"object_id": str(id1)}, {"$set": {name: node2}})
    else:
        node1 = db.activities.find_one_and_update({"object_id": str(id1)}, {"$set": {name: node2}})
    
    print(node1)

def updateEdge():
    #TODO ?
    print('todo updateEdge')

def removeEdge(ingoingNode, outgoingNode, name, labelsA, labelsB):
    id1 = ingoingNode.id
    id2 = outgoingNode.id
    
    print(id1,id2, name)
    if "Person" in labelsA:
        node1 = db.actors.find_one({"object_id": str(id1)}).__delitem__(name)
        db.actors.remove({"object_id": str(id1)})
        db.actors.insert_one(node1)
    else:
        node1 = db.activities.find_one({"object_id": str(id1)}).__delitem__(name)
        db.activities.remove({"object_id": str(id1)})
        db.activities.insert_one(node1)
    
def finalizeGraph():
    
    updateNodesQuery = """MATCH (n)
    WHERE n.graph_status = 'new' OR n.graph_status = 'updated'
    REMOVE n.graph_status, n.time
    RETURN n"""
    
    updateEdgesQuery = """MATCH (n)-[e]->(m)
    WHERE e.graph_status = 'new' OR e.graph_status = 'updated'
    REMOVE e.graph_status, e.time
    RETURN e"""
    
    removeNodesQuery = """"""
    
    session.run(finalizeNodesQuery)
    session.run(finalizeEdgesQuery)

#%%

initializeGraph()

#%% Close session
#session.close()
