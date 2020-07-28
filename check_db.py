# -*- coding: utf-8 -*-
"""
@author: Thijs Werrij
"""

from activitypub.manager import Manager
from activitypub.database import MongoDatabase
#from activitypub.database import *

#%%

## Pick one:
#db = RedisDatabase("redis://localhost:6379/0")
db = MongoDatabase("mongodb://localhost:27017", "dsblank_localhost")
#db = ListDatabase()
#db = SQLDatabase("sqlite://")
#db = SQLDatabase("sqlite:///sqlite.db")

manager = Manager(database=db)

#%%

def initializeGraph(session):
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

def checkForUpdates(session, originalQuery=None, testing=True):
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
            createEdge(nodes[0],nodes[1],e[1],e[2],e[3])
    
    if updated_edges.peek():
        print('Updated edges')
        for e in updated_edges:
            #TODO?
            print(e)
    
    if updated_nodes.peek():
        print('Updated nodes')
        for record in updated_nodes:
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
        print('Detached nodes')
        
        # First remove edges, then the nodes
        query_detached_edges = """MATCH (a)-[e]->(b)
        WHERE b.graph_status = "detach"
        RETURN e, type(e), labels(a), labels(b)"""
        
        to_detach_edges = session.run(query_detached_edges)
        
        for d in to_detach_edges:
            nodes = d[0].nodes
            removeEdge(nodes[0],nodes[1],d[1],d[2],d[3])
        
        for record in detached_nodes:
            deleteObject(record[0], record[1])
        
def createObject(node, labels, keys):
    node_id = node.id
    
    property_values = []
    
    for k in keys:
        property_values.append(node[k])
    
    objDict = {"object_id" : str(node_id)}
    
    for i in range(len(keys)):
        objDict[keys[i]] = property_values[i]
        
    del objDict['graph_status']
    
    if "Person" in labels:
        if "name" in objDict and not "id" in objDict:
            objDict["id"] = objDict["name"].lower()
        p = manager.Person(**objDict)
        db.actors.insert_one(p.to_dict())
    else:
        objDict.update(**{'attributedTo': '$DOMAIN',
            'published': '$NOW',
            'uuid': "$UUID",
            })
        
        note = manager.Note(**objDict)
        
        create = manager.Create(**{
            'object': note.to_dict(),
            'published': '$NOW',
            'id': str(note.attributedTo) + '/outbox/' + str(note.uuid),
            }
        )
        
        message = manager.Create(
        **{
            'object_id': str(node_id),
            'activity': create.to_dict(),
            'box': 'outbox',
            'type': ['Create'],
            'meta': {'undo': False, 'deleted': False},
            'remote_id': str(note.attributedTo) + '/outbox/' + str(note.uuid),
            })
        
        db.activities.insert_one(message.to_dict())
        
def updateObject(node, labels, keys):
    node_id = node.id
    
    property_values = []
    
    for k in keys:
        property_values.append(node[k])
    
    objDict = {}
    
    for i in range(len(keys)):
        objDict[keys[i]] = property_values[i]
        
    del objDict['graph_status']
    
    if "Person" in labels:
        updatedNode = db.actors.find_one_and_update({"object_id": str(node_id)}, {"$set": objDict})
    else:
        updatedNode = db.activities.find_one_and_update({"object_id": str(node_id)}, {"$set": objDict})
    
def deleteObject(node, labels):
    node_id = node.id
    
    if "Person" in labels:
        db.actors.remove({"object_id": str(node_id)})
    else:
        db.activities.remove({"object_id": str(node_id)})
        
def createEdge(ingoingNode, outgoingNode, name, labelsA, labelsB):
    id1 = ingoingNode.id
    id2 = outgoingNode.id
    
    if "Person" in labelsB:
        node2 = db.actors.find_one({"object_id": str(id2)})
    else:
        node2 = db.activities.find_one({"object_id": str(id2)})
    
    if "Person" in labelsA:
        setOutgoingEdge(db.actors, id1, name, node2)
    else:
        setOutgoingEdge(db.activities, id1, name, node2)

def updateEdge():
    #TODO ?
    print('todo updateEdge')

def removeEdge(ingoingNode, outgoingNode, name, labelsA, labelsB):
    id1 = ingoingNode.id
    id2 = outgoingNode.id
    
    if "Person" in labelsB:
        node2 = db.actors.find_one({"object_id": str(id2)})
    else:
        node2 = db.activities.find_one({"object_id": str(id2)})
    
    if "Person" in labelsA:
        removeOutgoingEdge(db.actors, id1, name, node2)
    else:
        removeOutgoingEdge(db.activities, id1, name, node2)
        
def setOutgoingEdge(table, idNr, edgeName, outgoingNode):
    # Helper function created so that instead of overwriting edges with the same name,
    # a list of edges for that name is added as a property
    # So for example (Bob)<-(Alice)->(Eve) should result in Alice having
    # property 'friends': [Bob,Eve]
    
    node = table.find_one({"object_id": str(idNr)})
    
    nested = ""
    
    if 'id' in outgoingNode:
        outgoingId = outgoingNode['id']
    else:
        outgoingId = outgoingNode['remote_id']
    
    if table.name == 'activities':
        # if the object is an activity, the edge has to be added to the nested object
        nested = "activity.object."
        node = node["activity"]["object"]
    
    if edgeName in node:
        value = node[edgeName]
        print('value', type(value), value)
        if type(value) is list:
            value.append(outgoingId)
            
            nodeResult = table.find_one_and_update({"object_id": str(idNr)}, {"$set": {nested + edgeName: value}})
        else:
            raise Exception('Expected list, got ' + type(value))
    else:
        nodeResult = table.find_one_and_update({"object_id": str(idNr)}, {"$set": {nested + edgeName: [outgoingId]}})
    
    if table.name == 'activities' and edgeName in ["attributedTo", "actor", "author"]: # check if the outgoing node is possibly an author
        nodeResult = table.find_one_and_update({"object_id": str(idNr)}, {"$set": {nested + "attributedTo": outgoingId}})
        uuid = nodeResult['activity']['object']['uuid']
        nodeResult = table.find_one_and_update({"object_id": str(idNr)},
                                                {"$set": {'remote_id': outgoingId + '/outbox/' + str(uuid),
                                                          'activity.id': outgoingId + '/outbox/' + str(uuid)}})
    return nodeResult 

def removeOutgoingEdge(table, idNr, edgeName, outgoingNode):
    
    node = table.find_one({"object_id": str(idNr)})
    
    toDelete = None
    nested = ""
    
    if 'id' in outgoingNode:
        outgoingId = outgoingNode['id']
    else:
        outgoingId = outgoingNode['remote_id']
    
    if table.name == 'activities':
        # if the object is an activity, the edge has to be removed from the nested object
        nested = "activity.object."
        node = node["activity"]["object"]
    
    if edgeName in node:
        value = node[edgeName]
        if type(value) is list:
            for n in value:
                if n == str(outgoingId):
                    toDelete = n
            
            # check if edge is found
            if toDelete:
                value.remove(toDelete)
            
            # check if list is empty
            if len(value) is 0:
                del node[edgeName]
                if table.name == 'activities':
                    table.find_one_and_update({"object_id": str(idNr)}, {"$set": {"activity.object": node}})
                else:
                    table.remove({"object_id": str(idNr)})
                    table.insert_one(node)
            else:
                table.find_one_and_update({"object_id": str(idNr)}, {"$set": {nested + edgeName: value}})
        else:
            raise Exception('Expected list, got ' + type(value))
        
    
def resetDB():
    # WARNING, RESETS DB
    manager.database.actors.clear()
    manager.database.activities.clear()

#%% Close session
#session.close()
