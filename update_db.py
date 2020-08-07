# -*- coding: utf-8 -*-
"""
@author: Thijs Werrij
"""

from activitypub.manager import Manager
from activitypub.database import MongoDatabase
#from activitypub.database import *

from cypher_read import sendSimpleQuery

import re

#%%

## Pick one:
#db = RedisDatabase("redis://localhost:6379/0")
db = MongoDatabase("mongodb://localhost:27017", "dsblank_localhost")
#db = ListDatabase()
#db = SQLDatabase("sqlite://")
#db = SQLDatabase("sqlite:///sqlite.db")

manager = Manager(database=db)

static_attributes =  ['@context', 'id', 'uuid', 'published', 'object_id', '_id']

exclude_from_nodes = static_attributes + ['id', 'type']

#%%

# Loops through all marked activities and processes them
def updateDB():
    new_activities = sorted(list(db.activities.find({'db_status': 'new'})), key = lambda i: i['time_posted'])
    for a in new_activities:
        aType = a['type']
        obj = retrieveObject(a)
        
        # If the activity contains an object, continue with that extracted object
        if obj:
            if 'Create' in aType:
                removeAttributes(obj, exclude_from_nodes)
                node, graph_id = createNode(obj)
                
                if graph_id:
                    db.activities.find_one_and_update({"remote_id": a["remote_id"]}, {"$set": {"db_status": "", "object_id":str(graph_id)}})
                else:
                    db.activities.find_one_and_update({"remote_id": a["remote_id"]}, {"$set": {"db_status": ""}})
            elif 'Update' in aType:
                updates = obj
                objId = obj["id"]
                
                original_activity = db.activities.find_one({"remote_id": objId})
                to_be_updated = retrieveObject(original_activity)
                
                if (to_be_updated):
                    removeAttributes(updates, static_attributes)
                    to_be_updated.update(updates)
                    db.activities.find_one_and_update({"remote_id": objId}, {"$set": {"activity.object": to_be_updated}})
                    
                if "object_id" in original_activity:
                    removeAttributes(obj, exclude_from_nodes)
                    node = updateNode(obj, original_activity["object_id"])
                
                db.activities.delete_one({"remote_id": a['remote_id']})
                
            elif 'Delete' in aType:
                original_activity = db.activities.find_one({"remote_id": obj["id"]})
                
                db.activities.delete_one({"remote_id": obj['id']})
                db.activities.delete_one({"remote_id": a['remote_id']})
                
                tombstone = manager.Create(**{
                    'type': ['Tombstone'],
                    'id': obj['id']
                })
                
                db.activities.insert_one(tombstone.to_dict())
                
                if "object_id" in original_activity:
                    node = deleteNode(original_activity["object_id"])
        else:
            # Create/Update/Delete activity should contain object
            db.activities.find_one_and_update({"remote_id": a["remote_id"]}, {"$set": {"db_status": "invalid"}})
                
# Checks if activity contains object and returns it
def retrieveObject(obj):
    if 'activity' in obj and 'object' in obj['activity']:
        return obj['activity']['object']
    return None

# Removes a list of attributes from a dict, returns dict with removed values
# (original dict already gets modified in the process so it does not need to be returned)
def removeAttributes(obj, attrList):
    removed = {}
    for toRemove in attrList:
        removed[toRemove] = obj.pop(toRemove, None)
    return removed

def createNode(obj):
    if not 'type' in obj:
        objType = "Note"
    else:
        objType = str(obj["type"])
    
    edges = filterNodes(obj)
    
    create_query = """CREATE (n:""" + objType + " " + stringifyDict(obj) + """) RETURN n, id(n)"""
    
    node = sendSimpleQuery(create_query)
    
    graph_id = None
    if node:
        for record in node:
            graph_id = record[1]
    
    # For every property that links to another node, an edge needs to be created
    for k in edges.keys():
        if isinstance(edges[k],list):
            for e in edges[k]:
                edge_query = createEdgeQuery(k, e, graph_id)
                sendSimpleQuery(edge_query)
        else:
            edge_query = createEdgeQuery(k, edges[k], graph_id)
            sendSimpleQuery(edge_query)
    
    return node, graph_id

def updateNode(obj, graph_id):
    update_query = """MATCH (n) WHERE id(n) = """ + str(graph_id) + """ SET n += """ + stringifyDict(obj) + """ RETURN n, id(n)"""
    
    node = sendSimpleQuery(update_query)
    
    for record in sendSimpleQuery("""MATCH (n)-[r]->(m) WHERE id(n) = %s RETURN type(r)""" % graph_id):
        edgeName = str(record[0])
        if edgeName in obj:
            sendSimpleQuery("""MATCH (n)-[r]->(m) WHERE id(n) = %s AND type(r) = %s DELETE r""" % (graph_id, edgeName))
            
    edges = filterNodes(obj)
    
    # For every new/updated property that links to another node, an edge needs to be created
    for k in edges.keys():
        if isinstance(edges[k],list):
            for e in edges[k]:
                edge_query = createEdgeQuery(k, e, graph_id)
                sendSimpleQuery(edge_query)
        else:
            edge_query = createEdgeQuery(k, edges[k], graph_id)
            sendSimpleQuery(edge_query)
            
    return node
    
def deleteNode(graph_id):
    delete_query = """MATCH (n) WHERE id(n) = """ + str(graph_id) + """ DETACH DELETE n RETURN n, id(n)"""
    return sendSimpleQuery(delete_query)

# Some parameters are node ids: these need to be added as edges in the graph instead of as properties.
def filterNodes(obj):
    keys = obj.keys()
    toRemove = []
    
    for k in keys:
        value = obj[k]
        if isinstance(value,list) and isinstance(value[0],str) and (re.search("^https?:\/\/", value[0])):
            toRemove.append(k)
        elif isinstance(value,str) and (re.search("^https?:\/\/", value)):
            toRemove.append(k)
    
    return removeAttributes(obj, toRemove)     

def createEdgeQuery(name, url, ingoingId):
    outgoingNode = db.actors.find_one({"id": str(url)})
    if not outgoingNode:
        outgoingNode = db.activities.find_one({"remote_id": str(url)})
        
    if outgoingNode and "object_id" in outgoingNode:
        edge_query = """MATCH (n), (m) WHERE id(n) = %s AND id(m) = %s CREATE (n)-[r:%s]->(m)"""
        return edge_query % (ingoingId, outgoingNode["object_id"], name)
    return None

# Turns dicts into strings in such a way they can be used in queries
def stringifyDict(obj):
    dict_string = "{ "
    for k in obj.keys():
        dict_string = dict_string + str(k) + ": "
        if isinstance(obj[k],dict):
            dict_string = dict_string + stringifyDict(obj[k]) + ","
        elif not isinstance(obj[k],str):
            dict_string = dict_string + str(obj[k]) + ','
        else:
            dict_string = dict_string + '"' + obj[k].replace('"','\\"') + '",'
    dict_string = dict_string[:-1]+ " }"
    return (dict_string)
        
updateDB()
