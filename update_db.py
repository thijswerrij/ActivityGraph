# -*- coding: utf-8 -*-
"""
@author: Thijs Werrij
"""

from activitypub.manager import Manager
from activitypub.database import MongoDatabase
#from activitypub.database import *

from cypher_read import sendSimpleQuery

#%%

## Pick one:
#db = RedisDatabase("redis://localhost:6379/0")
db = MongoDatabase("mongodb://localhost:27017", "dsblank_localhost")
#db = ListDatabase()
#db = SQLDatabase("sqlite://")
#db = SQLDatabase("sqlite:///sqlite.db")

manager = Manager(database=db)

static_attributes =  ['@context', 'id', 'uuid', 'published', 'object_id', '_id']

exclude_from_nodes = static_attributes + ['id', 'type', 'attributedTo', 'to']

#%%

def updateDB():
    new_activities = sorted(list(db.activities.find({'db_status': 'new'})), key = lambda i: i['time_posted'])
    for a in new_activities:
        aType = a['type']
        obj = retrieveObject(a)
        
        if obj:
            if 'Create' in aType:
                print('Create')
                
                removeAttributes(obj, exclude_from_nodes)
                node = createNode(obj)
                
                if node:
                    for record in node:
                        #print(record[0], record[1])
                        graph_id = record[1]
                        
                    db.activities.find_one_and_update({"remote_id": a["remote_id"]}, {"$set": {"db_status": "", "object_id":str(graph_id)}})
                else:
                    db.activities.find_one_and_update({"remote_id": a["remote_id"]}, {"$set": {"db_status": ""}})
            elif 'Update' in aType:
                print('Update')
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
                
                #db.activities.delete_one({"remote_id": a['remote_id']})
                
            elif 'Delete' in aType:
                print('Delete')
                
                original_activity = db.activities.find_one({"remote_id": obj["id"]})
                
                #db.activities.delete_one({"remote_id": obj['id']})
                #db.activities.delete_one({"remote_id": a['remote_id']})
                
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
                

def retrieveObject(obj):
    if 'activity' in obj and 'object' in obj['activity']:
        return obj['activity']['object']
    return None

def removeAttributes(obj, attrList):
    for toRemove in attrList:
        obj.pop(toRemove, None)
    return obj

def createNode(obj):
    if not 'type' in obj:
        objType = "Note"
    else:
        objType = str(obj["type"])
    create_query = """CREATE (n:""" + objType + " " + stringifyDict(obj) + """) RETURN n, id(n)"""
    #print(create_query)
    return sendSimpleQuery(create_query)

def updateNode(obj, graphId):
    update_query = """MATCH (n) WHERE id(n) = """ + str(graphId) + """ SET n += """ + stringifyDict(obj) + """ RETURN n, id(n)"""
    return sendSimpleQuery(update_query)
    
def deleteNode(graphId):
    delete_query = """MATCH (n) WHERE id(n) = """ + str(graphId) + """ DETACH DELETE n RETURN n, id(n)"""
    return sendSimpleQuery(delete_query)
        

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
