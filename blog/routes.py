# -*- coding: utf-8 -*-
"""
@author: Thijs Werrij
"""

from activitypub.manager.base import app
from flask import request

from activitypub.manager import FlaskManager as Manager
from activitypub.database import MongoDatabase

database = MongoDatabase("mongodb://localhost:27017", "dsblank_localhost")
manager = Manager(database=database)

#%%

def orderedCollection(items):
    if not isinstance(items, list):
        items = [items]
    contents = {
        "@context": "https://www.w3.org/ns/activitystreams",
        "type": "OrderedCollection",
        "totalItems": len(items),
        "orderedItems": items,}
    return contents

domain_url = "http://localhost:5000/"

def setIdToString(obj):
    if "_id" in obj:
        obj["_id"] = str(obj["_id"])
        for o in obj:
            if isinstance(obj[o], list):
                for i in range(len(obj[o])):
                    if isinstance(obj[o][i], dict):
                        obj[o][i] = setIdToString(obj[o][i])
            elif isinstance(obj[o], dict):
                obj[o] = setIdToString(obj[o])
        return obj
    return obj

def findUser(self, nickname):
    obj = self.database.actors.find_one({'id': domain_url + nickname})
    if isinstance(obj, dict):
        obj = setIdToString(obj)
        return obj
    return None

@app.route("/user/<nickname>", ["GET"])
def route_user(self, nickname):
    obj = findUser(self, nickname)
    if obj:
        return self.render_json(
            obj
        )
    else:
        return self.error(404)

@app.route("/user/<nickname>/outbox", ["GET", "POST"])
def route_user_outbox(self, nickname):
    obj = findUser(self, nickname)
    if obj:
        return sendToOutbox(self, obj)
    else:
        return self.error(404)
    
@app.route("/outbox", ["GET", "POST"])
def route_outbox(self):
    return sendToOutbox(self)

def sendToOutbox(self, obj=None):
    if request.method == "GET":
        activities = []
        if not obj is None:
            entries = self.database.activities.find({'box': 'outbox', 'activity.object.attributedTo': obj['id']})
        else:
            entries = self.database.activities.find({'box': 'outbox'})
        
        for a in entries:
            activities.append(setIdToString(a))
        return self.render_json(
            orderedCollection(activities)
        )
    elif request.method == "POST":
        data = request.get_json(force=True)#.to_dict()
        
        if obj is None:
            attributed = '$DOMAIN'
        else:
            attributed = obj['id']
        
        if not 'type' in data:
            data.update(**{'type': 'Note'})
        
        data.update(**{'id': attributed + '/' + data['type'].lower()})
        
        data.update(**{'attributedTo': attributed,
                'published': '$NOW',
                'temp_uuid': "$UUID",
        })
        
        create = manager.Create(**{
                'object': data,
                'published': '$NOW',
                'id': str(data.attributedTo) + '/outbox/' + str(data.uuid),
        })
        
        message = manager.Create(
        **{
                'activity': create.to_dict(),
                'box': 'outbox',
                'type': ['Create'],
                'meta': {'undo': False, 'deleted': False},
                'remote_id': str(data.attributedTo) + '/outbox/' + str(data.uuid),
        })
        self.database.activities.insert_one(message.to_dict())
        return self.render_json(
            setIdToString(message.to_dict())
        )
    
@app.route("/user/<nickname>/inbox", ["GET"])
def route_user_inbox(self, nickname):
    obj = findUser(self, nickname)
    if obj:
        activities = []
        for a in self.database.activities.find({'box': 'outbox', 'activity.object.to': obj['id']}):
            activities.append(setIdToString(a))
        return self.render_json(
            orderedCollection(activities)
        )
    else:
        return self.error(404)

@app.route("/user/<nickname>/outbox/<page>", ["GET"])
def route_outbox_page(self, nickname, page):
    obj = findUser(self, nickname)
    if obj:
        return self.render_json(
            obj.to_dict()
        )
    else:
        return self.error(404)