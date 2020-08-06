# -*- coding: utf-8 -*-
"""
@author: Thijs Werrij
"""

from activitypub.manager.base import app
from flask import request

from activitypub.manager import FlaskManager as Manager
from activitypub.database import MongoDatabase

from update_db import updateDB

database = MongoDatabase("mongodb://localhost:27017", "dsblank_localhost")
manager = Manager(database=database)

federated = True # is this ActivityPub instance federated or not

#%%

# Returns list as ordered collection, as specified in ActivityPub
def orderedCollection(items):
    if not isinstance(items, list):
        items = [items]
    contents = {
        "@context": "https://www.w3.org/ns/activitystreams",
        "type": "OrderedCollection",
        "totalItems": len(items),
        "orderedItems": items,}
    return contents

# Recursively sets Mongo id's to strings
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

# Find a user by their nickname
def findUser(self, nickname):
    obj = self.database.actors.find_one({'id': request.url_root + nickname})
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
    user = findUser(self, nickname)
    if user:
        return sendToOutbox(self, user)
    else:
        return self.error(404)
    
@app.route("/outbox", ["GET", "POST"])
def route_outbox(self):
    return sendToOutbox(self)

def sendToOutbox(self, user=None):
    if request.method == "GET":
        activities = []
        if not user is None:
            entries = self.database.activities.find({'box': 'outbox', 'activity.object.attributedTo': user['id']})
        else:
            entries = self.database.activities.find({'box': 'outbox'})
        
        for a in entries:
            activities.append(setIdToString(a))
        return self.render_json(
            orderedCollection(activities)
        )
    elif request.method == "POST":
        data = request.get_json(force=True)#.to_dict()
        
        message = createActivity(data, box='outbox')
        
        self.database.activities.insert_one(message.to_dict())
        return self.render_json(
            setIdToString(message.to_dict())
        )
    
@app.route("/user/<nickname>/inbox", ["GET", "POST"])
def route_user_inbox(self, nickname):
    user = findUser(self, nickname)
    if user:
        if request.method == "GET":
            activities = []
            for a in self.database.activities.find({'box': 'inbox', 'activity.object.to': user['id']}):
                activities.append(setIdToString(a))
            return self.render_json(
                orderedCollection(activities)
            )
        elif request.method == "POST":
            if federated:
                data = request.get_json(force=True)#.to_dict()
                
                for actor in getRecipients(self,data):
                    message = createActivity(data, box='inbox', id_preset=actor['id'])
                    
                    self.database.activities.insert_one(message.to_dict())
                    
                    # only call this if you want to automatically update
                    updateDB()
                return self.render_json(
                    setIdToString(message.to_dict())
                )
            else:
                return self.error(405)
    else:
        return self.error(404)

# Searches all possible recipients, looks them up and returns them in a list
def getRecipients(self, obj):
    
    recipients = set()
    
    for rec in ["to", "bto", "cc", "bcc", "audience"]:
        if rec in obj:
            if isinstance(obj[rec], list):
                recipients.update([r for r in obj[rec] if isinstance(r, str)])
            elif isinstance(obj[rec], str):
                recipients.add(obj[rec])
    
    actors = []
    for userId in recipients:
        actor = self.database.actors.find_one({"id": userId})
        if actor:
            actors.append(actor)
    
    return actors

def createActivity(data, actor=None, box='outbox', id_preset=''):
    if actor is None:
        attributed = '$DOMAIN'
    else:
        attributed = actor['id']
        
    if id_preset == '':
        id_preset = attributed
    
    data.update(**{'attributedTo': attributed,
                'published': '$NOW',
                'uuid': "$UUID",
        })
        
    data.update(**{'id': id_preset + '/' + box + '/$UUID'})
    
    if 'type' in data and data['type'] in ['Create', 'Update', 'Delete']:
        msg_type = data['type']
    else:
        msg_type = 'Create'
        
        if not 'type' in data:
            data.update(**{'type': 'Note'})
        
        note = manager.Note(**data)
    
        data = manager.Create(**{
                'object': note.to_dict(),
                'published': '$NOW',
                'id': '$object.id'#id_preset + '/' + box + '/' + str(data.uuid),
        }).to_dict()
    
    msg_data = {
            'activity': data,
            'box': box,
            'type': [msg_type],
            'meta': {'undo': False, 'deleted': False},
            'remote_id': '$activity.id',#id_preset + '/' + box + '/' + str(note.uuid),
            'db_status': 'new',
            'time_posted': '$NOW',
    }
    
    message = manager.Create(**msg_data
    )
    
    return message

@app.route("/user/<nickname>/outbox/<page>", ["GET"])
def route_outbox_page(self, nickname, page):
    obj = findUser(self, nickname)
    if obj:
        return self.render_json(
            obj.to_dict()
        )
    else:
        return self.error(404)
    
@app.route("/", ["GET"])
def route_home(self):
    return self.render_template(
        "index.html"
        )
    
if __name__ == "__main__":
    manager.run()