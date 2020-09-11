# -*- coding: utf-8 -*-
"""
@author: Thijs Werrij
"""

from check_db import resetDB
from cypher_read import checkQuery, resetGraph, sendQueries

#%%

resetGraph()
resetDB()

#%% Creating actors

create_abc = """CREATE (a:Person {name:'Alice'}), (b:Person {name:'Bob'}), (c:Person {name:'Eve'})"""

make_friends = """MATCH (a), (b)
WHERE a.name = "Alice" AND NOT b.name = "Alice"
CREATE (a)-[r:friends]->(b)"""

checkQuery(create_abc)
checkQuery(make_friends)

#%% Creating an event (birthday party)

create_birthday = """MATCH (b)
WHERE b.name = "Bob"
CREATE (e:Event {name:"Bob's birthday party", date:"01-01-2021", location:"Nijmegen"})-[r:attributedTo]->(b)
"""

checkQuery(create_birthday)

create_attending = """MATCH (a), (e)
WHERE e.name = "Bob's birthday party" AND (a.name = "Alice" OR a.name = "Bob" OR a.name = "Eve")
CREATE (e)-[r:attending]->(a)"""

checkQuery(create_attending)

#%% Update example

update_bob = """MATCH (n) WHERE n.name = 'Bob' SET n.age = '25' RETURN n"""
checkQuery(update_bob)

#%% Posting a message example

create_post = """MATCH (e)
WHERE e.name = "Bob's birthday party"
CREATE (p:Post {message: "I'll be there!"})-[r:posted]->(e)"""

add_author = """MATCH (p:Post)-[]->(e), (a)
WHERE e.name = "Bob's birthday party" AND a.name = "Alice"
CREATE (p)-[r:author]->(a)"""

sendQueries([create_post, add_author])

#%% Delete example

delete_eve = """MATCH (m)-[e]->(n)
WHERE n.name = 'Eve'
DELETE e, n"""

delete_eve = """MATCH (n) WHERE n.name = 'Eve' DETACH DELETE n"""

checkQuery(delete_eve, testing=False)
