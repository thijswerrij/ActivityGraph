# -*- coding: utf-8 -*-
"""
@author: Thijs Werrij
"""

from check_db import resetDB
from cypher_read import checkQuery, resetGraph

#%%

resetGraph()
resetDB()

create_abc = """CREATE (a:Person {name:'Alice'})-[r1:friends]->(b:Person {name:'Bob'})-[r2:friends]->(c:Person {name:'Eve'})"""

checkQuery(create_abc, True)

update_bob = """MATCH (n)
WHERE n.name = 'Bob'
SET n.age = '25'
RETURN n"""

checkQuery(update_bob, True)

delete_alice = """MATCH (n)-[e]->(m)
WHERE n.name = 'Alice'
DELETE e, n
RETURN n"""

checkQuery(delete_alice, True)