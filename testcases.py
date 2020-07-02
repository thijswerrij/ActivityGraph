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

checkQuery(create_abc, False)

update_bob = """MATCH (n)
WHERE n.name = 'Bob'
SET n.age = '25'
RETURN n"""

checkQuery(update_bob, False)