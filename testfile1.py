# -*- coding: utf-8 -*-
"""
@author: Thijs Werrij
"""

from neo4j import GraphDatabase, basic_auth
import re
from pypeg2 import parse
from time import time

driver = GraphDatabase.driver("bolt://localhost", auth=basic_auth("neo4j", "hunter2"))
session = driver.session()

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

#%% Close session
session.close()

#%%

example_query = """CREATE (a:Person {name:'Test1'}), (b:Object {name:'Test2'}), (c:Object)"""
example_query2 = """CREATE (a)-[r:RELTYPE { name: a.name + '<->' + b.name }]->(b), c"""
example_query3 = """MATCH (a:Person),(b:Person)
WHERE a.name = 'A' AND b.name = 'B'
CREATE (a)-[r:RELTYPE { name: a.name + '<->' + b.name }]->(b)
RETURN type(r), r.name"""

def checkQuery (input_query):
    
    if (re.search("CREATE", input_query)):
        createQuery(input_query)
    # TODO: Delete, update
        
        
def createQuery (input_query):
    #regex = "(\([^({]+)(?:({[^(]*)})?\)"
    regex = "(\([^({]+)(?:({[^(]*)})?\)(?:-(\[[^({]+)(?:({[^(]*)})?\]->)?"
    rr_list = []
    carry = ""
    
    matches = matchQuery(input_query)
    
    split_query = re.compile("(CREATE \(.*\)[ \n])").split(input_query)
    if (len(split_query) == 1):
        create_query = split_query[0]
    elif (len(split_query) == 3):
        create_query = split_query[1]
    else:
        raise Exception('Unexpected')
    
    for r in re.findall(regex, create_query):
        rr = carry
        carry = ""
        if r[0][1:].split(':')[0] in matches: # check if there is a match
            rr += r[0] + r[1]
            rr += ")" if r[1] == "" else "})"
        else:
            if r[1] == "": # check for earlier defined properties
                rr += r[0] + " {status:'new'})"
            else:
                rr += r[0] + r[1] + ", status:'new'})"
        if r[2] != "": # check if there is an edge
            rr += "-"
            if r[3] == "": # check for earlier defined properties
                rr += r[2] + " {status:'new'}]"
            else:
                rr += r[2] + r[3] + ", status:'new'}]"
            rr += "->"
            carry = rr
        else:
            rr_list.append(rr)
        
    regex_query = "CREATE " + ', '.join(rr_list)
    
    if (len(split_query) == 1):
        sendQuery(regex_query, 'Create')
    else:
        sendQuery(split_query[0] + regex_query + "\n" + split_query[2], 'Create')
            
def matchQuery(input_query):
    match_query = re.search("(MATCH \(.*\)[ \n])", input_query)
    
    if (match_query):
        return re.findall("""\(([^\) :{]*)[^\)]*\)""", match_query[0])
    return []

def sendQuery(input_query, query_type):
    
    print(str(query_type) + " query sent!")
    print(input_query)
    print()
    
checkQuery(example_query)
checkQuery(example_query2)
checkQuery(example_query3)