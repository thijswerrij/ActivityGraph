# -*- coding: utf-8 -*-
"""
@author: Thijs Werrij
"""

from neo4j import GraphDatabase, basic_auth
import re
#from pypeg2 import parse
from time import time

from check_db import checkForUpdates

#%%

driver = GraphDatabase.driver("bolt://localhost", auth=basic_auth("neo4j", "hunter2"))
session = driver.session()

#%%

example_query = """CREATE (a:Person {name:'Test1'}), (b:Object {name:'Test2'}), (c:Object)"""
example_query2 = """CREATE (a)-[r:RELTYPE { name: a.name + '<->' + b.name }]->(b), c"""
example_query3 = """MATCH (a:Person),(b:Person)
WHERE a.name = 'A' AND b.name = 'B'
CREATE (a)-[r:RELTYPE { name: a.name + '<->' + b.name }]->(b)
RETURN type(r), r.name"""

#%%

def checkQuery (input_query):
    
    if (re.search("CREATE", input_query)):
        input_query = splitQuery(input_query, "CREATE")
    elif (re.search("SET", input_query)):
        input_query = splitQuery(input_query, "SET")
    elif (re.search("DETACH DELETE", input_query)):
        input_query = splitQuery(input_query, "DETACH")
    elif (re.search("DELETE", input_query)):
        input_query = splitQuery(input_query, "DELETE")
    
    print(input_query)
    #sendQuery(input_query)
    

def splitQuery (input_query, query_type):
    regex = "\(([^({]+)(?:({[^(]*)})?\)(?:-\[([^({]+)(?:({[^(]*)})?\]->)?" # TODO: Explanation
    rr_list = []
    carry = ""
    query_is_split = False
    
    matches = matchQuery(input_query)
    print('matches', matches)
    
    #create_regex = re.compile("(" + query_type + " \(.*\))(?:\s|$)")
    create_regex = re.compile("(" + query_type + " \(?.*\)?)(?:\s|$)")
    
    if not (create_regex.search(input_query)):
        # if correct create syntax not detected, simply send the query so neo4j can send a response
        print("no match")
        return input_query
    
    split_query = create_regex.split(input_query)
    print('split_query', split_query)
    if (len(split_query) == 1):
        create_query = split_query[0]
    elif (len(split_query) == 3):
        create_query = split_query[1]
        query_is_split = True
        begin_query = split_query[0]
        end_query = split_query[2]
    else:
        raise Exception('Unexpected') # TODO: are there other cases that need implementation?
    
    if (query_type == "CREATE"):
        properties = "graph_status:'new'"
    elif (query_type == "SET"):
        properties = "graph_status:'updated'"
    elif (query_type == "DELETE"):
        properties = "graph_status:'delete'"
    elif (query_type == "DETACH"):
        properties = "graph_status:'detach'"
    else:
        raise Exception('Unexpected query type')
        
    #properties += ", time:'" + str(time()) + "'"
    
    if (query_type == "CREATE"):
        for r in re.findall(regex, create_query):
            rr = carry + "("    # if the previous node had an outward pointing edge, it is carried over
            carry = ""          # reset when carry is completed
            if r[0].split(':')[0] in matches: # check if there is a match
                rr += r[0] + r[1]
                rr += ")" if r[1] == "" else "})"
            else:
                if r[1] == "": # check for earlier defined properties
                    rr += r[0] + " {" + properties + "})"
                else:
                    rr += r[0] + r[1] + ", " + properties + "})"
            if r[2] != "": # check if there is an edge
                rr += "-["
                if r[3] == "": # check for earlier defined properties
                    rr += r[2] + " {" + properties + "}]"
                else:
                    rr += r[2] + r[3] + ", " + properties + "}]"
                rr += "->"
                carry = rr
            else:
                rr_list.append(rr)
    else:
        for m in matches:
            rr_list.append(m + "." + properties)
            
    print ("rr_list", rr_list)
    
    if (query_type == "CREATE"):
        return_query = "CREATE " + ', '.join(rr_list)
    elif (query_type == "SET"):
        return_query = create_query + ', '.join([''] + rr_list)
    elif (query_type == "DELETE" or query_type == "DETACH"):
        return_query = "SET " + ', '.join(rr_list) # this SET is intentional, node needs to be preserved and deleted later
    
    if (query_is_split):
        #return_query = re.sub('\s+', ' ', begin_query) + "\n" + return_query + "\n" + re.sub('\s+', ' ', end_query)
        return_query = begin_query + return_query + "\n" + end_query
    
    return return_query
            
def matchQuery(input_query):
    match_query = re.search("(MATCH \(.*\)[ \n])", input_query)
    
    if (match_query):
        return re.findall("""\(([^\) :{]*)[^\)]*\)""", match_query[0])
    return []

def sendQuery(input_query, query_type=""):
    print(input_query)
    results = session.run(input_query)
    if (query_type != ""):
        print(str(query_type) + " query sent!")
    else:
        print("Query sent!")
    print(results)
    print()

#%%
checkQuery(example_query3)

print()
print()

update_query = """MATCH (n)
WHERE id(n)=203
SET n.name = 'Test123'
RETURN n"""

checkQuery(update_query)

delete_query = """MATCH (n)
WHERE id(n)=203
DETACH DELETE n"""

checkQuery(delete_query)