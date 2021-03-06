# -*- coding: utf-8 -*-
"""
@author: Thijs Werrij
"""

from neo4j import GraphDatabase, basic_auth
import re
#from time import time

from check_db import checkForUpdates

#%%

driver = GraphDatabase.driver("bolt://localhost", auth=basic_auth("neo4j", "hunter2"))

try:
    session
except NameError:
    session = driver.session()

#%%

def checkQuery (input_query, testing = False):
    
    # splitQuery identifies queries and modifies them
    modified_query = splitQuery(input_query)
    
    if not testing:
        sendQuery(modified_query, testing, originalQuery=input_query)
    else:
        print(modified_query)
    
def splitQuery (input_query):
    rr_list = []
    carry = ""
    query_is_split = False
    
    # These regular expressions are to recognize CREATE, SET or DELETE
    # statements and separate them from the rest of the query
    # Expressions can get very complex when there are many ways to define statements
    create_regex = re.compile("(CREATE \(.+\)(?:, \(.+\))*)")
    set_regex    = re.compile("(SET (?:(?:(?:\([\s\S]+?\)|\w+)(?:\.\w+)? \+?= (?:\{[\s\S]*?}|\S+)|\w+(?::\w+)+)(?:,\s)?)*)")
    delete_regex = re.compile("((?:DETACH )?DELETE \w+(?:, [^,\s]+)*)")
    
    if ("CREATE" in input_query):
        query_type = "CREATE"
        properties = "graph_status:'new'"
        type_regex = create_regex
    elif ("SET" in input_query):
        query_type = "SET"
        properties = "graph_status = 'updated'"
        type_regex = set_regex
    elif ("DETACH DELETE" in input_query):
        query_type = "DETACH"
        properties = "graph_status = 'detach'"
        type_regex = delete_regex
    elif ("DELETE" in input_query):
        query_type = "DELETE"
        properties = "graph_status = 'delete'"
        type_regex = delete_regex
    else:
        # Only CREATE, SET and (DETACH) DELETE have been implemented
        query_type = None
        # raise Exception('Unexpected query type')
    
    # Next line can be uncommented if you want to add time as a property
    #properties += ", time:'" + str(time()) + "'"
    
    if query_type is None or not (type_regex.search(input_query)):
        # If correct syntax not detected, simply send the query so neo4j can send
        # a response. This is so that all incorrect queries are handled by the
        # neo4j parser immediately, and can give a meaningful error log.
        print("No match, query has not been modified")
        return input_query
    
    # Query is split, either in one or three parts, depending whether the regex
    # captures the whole statement or just a part, respectively.
    split_query = type_regex.split(input_query)
    
    if (len(split_query) == 1):
        mid_query = split_query[0]
    elif (len(split_query) == 3):
        mid_query = split_query[1]
        query_is_split = True
        begin_query = split_query[0]
        end_query = split_query[2]
    else:
        raise Exception('Unexpected') # TODO: are there other cases that need implementation?
    
    if (query_type == "CREATE"):
        regex = "\(([^({]+)(?:({[^(]*)})?\)(?:-\[([^({]+)(?:({[^(]*)})?\]->)?"
        
        """The idea behind this regular expression is that it captures certain
        pieces of the CREATE statement so that it can insert new properties.
        
        Example:
        CREATE (a:Person {name:'Alice'})-[r1:friends]->(b:Person {name:'Bob'})
        The statement itself captures two parts, namely:
            (a:Person {name:'Alice'})-[r1:friends]->
            and
            (b:Person {name:'Bob'})
        so that it can work recursively.
        
        It then captures the name of the node and its labels ("a:Person"),
        its properties ("{name:'Alice'"), and, if provided, its outward edge
        ("r1:friends") and its properties (in this case none).
        
        Then the properties are added and the statement is reconstructed."""
        
        matches = matchQuery(input_query)

        for r in re.findall(regex, mid_query):
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
                
        return_query = "CREATE " + ', '.join(rr_list)
                
    elif (query_type == "SET"):
        # This regex captures all nodes mentioned after SET and adds them to rr_list
        regex = "(?:SET )?(?:(\([\s\S]+\)|\w+)(?:\.\w+)? \+?= (?:\{.*?\}|\S+))|(?:(\w+)(?::\w+)+)"
        
        for r in set(re.findall(regex, mid_query)):
            if r[0] != "":
                rr_list.append(r[0] + "." + properties)
            elif r[1] != "":
                rr_list.append(r[1] + "." + properties)
                
        return_query = mid_query + ', '.join([''] + rr_list)

    elif (query_type == "DELETE" or query_type == "DETACH"):
        # This regex captures all nodes mentioned after DELETE and adds them to rr_list
        regex = "(?:(?:DETACH )?DELETE )?(\w+)"
        
        for r in set(re.findall(regex, mid_query)):
            rr_list.append(r + "." + properties)
        
        # this SET is intentional, node needs to be preserved and deleted later
        return_query = "SET " + ', '.join(rr_list)
    
    if (query_is_split):
        return_query = begin_query + return_query + end_query
    
    return return_query
            
def matchQuery(input_query):
    match_query = re.search("(MATCH \(.*\)[ \n])", input_query)
    
    if (match_query):
        return re.findall("""\(([^\) :{]*)[^\)]*\)""", match_query[0])
    return []

def sendQuery(input_query, testing, query_type=None, originalQuery=None, show_output=False):
    results = session.run(input_query)
    
    if (query_type != None):
        print(str(query_type) + " query sent!")
    else:
        print("Query sent!")
    
    if (show_output):
        print(results)
    print()
    
    checkForUpdates(session, originalQuery)
    
    if not testing:
        finalizeGraph()
        
    #session.close()
        
    
def sendSimpleQuery(input_query):
    if input_query:
        return session.run(input_query)
    return None
    
def finalizeGraph():
    
    updateNodesQuery = """MATCH (n)
    WHERE n.graph_status = 'new' OR n.graph_status = 'updated'
    REMOVE n.graph_status, n.time
    RETURN n"""
    
    updateEdgesQuery = """MATCH (n)-[e]->(m)
    WHERE e.graph_status = 'new' OR e.graph_status = 'updated'
    REMOVE e.graph_status, e.time
    RETURN e"""
    
    session.run(updateNodesQuery)
    session.run(updateEdgesQuery)
    
    detachNodesQuery = """MATCH (n)
    WHERE n.graph_status = 'detach'
    DETACH DELETE n"""
    
    detachEdgesQuery = """MATCH (n)-[e]->(m)
    WHERE e.graph_status = 'detach'
    DETACH DELETE e"""
    
    session.run(detachEdgesQuery)
    session.run(detachNodesQuery)

def resetGraph():
    # WARNING, RESETS GRAPH
    session.run("""MATCH (n) DETACH DELETE n""")
    
def sendQueries(qList, testing=False):
    # for sending multiple queries at once
    
    if isinstance(qList,list):
        for q in qList:
            checkQuery(q, testing = testing)