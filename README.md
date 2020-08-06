# ActivityGraph

This application runs using Neo4j and MongoDB (other database options are available in the code).

Some example queries have been provided (```testcases.py```). Add further activities and actors using queries on the graph or requests to the application.

Implemented queries: `CREATE`, `SET`, `DELETE`, `DETACH DELETE`

Implemented activities: `Create`, `Update`, `Delete`

### Graph

Queries are handled by `cypher_read.py`. Using `checkQuery`, you can pass Cypher queries, which will then be modified and sent to Neo4j.

Updates to the ActivityPub instance are then handled by `check_db.py`: `checkForUpdates` is called, and all marked nodes/edges are updated in the instance.

### ActivityPub

Run the application using:

```python app.py```

To interact with the application, send GET/POST requests to:

```localhost:5000/\[user\]/outbox```  
```localhost:5000/\[user\]/inbox```

Activities that are of type `Create`, `Update` or `Delete` are processed accordingly; all other activities are wrapped in a Create activity.

Afterwards, `update_db.py` is called, which processes the newly added activities and modifies both the instance and the graph representation.