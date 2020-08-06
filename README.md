# ActivityGraph

To run the application, simply use:

```python app.py```

This application runs using Neo4j and MongoDB (other database options are available in the code).

Some example queries have been provided (```testcases.py```). Add further activities and actors using queries on the graph or requests to the application.

To interact with the application, GET or POST, visit:

```localhost:5000/\[user\]/outbox```  
```localhost:5000/\[user\]/inbox```

Implemented queries: `CREATE`, `SET`, `DELETE`, `DETACH DELETE`

Implemented activities: `Create`, `Update`, `Delete`
