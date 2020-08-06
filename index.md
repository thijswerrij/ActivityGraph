ActivityGraph enables users to query ActivityPub in the way they are used to with graphs.

## How does it work?

We keep two separate datasets: one ActivityPub instance and one graph representation of that instance. These two can receive requests or queries, respectively, and because they need to represent the same data, they also update each other.

![ActivityGraph overview](overview.png "An overview of how ActivityGraph works")

## Supported queries

`CREATE`, `SET`, `DELETE` and `DELETE DETACH` are implemented. Queries that contain these keywords are modified, so that affected nodes and edges can also be synced to the ActivityPub instance. You can of course also use the standard query terms like `MATCH` and `RETURN`, these are unaffected during modification.

## Supported activities

`Create`, `Update`, `Delete` are implemented. When POST-ing objects to inbox of a user, the application checks whether this object is an activity that contains one of these types AND a nested object. If so, these objects are processed accordingly. Otherwise, the object is processed as a 'normal' object and gets added to a newly created `Create` activity.
