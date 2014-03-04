# Sky

## Overview

Sky is a database for storing behavioral data. It groups events by the actor who performed the event and stores the events in chronological order for fast querying. It's similar to a time series database except that event data can optionally persist over time.


## Getting Started

Sky uses LLVM for query compilation so unless you have an hour to spare and a lot of hair to pull out then it is highly recommended that you use Docker. After you have [Docker installed](http://docs.docker.io/en/latest/installation/), you can run Sky by using the following command:

```sh
# Start the Sky server.
$ docker run -t -i -p 8585:8585 skydb/sky-llvm

# -t -i:          Run as an interactive shell so you can CTRL-C to stop.
# -p 8585:8585:   Map port 8585 to the host machine.
# skydb/sky-llvm: The name of the trusted build.
```

You should see the following in the Docker shell:

```
Sky v0.4.0 (llvm/179a26d)
Listening on http://localhost:8585
```

From another shell, verify that you can hit the server:

```sh
$ curl http://localhost:8585
{"sky":"welcome","version":"v0.4.0 (llvm/179a26d)"}
```

### Setting ulimit

If you're using a large number of tables then you'll need to change your docker
upstart to support the higher ulimit:

1. `/etc/init/docker.conf`: Add `limit nofile 65536 65536` after start/stop.

2. `/etc/init/docker.conf`: Add `ulimit -n 65536` as the first line in your `script` block.

3. Run `service stop docker`

4. Run `service start docker`


## API

### Overview

Sky uses a RESTful JSON over HTTP API. Below you can find Table, Property, Event and Query endpoints.
The examples below use cURL but there are also client libraries available for different languages.

### Table API

```sh
# List all tables in the database.
$ curl -X GET http://localhost:8585/tables
```

```sh
# Retrieve a single table named 'users' from the database.
$ curl -X GET http://localhost:8585/tables/users
```

```sh
# Creates an empty table named 'users'.
$ curl -X POST http://localhost:8585/tables -d '{"name":"users"}'
```

```sh
# Deletes the table named 'users'.
$ curl -X DELETE http://localhost:8585/tables/users
```

### Property API

```sh
# List all properties on the 'users' table.
$ curl http://localhost:8585/tables/users/properties
```

```sh
# Add the 'username' property to the 'users' table.
$ curl -X POST http://localhost:8585/tables/users/properties -d '{"name":"username","transient":false,"dataType":"string"}'
```

```sh
# Retrieve the 'username' property from the 'users' table.
$ curl http://localhost:8585/tables/users/properties/username
```

```sh
# Change the name of the 'username' property on the 'users' table to be 'username2'.
$ curl -X PATCH http://localhost:8585/tables/users/properties/username -d '{"name":"username2"}'
```

```sh
# Change the name of the 'username' property on the 'users' table to be 'username2'.
$ curl -X PATCH http://localhost:8585/tables/users/properties/username -d '{"name":"username2"}'
```

```sh
# Delete the 'username2' property on the 'users' table.
$ curl -X DELETE http://localhost:8585/tables/users/properties/username2
```

### Event API

```sh
# List all events for the 'john' object on the 'users' table.
$ curl http://localhost:8585/tables/users/objects/john/events
```

```sh
# Delete all events for the 'john' object on the 'users' table.
$ curl -X DELETE http://localhost:8585/tables/users/objects/john/events
```

```sh
# Retrieve the event for the 'john' object on the 'users' table that
# occurred at midnight on January 20st, 2012 UTC.
$ curl http://localhost:8585/tables/users/objects/john/events/2012-01-20T00:00:00Z
```

```sh
# Replace the event for the 'john' object in the 'users' table that
# occurred at midnight on January 20st, 2012 UTC.
$ curl -X PUT http://localhost:8585/tables/users/objects/john/events/2012-01-20T00:00:00Z -d '{"data":{"username":"johnny1000"}}'
```

```sh
# Merge the event for the 'john' object in the 'users' table that
# occurred at midnight on January 20st, 2012 UTC.
$ curl -X PATCH http://localhost:8585/tables/users/objects/john/events/2012-01-20T00:00:00Z -d '{"data":{"age":12}}'
```

```sh
# Delete the event for the 'john' object in the 'users' table that
# occurred at midnight on January 20st, 2012 UTC.
$ curl -X DELETE http://localhost:8585/tables/users/objects/john/events/2012-01-20T00:00:00Z
```


### Query API

```sh
# Count the total number of events.
$ curl -X POST http://localhost:8585/tables/users/query -d '{
  "steps": [
    {"type":"selection","fields":[{"name":"count","expression":"count()"}]}
  ]
}'
```

```sh
# Retrieve stats on the 'users' table.
$ curl -X GET http://localhost:8585/tables/users/stats
```

### Miscellaneous API

```sh
# Ping the server to see if it's functional.
$ curl http://localhost:8585/ping
```

