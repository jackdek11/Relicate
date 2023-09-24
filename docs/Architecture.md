# Repl.

This service is originally designed to subscrine to master databases in CQRS patterns, and relay there
replication publications via websocket connection. The relayed messages will go through a preconfigured 
"representation" transform to json.

## Proof of concept
The POC for this service is against a Postgres database

> Ref: [Postgres replication documentation](https://www.postgresql.org/docs/current/runtime-config-replication.html)

### Streaming replication
For streaming replication, servers will be either a primary or a 
standby server. Primaries can send data, while standbys are always 
receivers of replicated data. Standby servers can also be senders, as 
well as receivers. Parameters are mainly for sending and standby 
servers.

### Logical replication
For logical replication, publishers (servers that do CREATE 
PUBLICATION) replicate data to subscribers (servers that do CREATE 
SUBSCRIPTION). Servers can also be publishers and subscribers at the 
same time.

