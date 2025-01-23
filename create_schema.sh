cqlsh cassandra 9042 -f /etc/atomix/cassandra/keyspace.cql
cqlsh cassandra 9042 -k atomix -f /etc/atomix/cassandra/schema.cql
tail -f /dev/null
