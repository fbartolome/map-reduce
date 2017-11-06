mvn clean && mvn package
cd server/target
tar -zvxf hazelcast-server-1.0-SNAPSHOT-bin.tar.gz
cd hazelcast-server-1.0-SNAPSHOT/
chmod +x run-server.sh
cd ../../../client/target
tar -zvxf hazelcast-client-1.0-SNAPSHOT-bin.tar.gz
cd hazelcast-client-1.0-SNAPSHOT/
chmod +x run-client.sh
cd ../../../