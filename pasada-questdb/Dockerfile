FROM questdb/questdb:latest

# Configure QuestDB to listen on all interfaces
ENV QDB_HTTP_BIND_TO=0.0.0.0:9000
ENV QDB_PG_BIND_TO=0.0.0.0:8812
ENV QDB_INFLUXDB_BIND_TO=0.0.0.0:9009

EXPOSE 9000 9009 8812

