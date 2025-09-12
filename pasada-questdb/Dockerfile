FROM questdb/questdb:latest

# Configure QuestDB to listen on all interfaces
ENV QDB_HTTP_BIND_TO=0.0.0.0:9000
ENV QDB_PG_BIND_TO=0.0.0.0:8812
ENV QDB_INFLUXDB_BIND_TO=0.0.0.0:9009

# Create a startup script to handle system limits
RUN echo '#!/bin/bash\n\
# Try to set vm.max_map_count if possible (will fail silently if not privileged)\n\
echo 1048576 > /proc/sys/vm/max_map_count 2>/dev/null || echo "Note: Could not set vm.max_map_count (not privileged)"\n\
# Set file descriptor limits\n\
ulimit -n 1048576 2>/dev/null || echo "Note: Could not set ulimit (not privileged)"\n\
# Start QuestDB\n\
exec /docker-entrypoint.sh "$@"' > /start-questdb.sh \
&& chmod +x /start-questdb.sh

EXPOSE 9000 9009 8812

ENTRYPOINT ["/start-questdb.sh"]

