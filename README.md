This is a test application for concurrently writing batches of points to InfluxDB. It sometimes fails against InfluxDB 1.2.0 where not all points are recorded.

# To run:
Replace the InfluxDB URL in this command with one that works for your environment:

    mvn clean compile exec:java -Dexec.arguments="http://influxdb:8086"

