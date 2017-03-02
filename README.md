This is a test application for concurrently writing batches of points to
InfluxDB. It sometimes fails against InfluxDB 1.2.0 where not all points are
recorded.

# How this test works
This test will create 1 million points and send them to InfluxDB as quickly as
possible using 5 writers. It relies on Flink to handle the coordination between
writers and flushing data. In short the writing works like this:

1. 1 million points are generated.
1. Points are split up in to 5 groups of 200,000.
1. Writers (the SimpleBatchingInfluxDBTelemetrySink) will each create their own
InfluxDB client to use when sending points.
1. Points will be written to InfluxDB by each writer every 5000 points or every
checkpoint interval.
1. The Flink environment is configured to checkpoint every 100ms so we are
guaranteed a write every 100ms even if the batch size hasn't reached 5000.
1. Once the Flink pipeline completes we query InfluxDB to see if all the points
are still in the database. We query twice with a slight delay because we've
observed the first query will sometimes return the correct count but a
subsequent one will return an incorrect count.

# To run:
Replace the InfluxDB URL in this command with one that works for your
environment:

    mvn clean compile exec:java -Dexec.arguments="http://influxdb:8086"

