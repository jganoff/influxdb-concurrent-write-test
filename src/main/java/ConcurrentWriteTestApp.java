/*
 * Copyright 2017 Jordan Ganoff
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

import com.google.common.base.Stopwatch;
import flink.SimpleBatchingInfluxDBTelemetrySink;
import okhttp3.HttpUrl;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.log4j.LogManager;
import org.influxdb.InfluxDB;
import org.influxdb.InfluxDBFactory;
import org.influxdb.dto.Point;
import org.influxdb.dto.Query;
import org.influxdb.dto.QueryResult;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

public class ConcurrentWriteTestApp implements AutoCloseable {
    private static final Logger log = LoggerFactory.getLogger(ConcurrentWriteTestApp.class);

    private final String influxDBUrl;
    private final InfluxDB.LogLevel logLevel;
    private final String database;
    private final String retentionPolicy = "autogen";
    private final String measurement;
    private final int pointsToSend;

    private InfluxDB influxDB;

    public ConcurrentWriteTestApp(String influxDBUrl, InfluxDB.LogLevel logLevel, String database, String measurement, int pointsToSend) {
        this.influxDBUrl = influxDBUrl;
        this.logLevel = logLevel;
        this.database = database;
        this.measurement = measurement;
        this.pointsToSend = pointsToSend;
    }

    public void init() {
        log.info("Connecting to InfluxDB at {}", influxDBUrl);
        influxDB = InfluxDBFactory.connect(influxDBUrl).enableGzip();
        influxDB.setLogLevel(logLevel);
    }

    public void close() {
        if (influxDB != null) {
            influxDB.close();
        }
    }

    public void runOneIteration() throws Exception {
        log.info("Running one iteration: database={},retention_policy={},log_level={}", database, retentionPolicy, logLevel);
        StreamExecutionEnvironment env = StreamExecutionEnvironment.createLocalEnvironment();
        env.setParallelism(5);
        env.enableCheckpointing(100);

        influxDB.deleteDatabase(database);
        influxDB.createDatabase(database);

        SimpleBatchingInfluxDBTelemetrySink sink = new SimpleBatchingInfluxDBTelemetrySink(influxDBUrl, database, retentionPolicy, logLevel);

        log.info("Generating {} points", pointsToSend);
        List<Point> points = IntStream.range(0, pointsToSend).mapToObj(this::createPoint).collect(Collectors.toList());

        log.info("Starting up Flink environment and sending points");
        env.fromCollection(points)
                .addSink(sink);
        Stopwatch stopwatch = Stopwatch.createStarted();
        env.execute("send-to-influx");
        stopwatch.stop();

        log.info("Generated and wrote {} points in {} using Flink", pointsToSend, Duration.ofNanos(stopwatch.elapsed(TimeUnit.NANOSECONDS)));

        assertPointsWerePersisted(pointsToSend);
        Thread.sleep(2000);
        assertPointsWerePersisted(pointsToSend);
    }

    private Point createPoint(long timestampInMillis) {
        return Point.measurement(measurement)
                .time(timestampInMillis, TimeUnit.MILLISECONDS)
                .tag("anomalous", "false")
                .tag("field", "field1")
                .addField("anomalies", 0L)
                .addField("baseline_max", 1.05)
                .addField("baseline_min", 0.95)
                .addField("score", 0.0)
                .addField("value", 1.0)
                .build();
    }

    private void assertPointsWerePersisted(long totalPoints) {
        log.info("Checking how many points InfluxDB recorded...");

        Query query = new Query(String.format("SELECT count(\"value\") from \"%s\"", measurement), database, true);
        QueryResult queryResult = influxDB.query(query, TimeUnit.NANOSECONDS);
        long pointsPersisted = ((Double) queryResult.getResults().get(0).getSeries().get(0).getValues().get(0).get(1)).longValue();

        log.info("InfluxDB recorded {} points", pointsPersisted);

        if (totalPoints != pointsPersisted) {
            throw new AssertionError(String.format("We sent %d points but InfluxDB only persisted %d!", totalPoints, pointsPersisted));
        }
    }

    public static void main(String[] args) throws Exception {
        if (args.length != 1) {
            System.err.println("Must provide InfluxDB URL as the only command line argument");
            System.exit(1);
        }

        String influxDBUrl = args[0];

        if (HttpUrl.parse(influxDBUrl) == null) {
            throw new IllegalArgumentException("Invalid InfluxDB URL: " + influxDBUrl);
        }

        String database = "test";
        String measurement = "test_measurement";
        int pointsToSend = 1_000_000;

        try (ConcurrentWriteTestApp app = new ConcurrentWriteTestApp(influxDBUrl, InfluxDB.LogLevel.NONE, database, measurement, pointsToSend)) {
            app.init();
            app.runOneIteration();
            log.info("All looks good!");
        } finally {
            // Flush all buffered logs
            LogManager.shutdown();
        }
    }
}
