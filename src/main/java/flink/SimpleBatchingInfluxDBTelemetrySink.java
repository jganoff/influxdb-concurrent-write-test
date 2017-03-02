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

package flink;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import com.google.common.base.Strings;
import okhttp3.HttpUrl;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.checkpoint.Checkpointed;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;
import org.influxdb.InfluxDB;
import org.influxdb.InfluxDBFactory;
import org.influxdb.dto.BatchPoints;
import org.influxdb.dto.Point;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Serializable;
import java.util.concurrent.TimeoutException;

/**
 * Flink Sink to write data into an InfluxDB database. This sink guarantees at-least-once delivery of all records if checkpointing is enabled. Otherwise the
 * sink doesn't provide any reliability guarantees.
 */
public class SimpleBatchingInfluxDBTelemetrySink extends RichSinkFunction<Point> implements Checkpointed<Serializable> {
    private static final Logger log = LoggerFactory.getLogger(SimpleBatchingInfluxDBTelemetrySink.class);

    // InfluxDB has an internal 5 second timeout for requests. They recommend limiting batches to 5000. If a single batch takes longer than 5 seconds the
    // records /may/ be written but there's no guarantee.
    private static final int BUFFER_SIZE = 5000;

    // Require acks from a quorum before considering the write successful.
    private final InfluxDB.ConsistencyLevel WRITE_CONSISTENCY = InfluxDB.ConsistencyLevel.QUORUM;

    private final String influxDBUrl;
    private final String databaseName;
    private final String retentionPolicy;

    private final InfluxDB.LogLevel logLevel;

    private transient InfluxDB influxDB;

    public SimpleBatchingInfluxDBTelemetrySink(String influxDBUrl, String databaseName, String retentionPolicy, InfluxDB.LogLevel logLevel) {
        Preconditions.checkArgument(HttpUrl.parse(influxDBUrl) != null, "influxDBUrl (%s) is not a valid url", influxDBUrl);
        Preconditions.checkArgument(!Strings.isNullOrEmpty(databaseName), "databaseName (%s) must be non-empty", databaseName);
        Preconditions.checkArgument(!Strings.isNullOrEmpty(retentionPolicy), "retentionPolicy (%s) must be non-empty", retentionPolicy);
        Preconditions.checkNotNull(logLevel);

        this.influxDBUrl = influxDBUrl;
        this.databaseName = databaseName;
        this.retentionPolicy = retentionPolicy;
        this.logLevel = logLevel;
    }

    @VisibleForTesting
    void setInfluxDBForTesting(InfluxDB influxDB) {
        this.influxDB = influxDB;
    }

    @Override
    public void open(Configuration parameters) {
        if (influxDB == null) {
            influxDB = InfluxDBFactory.connect(influxDBUrl)
                    .enableGzip();
            influxDB.setLogLevel(logLevel);
        }
    }

    @Override
    public void close() throws TimeoutException, InterruptedException {
        simpleFlush();
        if (influxDB != null) {
            influxDB.close();
        }
    }

    private transient BatchPoints batch;

    @Override
    public void invoke(Point point) throws TimeoutException, InterruptedException {
        ensureBatchIsReady();

        batch.point(point);

        if (batch.getPoints().size() == BUFFER_SIZE) {
            simpleFlush();
        }
    }

    private void ensureBatchIsReady() {
        if (batch == null) {
            batch = BatchPoints.database(databaseName)
                    .retentionPolicy(retentionPolicy)
                    .consistency(WRITE_CONSISTENCY)
                    .build();
        }
    }

    private void simpleFlush() {
        if (batch != null) {
            influxDB.write(batch);
            batch = null;
        }
    }

    @Override
    public Serializable snapshotState(long checkpointId, long checkpointTimestamp) throws InterruptedException {
        simpleFlush();

        // Don't return any state - we only use the checkpointed interface to guarantee writes are flushed for all records received up to a checkpoint barrier.
        return null;
    }

    @Override
    public void restoreState(Serializable state) throws Exception {
        // Nothing to do here!
    }
}
