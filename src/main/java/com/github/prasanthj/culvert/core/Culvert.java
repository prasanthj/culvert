/*
 * Copyright 2018 Prasanth Jayachandran
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
package com.github.prasanthj.culvert.core;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.metastore.conf.MetastoreConf;
import org.apache.hive.streaming.HiveStreamingConnection;
import org.apache.hive.streaming.StrictDelimitedInputWriter;

/**
 *
 */
public class Culvert {
  private String name;
  private List<Stream> streams;
  private long streamDelay;
  private Column[] columnsOverride;
  private ExecutorService executorService;

  private Culvert(CulvertBuilder builder) {
    this.name = builder.name;
    this.streams = builder.streams;
    this.streamDelay = builder.streamLaunchDelayMs;
    this.columnsOverride = builder.columns;
    if (columnsOverride != null) {
      for (Stream stream : streams) {
        stream.setColumns(builder.columns);
      }
    }
    if (builder.dp) {
      for (Stream stream : streams) {
        stream.setDynamicPartition();
      }
    }
    this.executorService = Executors.newFixedThreadPool(streams.size());
  }

  private static class CulvertBuilder {
    private String name = "culvert";
    private List<Stream> streams = new ArrayList<Stream>();
    private Column[] columns;
    private long streamLaunchDelayMs = 1000;
    private boolean dp;

    public CulvertBuilder withName(String name) {
      this.name = name;
      return this;
    }

    public CulvertBuilder addStream(Stream stream) {
      this.streams.add(stream);
      return this;
    }

    public CulvertBuilder withStreamLaunchDelay(long delayMs) {
      this.streamLaunchDelayMs = delayMs;
      return this;
    }

    public CulvertBuilder withColumns(Column[] columnsOverride) {
      this.columns = columnsOverride;
      return this;
    }

    public CulvertBuilder withDynamicPartitioning(boolean dp) {
      this.dp = dp;
      return this;
    }

    public Culvert build() {
      return new Culvert(this);
    }
  }

  public static CulvertBuilder newBuilder() {
    return new CulvertBuilder();
  }

  public void run(CountDownLatch countDownLatch) {
    try {
      System.err.println("Starting culvert: " + getName());
      for (Stream stream : streams) {
        stream.setCountDownLatch(countDownLatch);
        if (streamDelay > 0) {
          System.err.println("Waiting " + streamDelay + "ms to start next stream: " + stream.getName());
          Thread.sleep(streamDelay);
        } else {
          System.err.println("Starting stream: " + stream.getName());
        }
        executorService.submit(stream);
      }
    } catch (Exception e) {
      System.err.println("Culvert run failed! error: " + e.getMessage());
      e.printStackTrace();
    }
  }

  public String getName() {
    return name;
  }

  public List<Stream> getStreams() {
    return streams;
  }

  public long getStreamDelay() {
    return streamDelay;
  }

  public Column[] getColumnsOverride() {
    return columnsOverride;
  }

  public static void main(String[] args) {
    int commitAfterNRows = 1_000_000;
    long timeout = TimeUnit.SECONDS.toMillis(60);
    boolean dynamicPartitioning = false;
    boolean streamingOptimizations = true;
    int transactionBatchSize = 1;
    int eventsPerSecond = 10_000;
    int numParallelStreams = 1;
    int streamLaunchDelayMs = 0;
    boolean enableAutoFlush = true;
    String metastoreUrl = "thrift://localhost:9083";
    String db = "default";
    String table = "culvert";
    startCulvert(metastoreUrl, db, table, numParallelStreams, commitAfterNRows, eventsPerSecond, timeout,
      dynamicPartitioning, streamingOptimizations, transactionBatchSize, streamLaunchDelayMs, enableAutoFlush);
  }

  static void startCulvert(final String metastoreUrl, final String db, final String table,
    final int numParallelStreams, final int commitAfterNRows, final int eventsPerSecond,
    final long timeout, final boolean dynamicPartitioning, final boolean streamingOptimizations,
    final int transactionBatchSize, final long streamLaunchDelayMs, final boolean enableAutoFlush) {
    Culvert culvert = buildCulvert(metastoreUrl, db, table, numParallelStreams, commitAfterNRows, eventsPerSecond,
      timeout, dynamicPartitioning, streamingOptimizations, transactionBatchSize, streamLaunchDelayMs, enableAutoFlush);
    CountDownLatch countDownLatch = new CountDownLatch(culvert.getStreams().size());
    culvert.run(countDownLatch);
    try {
      countDownLatch.await();
    } catch (InterruptedException e) {
      e.printStackTrace();
    }
    long totalRowsCommitted = 0;
    for (Stream stream : culvert.getStreams()) {
      totalRowsCommitted += stream.getRowsCommitted();
    }
    System.out.println("Total rows committed: " + totalRowsCommitted);
    int timeoutSeconds = (int) TimeUnit.MILLISECONDS.toSeconds(timeout);
    System.out.println("Throughput: " + (totalRowsCommitted / timeoutSeconds) + " rows/second");
    System.exit(0);
  }

  private static Culvert buildCulvert(final String metastoreUrl, final String db, final String table,
    final int numParallelStreams, final int commitAfterNRows, final int eventsPerSecond,
    final long timeout, final boolean dynamicPartitioning, final boolean streamingOptimizations,
    final int transactionBatchSize, final long streamLaunchDelayMs, final boolean enableAutoFlush) {
    Culvert.CulvertBuilder culvertBuilder = Culvert.newBuilder();
    for (int i = 0; i < numParallelStreams; i++) {
      HiveStreamingConnection.Builder connection = getStreamingConnection(metastoreUrl, db, table,
        Arrays.asList("2018", "" + i), dynamicPartitioning, streamingOptimizations, transactionBatchSize,
        enableAutoFlush);
      Stream stream = Stream.newBuilder()
        .withName("stream-" + i)
        .withCommitAfterRows(commitAfterNRows)
        .withEventsPerSecond(eventsPerSecond)
        .withTimeout(timeout)
        .withHiveStreamingConnectionBuilder(connection)
        .build();
      culvertBuilder.addStream(stream);
    }
    culvertBuilder.withDynamicPartitioning(dynamicPartitioning)
      .withStreamLaunchDelay(streamLaunchDelayMs)
      .build();
    return culvertBuilder.build();
  }

  // Table schema:
  // create table if not exists culvert (
  //  user_id string,
  //  page_id string,
  //  ad_id string,
  //  ad_type string,
  //  event_type string,
  //  event_time string,
  //  ip_address string)
  // partitioned by (year int, month int)
  // clustered by (user_id)
  // into 8 buckets
  // stored as orc
  // tblproperties("transactional"="true");
  private static HiveStreamingConnection.Builder getStreamingConnection(final String metastoreUrl,
    final String db, final String table, final List<String> staticPartitions,
    final boolean dynamicPartitioning, final boolean streamingOptimizations, final int transactionBatchSize,
    final boolean enableAutoFlush) {
    StrictDelimitedInputWriter writer = StrictDelimitedInputWriter.newBuilder()
      .withFieldDelimiter(',')
      .build();
    HiveConf conf = new HiveConf();
    conf.set(MetastoreConf.ConfVars.THRIFT_URIS.getHiveName(), metastoreUrl);
    conf.setBoolVar(HiveConf.ConfVars.HIVE_STREAMING_AUTO_FLUSH_ENABLED, enableAutoFlush);
    return HiveStreamingConnection.newBuilder()
      .withDatabase(db)
      .withTable(table)
      .withStaticPartitionValues(dynamicPartitioning ? null : staticPartitions)
      .withRecordWriter(writer)
      .withStreamingOptimizations(streamingOptimizations)
      .withTransactionBatchSize(transactionBatchSize)
      .withHiveConf(conf);
  }
}
