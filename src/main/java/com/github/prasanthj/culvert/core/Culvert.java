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

import org.apache.hive.streaming.HiveStreamingConnection;
import org.apache.hive.streaming.StrictDelimitedInputWriter;

/**
 *
 */
public class Culvert {
  private String name;
  private List<Stream> streams;
  private int streamDelay;
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
    private int streamLaunchDelayMs = 1000;
    private boolean dp;

    public CulvertBuilder withName(String name) {
      this.name = name;
      return this;
    }

    public CulvertBuilder addStream(Stream stream) {
      this.streams.add(stream);
      return this;
    }

    public CulvertBuilder withStreamLaunchDelay(int delayMs) {
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

  public int getStreamDelay() {
    return streamDelay;
  }

  public Column[] getColumnsOverride() {
    return columnsOverride;
  }

  public static void main(String[] args) {
    int commitAfterNRows = 1_000_000;
    long timeout = TimeUnit.SECONDS.toMillis(120);
    boolean dynamicPartitioning = false;
    boolean streamingOptimizations = true;
    int transactionBatchSize = 1;
    HiveStreamingConnection.Builder connection1 = getStreamingConnection(Arrays.asList("2018", "1"),
      dynamicPartitioning, streamingOptimizations, transactionBatchSize);
    HiveStreamingConnection.Builder connection2 = getStreamingConnection(Arrays.asList("2018", "2"),
      dynamicPartitioning, streamingOptimizations, transactionBatchSize);
    HiveStreamingConnection.Builder connection3 = getStreamingConnection(Arrays.asList("2018", "3"),
      dynamicPartitioning, streamingOptimizations, transactionBatchSize);
    HiveStreamingConnection.Builder connection4 = getStreamingConnection(Arrays.asList("2018", "4"),
      dynamicPartitioning, streamingOptimizations, transactionBatchSize);
    Stream stream1 = Stream.newBuilder()
      .withName("stream-1")
      .withCommitAfterRows(commitAfterNRows)
      .withEventsPerSecond(100)
      .withTimeout(timeout)
      .withHiveStreamingConnectionBuilder(connection1)
      .build();

    Stream stream2 = Stream.newBuilder()
      .withName("stream-2")
      .withCommitAfterRows(commitAfterNRows)
      .withEventsPerSecond(500)
      .withTimeout(timeout)
      .withHiveStreamingConnectionBuilder(connection2)
      .build();

    Stream stream3 = Stream.newBuilder()
      .withName("stream-3")
      .withCommitAfterRows(commitAfterNRows)
      .withEventsPerSecond(1000)
      .withTimeout(timeout)
      .withHiveStreamingConnectionBuilder(connection3)
      .build();

    Stream stream4 = Stream.newBuilder()
      .withName("stream-4")
      .withCommitAfterRows(commitAfterNRows)
      .withEventsPerSecond(10000)
      .withTimeout(timeout)
      .withHiveStreamingConnectionBuilder(connection4)
      .build();

    Culvert culvert = Culvert.newBuilder()
      .addStream(stream1)
      .addStream(stream2)
      .addStream(stream3)
      .addStream(stream4)
      .withDynamicPartitioning(dynamicPartitioning)
      .withStreamLaunchDelay(0)
      .build();
    int streamSize = culvert.getStreams().size();
    CountDownLatch countDownLatch = new CountDownLatch(streamSize);
    culvert.run(countDownLatch);
    try {
      countDownLatch.await();
    } catch (InterruptedException e) {
      e.printStackTrace();
    }
    System.exit(0);
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
  private static HiveStreamingConnection.Builder getStreamingConnection(final List<String> staticPartitions,
    final boolean dynamicPartitioning, final boolean streamingOptimizations, final int transactionBatchSize) {
    StrictDelimitedInputWriter writer = StrictDelimitedInputWriter.newBuilder()
      .withFieldDelimiter(',')
      .build();
    final String dbName = "default";
    final String tableName = "culvert";
    return HiveStreamingConnection.newBuilder()
      .withDatabase(dbName)
      .withTable(tableName)
      .withStaticPartitionValues(dynamicPartitioning ? null : staticPartitions)
      .withRecordWriter(writer)
      .withStreamingOptimizations(streamingOptimizations)
      .withTransactionBatchSize(transactionBatchSize);
  }
}
