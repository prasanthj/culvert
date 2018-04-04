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
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

import org.apache.hive.hcatalog.streaming.HiveEndPoint;

/**
 *
 */
public class Culvert {
  private String name;
  private List<Stream> streams;
  private int culvertDelay;
  private int streamDelay;
  private Column[] columnsOverride;
  private ExecutorService executorService;
  private boolean emitRowId;
  private boolean emitStreamName;
  private int txnsPerBatch;
  private int commitAfterNRows;
  private boolean isStopped = false;

  private Culvert(final String name, List<Stream> streams, int culvertDelay, int streamDelay, Column[] columns,
    boolean emitRowId, boolean emitStreamName, int txnsPerBatch, int commitAfterNRows,
    long timeout) {
    this.name = name;
    this.streams = streams;
    this.culvertDelay = culvertDelay;
    this.streamDelay = streamDelay;
    this.columnsOverride = columns;
    if (columnsOverride != null) {
      for (Stream stream : streams) {
        stream.setColumns(columns);
      }
    }
    this.emitRowId = emitRowId;
    if (emitRowId) {
      for (Stream stream : streams) {
        stream.setEmitRowId(true);
      }
    }
    this.emitStreamName = emitStreamName;
    if (emitStreamName) {
      for (Stream stream : streams) {
        stream.setEmitStreamName(true);
      }
    }
    this.txnsPerBatch = txnsPerBatch;
    if (txnsPerBatch > 0) {
      for (Stream stream : streams) {
        stream.setTxnsPerBatch(txnsPerBatch);
      }
    }
    this.commitAfterNRows = commitAfterNRows;
    if (commitAfterNRows > 0) {
      for (Stream stream : streams) {
        stream.setCommitAfterNRows(commitAfterNRows);
      }
    }
    this.executorService = Executors.newFixedThreadPool(streams.size());
    ScheduledExecutorService scheduledExecutor = Executors.newSingleThreadScheduledExecutor();
    scheduledExecutor.schedule(() -> {
      System.err.println("Timeout " + timeout + "ms expired. Shutting down streams..");
      for (Stream stream : streams) {
        stream.close();
      }
      isStopped = true;
    }, timeout, TimeUnit.MILLISECONDS);
  }

  private static class CulvertBuilder {
    private String name = "culvert";
    private List<Stream> streams = new ArrayList<Stream>();
    private Column[] columns;
    private boolean emitRowId;
    private boolean emitStreamName;
    private int culvertLaunchDelayMs = 1000;
    private int streamLaunchDelayMs = 1000;
    private int txnsPerBatch = 10;
    private int commitAfterNRows = 10000;
    private long timeout = -1;

    public CulvertBuilder withName(String name) {
      this.name = name;
      return this;
    }

    public CulvertBuilder addStream(Stream stream) {
      this.streams.add(stream);
      return this;
    }

    public CulvertBuilder withCulvertLaunchDelay(int delayMs) {
      this.culvertLaunchDelayMs = delayMs;
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

    public CulvertBuilder withEmitRowId(boolean emitRowId) {
      this.emitRowId = emitRowId;
      return this;
    }

    public CulvertBuilder withEmitStreamName(boolean emitStreamName) {
      this.emitStreamName = emitStreamName;
      return this;
    }

    public CulvertBuilder withTxnsPerBatch(int txnsPerBatch) {
      this.txnsPerBatch = txnsPerBatch;
      return this;
    }

    public CulvertBuilder withCommitAfterRows(int numRows) {
      this.commitAfterNRows = numRows;
      return this;
    }

    public CulvertBuilder withTimeout(long millis) {
      this.timeout = millis;
      return this;
    }

    public Culvert build() {
      return new Culvert(name, new ArrayList<>(streams), culvertLaunchDelayMs, streamLaunchDelayMs,
        columns, emitRowId, emitStreamName, txnsPerBatch, commitAfterNRows, timeout);
    }
  }

  public static CulvertBuilder newBuilder() {
    return new CulvertBuilder();
  }

  public void run() {
    try {
      if (culvertDelay > 0) {
        System.err.println("Waiting " + culvertDelay + "ms to start culvert: " + getName());
        Thread.sleep(culvertDelay);
      } else {
        System.err.println("Starting culvert: " + getName());
      }
      for (Stream stream : streams) {
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
      // ignore
    }
    while (!isStopped && !Thread.interrupted()) {
      try {
        Thread.sleep(1000);
      } catch (InterruptedException e) {
        // ignore
      }
    }
    System.exit(0);
  }

  public String getName() {
    return name;
  }

  public List<Stream> getStreams() {
    return streams;
  }

  public int getCulvertDelay() {
    return culvertDelay;
  }

  public int getStreamDelay() {
    return streamDelay;
  }

  public Column[] getColumnsOverride() {
    return columnsOverride;
  }

  public boolean isEmitRowId() {
    return emitRowId;
  }

  public boolean isEmitStreamName() {
    return emitStreamName;
  }

  public int getTxnsPerBatch() {
    return txnsPerBatch;
  }

  public int getCommitAfterNRows() {
    return commitAfterNRows;
  }

  public static void main(String[] args) {
    String metastoreUri = "thrift://localhost:9083";
    HiveEndPoint hiveEndPoint1 = new HiveEndPoint(metastoreUri, "default", "culvert", Arrays.asList("2018", "1"));
    Stream stream1 = Stream.newBuilder()
      .withName("click-stream-1")
      .withEventsPerSecond(100)
      .withHiveEndPoint(hiveEndPoint1)
      .build();
    HiveEndPoint hiveEndPoint2 = new HiveEndPoint(metastoreUri, "default", "culvert", Arrays.asList("2018", "2"));
    Stream stream2 = Stream.newBuilder()
      .withName("click-stream-2")
      .withEventsPerSecond(500)
      .withHiveEndPoint(hiveEndPoint2)
      .build();
    HiveEndPoint hiveEndPoint3 = new HiveEndPoint(metastoreUri, "default", "culvert", Arrays.asList("2018", "3"));
    Stream stream3 = Stream.newBuilder()
      .withName("click-stream-3")
      .withEventsPerSecond(1000)
      .withHiveEndPoint(hiveEndPoint3)
      .build();
    HiveEndPoint hiveEndPoint4 = new HiveEndPoint(metastoreUri, "default", "culvert", Arrays.asList("2018", "4"));
    Stream stream4 = Stream.newBuilder()
      .withName("click-stream-4")
      .withEventsPerSecond(800)
      .withHiveEndPoint(hiveEndPoint4)
      .build();
    Culvert culvert = Culvert.newBuilder()
      .addStream(stream1)
      .addStream(stream2)
      .addStream(stream3)
      .addStream(stream4)
      .withCulvertLaunchDelay(0)
      .withStreamLaunchDelay(0)
      .withEmitRowId(false)
      .withEmitStreamName(false)
      .withTimeout(TimeUnit.MINUTES.toMillis(2))
      .build();
    culvert.run();
  }
}
