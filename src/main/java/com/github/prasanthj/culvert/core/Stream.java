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

import java.io.OutputStream;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.stream.Collectors;

import org.apache.hive.streaming.HiveStreamingConnection;
import org.apache.hive.streaming.StreamingConnection;
import org.apache.hive.streaming.StreamingException;

/**
 *
 */
public class Stream implements Runnable {
  private static int streamIdx = 0;
  private String name;
  private List<Column> columns;
  private OutputStream outputStream;
  private long sleepMs;
  private final HiveStreamingConnection.Builder streamingConnectionBuilder;
  private StreamingConnection streamingConnection;
  private int txnsPerBatch;
  private int commitAfterNRows;
  private AtomicBoolean isClosed = new AtomicBoolean(false);
  private long rowsCommitted = 0;
  private long rowsWritten = 0;
  private long timeout;
  private CountDownLatch countDownLatch;

  public Stream(final StreamBuilder builder) {
    this.name = builder.name;
    this.columns = new ArrayList<>(Arrays.asList(builder.columns));
    this.outputStream = builder.outputStream;
    this.sleepMs = builder.eventsPerSecond == 0 ? 0 : (long) (1000.0 / builder.eventsPerSecond);
    this.streamingConnectionBuilder = builder.streamingConnection;
    this.txnsPerBatch = builder.txnsPerBatch;
    this.commitAfterNRows = builder.commitAfterNRows;
    this.timeout = builder.timeout;
  }

  private void startStreamingConnection() throws StreamingException {
    if (streamingConnection == null) {
      streamingConnection = streamingConnectionBuilder.connect();
      try {
        streamingConnection.beginTransaction();
      } catch (StreamingException connectionError) {
        connectionError.printStackTrace();
      }
    }
  }

  void setColumns(final Column[] columns) {
    this.columns = new ArrayList<>(Arrays.asList(columns));
  }

  void setDynamicPartition() {
    this.columns.add(Column.newBuilder().withName("year").withType(Column.Type.INT_YEAR).build());
    this.columns.add(Column.newBuilder().withName("month").withType(Column.Type.INT_MONTH).build());
  }

  void setTxnsPerBatch(final int txnsPerBatch) {
    this.txnsPerBatch = txnsPerBatch;
  }

  void setCommitAfterNRows(final int commitAfterNRows) {
    this.commitAfterNRows = commitAfterNRows;
  }

  public static StreamBuilder newBuilder() {
    return new StreamBuilder();
  }

  public static class StreamBuilder {
    private String name = "stream-" + streamIdx++;
    private Column[] columns = null;
    private OutputStream outputStream = System.out;
    private double eventsPerSecond = 10.0;
    private HiveStreamingConnection.Builder streamingConnection;
    private int txnsPerBatch = 10;
    private int commitAfterNRows = 10000;
    private long timeout = -1;

    public StreamBuilder withName(String name) {
      this.name = name;
      return this;
    }

    public StreamBuilder withColumns(Column[] columns) {
      this.columns = columns;
      return this;
    }

    public StreamBuilder withOutputStream(OutputStream outputStream) {
      this.outputStream = outputStream;
      return this;
    }

    public StreamBuilder withEventsPerSecond(double eventsPerSecond) {
      this.eventsPerSecond = eventsPerSecond;
      return this;
    }

    public StreamBuilder withHiveStreamingConnectionBuilder(HiveStreamingConnection.Builder streamingConnection) {
      this.streamingConnection = streamingConnection;
      return this;
    }

    public StreamBuilder withTxnsPerBatch(int txnsPerBatch) {
      this.txnsPerBatch = txnsPerBatch;
      return this;
    }

    public StreamBuilder withCommitAfterRows(int numRows) {
      this.commitAfterNRows = numRows;
      return this;
    }

    public StreamBuilder withTimeout(final long timeout) {
      this.timeout = timeout;
      return this;
    }
    public Stream build() {
      if (columns == null) {
        populatedDefaultColumns();
      }
      return new Stream(this);
    }

    // schema from https://yahooeng.tumblr.com/post/135321837876/benchmarking-streaming-computation-engines-at
    private void populatedDefaultColumns() {
      Column[] cols = new Column[7];
      cols[0] = Column.newBuilder().withName("user_id").withType(Column.Type.STRING_UUID_DICT).build();
      cols[1] = Column.newBuilder().withName("page_id").withType(Column.Type.STRING_UUID_DICT).build();
      cols[2] = Column.newBuilder().withName("ad_id").withType(Column.Type.STRING_UUID_DICT).build();
      cols[3] = Column.newBuilder().withName("ad_type").withType(Column.Type.STRING_DICT)
        .withDictionary(new Object[]{"banner", "modal", "sponsored-search", "mail", "mobile"})
        .build();
      cols[4] = Column.newBuilder().withName("event_type").withType(Column.Type.STRING_DICT)
        .withDictionary(new Object[]{"view", "click", "purchase"})
        .build();
      cols[5] = Column.newBuilder().withName("event_time").withType(Column.Type.TIMESTAMP).build();
      cols[6] = Column.newBuilder().withName("ip_address").withType(Column.Type.STRING_IP_ADDRESS).build();
      columns = cols;
    }
  }

  @Override
  public void run() {
    Thread.currentThread().setName(name);
    long txnBatchesCommitted = 0;
    try {
      startStreamingConnection();
    } catch (Exception | Error e) {
      System.err.println("Unable to start hive streaming connection");
      e.printStackTrace();
      close();
      return;
    }
    if (timeout > 0) {
      startTimeoutThread();
    }
    while (!isClosed.get() && !Thread.interrupted()) {
      String row = columns.stream().map(c -> c.getValue(rowsWritten).toString()).collect(Collectors.joining(","));
      rowsWritten++;
      try {
        if (streamingConnection == null) {
          outputStream.write(row.getBytes("UTF-8"));
        } else {
          streamingConnection.write(row.getBytes("UTF-8"));
          if (rowsWritten > 0 && (rowsWritten % commitAfterNRows == 0)) {
            streamingConnection.commitTransaction();
            streamingConnection.beginTransaction();
            rowsCommitted = rowsWritten;
            txnBatchesCommitted++;
            System.out.println("Stream [" + name + "] committed " + txnBatchesCommitted + " transactions [rows: " +
              rowsCommitted + "]..");
          }
        }
        if (sleepMs > 0) {
          Thread.sleep(sleepMs);
        }
      } catch (Exception | Error e) {
        System.err.println("Stream [" + name + "] died! error: " + e.getMessage());
        e.printStackTrace();
        close();
        break;
      }
    }

    close();
  }

  private void startTimeoutThread() {
    ScheduledExecutorService scheduledExecutor = Executors.newSingleThreadScheduledExecutor();
    scheduledExecutor.schedule(() -> {
      System.err.println("Timeout " + timeout + "ms expired. Shutting down streams.. ");
      isClosed.set(true);
    }, timeout, TimeUnit.MILLISECONDS);
  }

  private void close() {
    if (streamingConnection != null) {
      streamingConnection.close();
      streamingConnection = null;
      isClosed.set(true);
      System.err.println("Closed [" + name + "]. Rows committed: " + rowsCommitted);
    }
    if (countDownLatch != null) {
      countDownLatch.countDown();
    }
  }

  public void setCountDownLatch(CountDownLatch countDownLatch) {
    this.countDownLatch = countDownLatch;
  }

  public String getName() {
    return name;
  }

  public Column[] getColumns() {
    return columns.toArray(new Column[]{null});
  }

  public OutputStream getOutputStream() {
    return outputStream;
  }

  public long getSleepMs() {
    return sleepMs;
  }

  public int getTxnsPerBatch() {
    return txnsPerBatch;
  }

  public int getCommitAfterNRows() {
    return commitAfterNRows;
  }
}
