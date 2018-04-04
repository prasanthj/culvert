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

import java.io.IOException;
import java.io.OutputStream;
import java.util.StringJoiner;

import org.apache.hive.hcatalog.streaming.DelimitedInputWriter;
import org.apache.hive.hcatalog.streaming.HiveEndPoint;
import org.apache.hive.hcatalog.streaming.RecordWriter;
import org.apache.hive.hcatalog.streaming.StreamingConnection;
import org.apache.hive.hcatalog.streaming.StreamingException;
import org.apache.hive.hcatalog.streaming.TransactionBatch;

/**
 *
 */
public class Stream implements Runnable {
  private static int streamIdx = 0;
  private String name;
  private Column[] columns;
  private OutputStream outputStream;
  private long sleepMs;
  private boolean emitRowId;
  private boolean emitStreamName;
  private final HiveEndPoint hiveEndpoint;
  private int txnsPerBatch;
  private int commitAfterNRows;
  private StreamingConnection streamingConnection;
  private RecordWriter recordWriter;
  private TransactionBatch txnBatch;
  private boolean isClosed = false;
  private long rowsCommitted = 0;
  private long rowsWritten = 0;

  private Stream(String name, Column[] columns, OutputStream outputStream, double eventsPerSecond, boolean emitRowId,
    boolean emitStreamName, HiveEndPoint hiveEndpoint, int txnsPerBatch, int commitAfterNRows) {
    this.name = name;
    this.columns = columns;
    this.outputStream = outputStream;
    this.sleepMs = eventsPerSecond == 0 ? 0 : (long) (1000.0 / eventsPerSecond);
    this.emitRowId = emitRowId;
    this.emitStreamName = emitStreamName;
    this.hiveEndpoint = hiveEndpoint;
    this.txnsPerBatch = txnsPerBatch;
    this.commitAfterNRows = commitAfterNRows;
  }

  private void setupStreamingConnection() {
    if (hiveEndpoint != null) {
      try {
        this.streamingConnection = hiveEndpoint.newConnection(true, "culvert-agent");
        String[] fields = new String[columns.length];
        for (int i = 0; i < columns.length; i++) {
          fields[i] = columns[i].getName();
        }
        this.recordWriter = new DelimitedInputWriter(fields, ",", hiveEndpoint, streamingConnection);
        createAndBeginNextTxnBatch();
      } catch (InterruptedException | StreamingException | ClassNotFoundException connectionError) {
        connectionError.printStackTrace();
      }
    }
  }

  private void createAndBeginNextTxnBatch() throws InterruptedException, StreamingException {
    this.txnBatch = streamingConnection.fetchTransactionBatch(txnsPerBatch, recordWriter);
    this.txnBatch.beginNextTransaction();
  }

  void setColumns(final Column[] columns) {
    this.columns = columns;
  }

  void setEmitRowId(boolean emitRowId) {
    this.emitRowId = emitRowId;
  }

  void setEmitStreamName(boolean emitStreamName) {
    this.emitStreamName = emitStreamName;
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
    private boolean emitRowId = false;
    private boolean emitStreamName = false;
    private HiveEndPoint hiveEndpoint;
    private int txnsPerBatch = 10;
    private int commitAfterNRows = 10000;

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

    public StreamBuilder withEmitRowId(boolean emitRowId) {
      this.emitRowId = emitRowId;
      return this;
    }

    public StreamBuilder withEmitStreamName(boolean emitStreamName) {
      this.emitStreamName = emitStreamName;
      return this;
    }

    public StreamBuilder withHiveEndPoint(HiveEndPoint hiveEndpoint) {
      this.hiveEndpoint = hiveEndpoint;
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

    public Stream build() {
      if (columns == null) {
        populatedDefaultColumns();
      }
      return new Stream(name, columns, outputStream, eventsPerSecond, emitRowId, emitStreamName, hiveEndpoint,
        txnsPerBatch, commitAfterNRows);
    }

    // schema from https://yahooeng.tumblr.com/post/135321837876/benchmarking-streaming-computation-engines-at
    private void populatedDefaultColumns() {
      Column[] cols = new Column[7];
      cols[0] = Column.newBuilder().withName("user_id").withType(Column.Type.STRING_UUID).build();
      cols[1] = Column.newBuilder().withName("page_id").withType(Column.Type.STRING_UUID).build();
      cols[2] = Column.newBuilder().withName("ad_id").withType(Column.Type.STRING_UUID).build();
      cols[3] = Column.newBuilder().withName("ad_type").withType(Column.Type.STRING_DICT)
        .withDictionary(new Object[]{"banner", "modal", "sponsored-search", "mail", "mobile"})
        .build();
      cols[4] = Column.newBuilder().withName("event_type").withType(Column.Type.STRING_DICT)
        .withDictionary(new Object[]{"view", "click", "purchase"})
        .build();
      cols[5] = Column.newBuilder().withName("event_time").withType(Column.Type.TIMESTAMP).build();
      cols[6] = Column.newBuilder().withName("ip_address").withType(Column.Type.STRING_IPADDRESS).build();
      columns = cols;
    }
  }

  @Override
  public void run() {
    Thread.currentThread().setName(name);
    Thread.setDefaultUncaughtExceptionHandler((t, e) -> close());
    long txnBatchesCommitted = 0;
    setupStreamingConnection();
    while (!isClosed && !Thread.interrupted()) {
      StringJoiner stringJoiner = new StringJoiner(",");
      if (emitStreamName) {
        stringJoiner.add("" + name);
      }
      rowsWritten++;
      if (emitRowId) {
        stringJoiner.add("" + rowsWritten);
      }
      for (Column column : columns) {
        stringJoiner.add(column.getValue().toString());
      }
      String row = stringJoiner.toString() + "\n";
      try {
        if (txnBatch == null) {
          outputStream.write(row.getBytes("UTF-8"));
        } else {
          if (isClosed) {
            throw new RuntimeException("Cannot write to closed stream: " + name);
          }
          txnBatch.write(row.getBytes("UTF-8"));
          if (rowsWritten > 0 && (rowsWritten % commitAfterNRows == 0)) {
            commitAndMoveForward();
            rowsCommitted = rowsWritten;
            txnBatchesCommitted++;
            System.err.println("Stream [" + name + "] committed " + txnBatchesCommitted + " transactions [rows: " +
              rowsCommitted + ", currTxnId: " + txnBatch.getCurrentTxnId() + ", currWriteId: " +
              txnBatch.getCurrentWriteId() + "]..");
          }
        }
        if (sleepMs > 0) {
          Thread.sleep(sleepMs);
        }
      } catch (IOException | InterruptedException | StreamingException e) {
        System.err.println("Stream [" + name + "] died! error: " + e.getMessage());
        close();
        break;
      }
    }
  }

  private void commitAndMoveForward() throws StreamingException, InterruptedException {
    synchronized (txnBatch) {
      if (!txnBatch.isClosed()) {
        txnBatch.commit();
        txnBatch.beginNextTransaction();
        txnBatch.heartbeat();
        if (txnBatch.remainingTransactions() == 0) {
          txnBatch.close();
          createAndBeginNextTxnBatch();
        }
      }
    }
  }

  public void close() {
    try {
      synchronized (txnBatch) {
        if (txnBatch != null) {
          txnBatch.close();
          streamingConnection.close();
          recordWriter = null;
          txnBatch = null;
          streamingConnection = null;
          isClosed = true;
          System.err.println("Closed [" + name + "]. Rows committed: " + rowsCommitted);
        }
      }
    } catch (StreamingException | InterruptedException e1) {
      // ignore
    }
  }


  public String getName() {
    return name;
  }

  public Column[] getColumns() {
    return columns;
  }

  public OutputStream getOutputStream() {
    return outputStream;
  }

  public long getSleepMs() {
    return sleepMs;
  }

  public boolean isEmitRowId() {
    return emitRowId;
  }

  public boolean isEmitStreamName() {
    return emitStreamName;
  }

  public HiveEndPoint getHiveEndpoint() {
    return hiveEndpoint;
  }

  public int getTxnsPerBatch() {
    return txnsPerBatch;
  }

  public int getCommitAfterNRows() {
    return commitAfterNRows;
  }
}
