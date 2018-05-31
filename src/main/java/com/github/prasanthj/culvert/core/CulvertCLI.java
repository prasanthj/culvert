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

import org.apache.commons.cli.BasicParser;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;


/**
 *
 */
public class CulvertCLI {
  public static void main(String[] args) {
    Options options = new Options();
    addOptions(options);

    CommandLineParser parser = new BasicParser();
    CommandLine cli;
    String metastoreUrl = "thrift://localhost:9083";
    int commitAfterNRows = 1_000_000;
    long timeout = 60000;
    boolean dynamicPartitioning = false;
    boolean streamingOptimizations = true;
    int transactionBatchSize = 1;
    int eventsPerSecond = 10_000;
    int numParallelStreams = 1;
    long streamLaunchDelayMs = 0;
    boolean enableAutoFlush = true;
    try {
      cli = parser.parse(options, args);

      if (cli.hasOption('u')) {
        metastoreUrl = cli.getOptionValue('u');
      }

      if (cli.hasOption('n')) {
        commitAfterNRows = Integer.parseInt(cli.getOptionValue('n'));
      }

      if (cli.hasOption('t')) {
        timeout = Long.parseLong(cli.getOptionValue('t'));
      }

      if (cli.hasOption('d')) {
        dynamicPartitioning = true;
      }

      if (cli.hasOption('s')) {
        streamingOptimizations = false;
      }

      if (cli.hasOption('b')) {
        transactionBatchSize = Integer.parseInt(cli.getOptionValue('b'));
      }

      if (cli.hasOption('e')) {
        eventsPerSecond = Integer.parseInt(cli.getOptionValue('e'));
      }

      if (cli.hasOption('p')) {
        numParallelStreams = Integer.parseInt(cli.getOptionValue('p'));
      }

      if (cli.hasOption('l')) {
        streamLaunchDelayMs = Long.parseLong(cli.getOptionValue('l'));
      }

      if (cli.hasOption('f')) {
        enableAutoFlush = false;
      }

      if (cli.hasOption('h')) {
        usage(options);
        return;
      }

      Culvert.startCulvert(metastoreUrl, numParallelStreams, commitAfterNRows, eventsPerSecond, timeout,
        dynamicPartitioning, streamingOptimizations, transactionBatchSize, streamLaunchDelayMs, enableAutoFlush);
    } catch (ParseException e) {
      System.err.println("Invalid parameter.");
      usage(options);
    } catch (NumberFormatException e) {
      System.err.println("Invalid type for parameter.");
      usage(options);
    }
  }

  private static void addOptions(Options options) {
    options.addOption("u", "metastore-url", true, "remote metastore url." +
      " default = 'thrift://localhost:9083'");
    options.addOption("n", "commit-after-n-rows", true, "commit transaction " +
      " after every n rows. default = 1_000_000");
    options.addOption("t", "timeout", true, "timeout in milliseconds after which " + "" +
      "all streams in culvert will be stopped. default = 60000");
    options.addOption("d", "enable-dynamic-partition", false, "enable dynamic "
      + " partitioned insert (destination table has to be partitioned correctly). default = false");
    options.addOption("s", "disable-streaming-optimization", false, "disables "
      + "all streaming optimizations. default = false");
    options.addOption("b", "transaction-batch-size", true, "size of transaction batch" +
      ". default = 1");
    options.addOption("e", "events-per-second", true, "events/records per second " +
      "(values >1000 will all be same, as 1000/events-per-second will be used as sleep interval). default = 10_0000");
    options.addOption("p", "parallelism", true,
      "number of parallel streams. default = 1");
    options.addOption("l", "stream-launch-delay", true, "delay in milliseconds between" +
      " launching streams. default = 0");
    options.addOption("f", "disable-auto-flush", false,
      "disable auto-flush of open orc files. default = false");
    options.addOption("h", "help", false, "usage help");
  }

  static void usage(Options options) {
    System.out.println("Example usage: culvert -n 100000 -t 60000 -e 100\n");
    HelpFormatter formatter = new HelpFormatter();
    formatter.printHelp("Culvert", options);
  }
}
