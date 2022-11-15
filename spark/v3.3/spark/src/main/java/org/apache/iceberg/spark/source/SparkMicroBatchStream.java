/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.iceberg.spark.source;

import java.io.BufferedWriter;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.OutputStreamWriter;
import java.io.UncheckedIOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;
import java.util.Iterator;
import java.util.List;
import java.util.Locale;
import java.util.Spliterator;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import org.apache.iceberg.CombinedScanTask;
import org.apache.iceberg.DataOperations;
import org.apache.iceberg.FileScanTask;
import org.apache.iceberg.MicroBatches;
import org.apache.iceberg.MicroBatches.MicroBatch;
import org.apache.iceberg.Schema;
import org.apache.iceberg.SchemaParser;
import org.apache.iceberg.Snapshot;
import org.apache.iceberg.SnapshotSummary;
import org.apache.iceberg.Table;
import org.apache.iceberg.io.CloseableIterable;
import org.apache.iceberg.io.FileIO;
import org.apache.iceberg.io.InputFile;
import org.apache.iceberg.io.OutputFile;
import org.apache.iceberg.relocated.com.google.common.base.Joiner;
import org.apache.iceberg.relocated.com.google.common.base.Preconditions;
import org.apache.iceberg.relocated.com.google.common.collect.Iterables;
import org.apache.iceberg.relocated.com.google.common.collect.Iterators;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;
import org.apache.iceberg.spark.SparkReadConf;
import org.apache.iceberg.spark.SparkReadOptions;
import org.apache.iceberg.spark.source.SparkScan.ReaderFactory;
import org.apache.iceberg.util.PropertyUtil;
import org.apache.iceberg.util.SnapshotUtil;
import org.apache.iceberg.util.TableScanUtil;
import org.apache.iceberg.util.Tasks;
import org.apache.iceberg.util.ThreadPools;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.broadcast.Broadcast;
import org.apache.spark.sql.connector.read.InputPartition;
import org.apache.spark.sql.connector.read.PartitionReaderFactory;
import org.apache.spark.sql.connector.read.streaming.MicroBatchStream;
import org.apache.spark.sql.connector.read.streaming.Offset;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class SparkMicroBatchStream implements MicroBatchStream {
  private static final Joiner SLASH = Joiner.on("/");
  private static final Logger LOG = LoggerFactory.getLogger(SparkMicroBatchStream.class);

  private final Table table;
  private final boolean caseSensitive;
  private final String expectedSchema;
  private final Broadcast<Table> tableBroadcast;
  private final Long splitSize;
  private final Integer splitLookback;
  private final Long splitOpenFileCost;
  private final boolean localityPreferred;
  private final boolean skipDelete;
  private final boolean skipOverwrite;
  private final Long fromTimestamp;

  SparkMicroBatchStream(
      JavaSparkContext sparkContext,
      Table table,
      SparkReadConf readConf,
      Schema expectedSchema) {
    this.table = table;
    this.caseSensitive = readConf.caseSensitive();
    this.expectedSchema = SchemaParser.toJson(expectedSchema);
    this.localityPreferred = readConf.localityEnabled();
    this.tableBroadcast = sparkContext.broadcast(SerializableTableWithSize.copyOf(table));
    this.splitSize = readConf.splitSize();
    this.splitLookback = readConf.splitLookback();
    this.splitOpenFileCost = readConf.splitOpenFileCost();
    this.fromTimestamp = readConf.streamFromTimestamp();
    this.skipDelete = readConf.streamingSkipDeleteSnapshots();
    this.skipOverwrite = readConf.streamingSkipOverwriteSnapshots();
  }

  @Override
  public Offset latestOffset() {
    table.refresh();
    final Snapshot latestSnapshot = table.currentSnapshot();
    if (latestSnapshot == null) {
      throw new IllegalStateException("Table " + table.name() + " does not have any current snapshot");
    }

    final long addedFilesCountProp =
        PropertyUtil.propertyAsLong(latestSnapshot.summary(), SnapshotSummary.ADDED_FILES_PROP, -1);
    // If snapshotSummary doesn't have SnapshotSummary.ADDED_FILES_PROP, iterate through addedFiles
    // iterator to find
    // addedFilesCount.
    final long addedFilesCount =
        addedFilesCountProp == -1
            ? Iterables.size(latestSnapshot.addedDataFiles(table.io()))
            : addedFilesCountProp;

    return new StreamingOffset(latestSnapshot.snapshotId(), addedFilesCount);
  }

  @Override
  public InputPartition[] planInputPartitions(Offset start, Offset end) {
    Preconditions.checkArgument(
            start instanceof StreamingOffset,
            "Invalid start offset: %s is not a StreamingOffset",
            start);
    Preconditions.checkArgument(
            end instanceof StreamingOffset,
            "Invalid end offset: %s is not a StreamingOffset",
            end);

    Snapshot startOffset = table.snapshot(((StreamingOffset) start).snapshotId());
    Snapshot endOffset = table.snapshot(((StreamingOffset) end).snapshotId());

    List<FileScanTask> fileScanTasks = planFiles(startOffset, endOffset);
    try {
      Files.write(Paths.get("./log.txt"), ("\nfileScanTasks: " + fileScanTasks).getBytes(), StandardOpenOption.APPEND);
    } catch (Exception ignored) {}

    CloseableIterable<FileScanTask> splitTasks =
        TableScanUtil.splitFiles(CloseableIterable.withNoopClose(fileScanTasks), splitSize);
    List<CombinedScanTask> combinedScanTasks =
        Lists.newArrayList(
            TableScanUtil.planTasks(splitTasks, splitSize, splitLookback, splitOpenFileCost));

    InputPartition[] partitions = new InputPartition[combinedScanTasks.size()];

    Tasks.range(partitions.length)
        .stopOnFailure()
        .executeWith(localityPreferred ? ThreadPools.getWorkerPool() : null)
        .run(
            index ->
                partitions[index] =
                    new SparkInputPartition(
                        combinedScanTasks.get(index),
                        tableBroadcast,
                        expectedSchema,
                        caseSensitive,
                        localityPreferred));

    return partitions;
  }

  @Override
  public PartitionReaderFactory createReaderFactory() {
    return new ReaderFactory(0);
  }

  @Override
  public Offset initialOffset() {
    Snapshot snapshot = fromTimestamp == null
            ? SnapshotUtil.oldestAncestor(table) // start from the oldest snapshot
            : SnapshotUtil.oldestAncestorAfter(table, fromTimestamp);

    if (snapshot == null) {
      throw new IllegalStateException("Table " + table.name() + " does not have any current snapshot");
    }

    return new StreamingOffset(snapshot.snapshotId(), 0);
  }

  @Override
  public Offset deserializeOffset(String json) {
    return StreamingOffset.fromJson(json);
  }

  @Override
  public void commit(Offset end) {}

  @Override
  public void stop() {}

  private List<FileScanTask> planFiles(Snapshot startSnapshot, Snapshot endSnapshot) {
    List<FileScanTask> fileScanTasks = Lists.newArrayList();

    Snapshot currentSnapshot = endSnapshot;
    while (currentSnapshot.timestampMillis() >= startSnapshot.timestampMillis()) {
      if (!shouldProcess(currentSnapshot)) {
        LOG.debug("Skipping snapshot: {} of table {}", currentSnapshot.snapshotId(), table.name());
        continue;
      }

      MicroBatch latestMicroBatch =
          MicroBatches.from(currentSnapshot, table.io())
              .caseSensitive(caseSensitive)
              .specsById(table.specs())
              .generate(0, Long.MAX_VALUE);

      fileScanTasks.addAll(0, latestMicroBatch.tasks());
      try {
        Files.write(Paths.get("./log.txt"), ("\ncurrentSnapshot.parentId(): " + currentSnapshot.parentId()).getBytes(), StandardOpenOption.APPEND);
      } catch (Exception ignored) {}

      if (currentSnapshot.parentId() != null) {
        currentSnapshot = table.snapshot(currentSnapshot.parentId());
      } else {
        break;
      }
    }

    return fileScanTasks;
  }

  private boolean shouldProcess(Snapshot snapshot) {
    String op = snapshot.operation();
    switch (op) {
      case DataOperations.APPEND:
        return true;
      case DataOperations.REPLACE:
        return false;
      case DataOperations.DELETE:
        Preconditions.checkState(
            skipDelete,
            "Cannot process delete snapshot: %s, to ignore deletes, set %s=true",
            snapshot.snapshotId(),
            SparkReadOptions.STREAMING_SKIP_DELETE_SNAPSHOTS);
        return false;
      case DataOperations.OVERWRITE:
        Preconditions.checkState(
            skipOverwrite,
            "Cannot process overwrite snapshot: %s, to ignore overwrites, set %s=true",
            snapshot.snapshotId(),
            SparkReadOptions.STREAMING_SKIP_OVERWRITE_SNAPSHOTS);
        return false;
      default:
        throw new IllegalStateException(
            String.format(
                "Cannot process unknown snapshot operation: %s (snapshot id %s)",
                op.toLowerCase(Locale.ROOT), snapshot.snapshotId()));
    }
  }
}
