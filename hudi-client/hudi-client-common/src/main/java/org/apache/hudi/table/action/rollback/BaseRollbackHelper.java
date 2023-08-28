/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hudi.table.action.rollback;

import org.apache.hudi.avro.model.HoodieRollbackRequest;
import org.apache.hudi.common.HoodieRollbackStat;
import org.apache.hudi.common.config.SerializableConfiguration;
import org.apache.hudi.common.data.HoodiePairData;
import org.apache.hudi.common.engine.HoodieEngineContext;
import org.apache.hudi.common.fs.FSUtils;
import org.apache.hudi.common.function.SerializableFunction;
import org.apache.hudi.common.function.SerializablePairFunction;
import org.apache.hudi.common.model.HoodieLogFile;
import org.apache.hudi.common.model.IOType;
import org.apache.hudi.common.table.HoodieTableMetaClient;
import org.apache.hudi.common.table.log.HoodieLogFileWriteCallback;
import org.apache.hudi.common.table.log.HoodieLogFormat;
import org.apache.hudi.common.table.log.block.HoodieCommandBlock;
import org.apache.hudi.common.table.log.block.HoodieLogBlock;
import org.apache.hudi.common.table.timeline.HoodieInstant;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.common.util.collection.Pair;
import org.apache.hudi.config.HoodieWriteConfig;
import org.apache.hudi.exception.HoodieIOException;
import org.apache.hudi.exception.HoodieRollbackException;
import org.apache.hudi.table.HoodieTable;
import org.apache.hudi.table.marker.WriteMarkers;
import org.apache.hudi.table.marker.WriteMarkersFactory;

import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/**
 * Contains common methods to be used across engines for rollback operation.
 */
public class BaseRollbackHelper implements Serializable {

  private static final Logger LOG = LoggerFactory.getLogger(BaseRollbackHelper.class);
  protected static final String EMPTY_STRING = "";

  protected final HoodieTable table;
  protected final HoodieTableMetaClient metaClient;
  protected final HoodieWriteConfig config;

  public BaseRollbackHelper(HoodieTable table, HoodieWriteConfig config) {
    this.table = table;
    this.metaClient = table.getMetaClient();
    this.config = config;
  }

  /**
   * Performs all rollback actions that we have collected in parallel.
   */
  public List<HoodieRollbackStat> performRollback(HoodieEngineContext context, String instantTime, HoodieInstant instantToRollback,
                                                  List<HoodieRollbackRequest> rollbackRequests) {
    int parallelism = Math.max(Math.min(rollbackRequests.size(), config.getRollbackParallelism()), 1);
    context.setJobStatus(this.getClass().getSimpleName(), "Perform rollback actions: " + config.getTableName());
    // If not for conversion to HoodieRollbackInternalRequests, code fails. Using avro model (HoodieRollbackRequest) within spark.parallelize
    // is failing with com.esotericsoftware.kryo.KryoException
    // stack trace: https://gist.github.com/nsivabalan/b6359e7d5038484f8043506c8bc9e1c8
    // related stack overflow post: https://issues.apache.org/jira/browse/SPARK-3601. Avro deserializes list as GenericData.Array.
    List<SerializableHoodieRollbackRequest> serializableRequests = rollbackRequests.stream().map(SerializableHoodieRollbackRequest::new).collect(Collectors.toList());
    WriteMarkers markers = WriteMarkersFactory.get(config.getMarkersType(), table, instantTime);

    // Considering rollback may failed before, which generated some additional log files. We need to add these log files back.
    Set<String> logPaths = new HashSet<>();
    try {
      logPaths = markers.getAppendedLogPaths(context, config.getFinalizeWriteParallelism());
    } catch (FileNotFoundException fnf) {
      LOG.warn("Rollback never failed and hence no marker dir was found. Safely moving on");
    } catch (IOException e) {
      throw new HoodieRollbackException("Failed to list log file markers for previous attempt of rollback ", e);
    }

    List<Pair<String, HoodieRollbackStat>> getRollbackStats = maybeDeleteAndCollectStats(context, instantTime, instantToRollback, serializableRequests, true, parallelism,
        logPaths);
    List<HoodieRollbackStat> mergedRollbackStatByPartitionPath = context.reduceByKey(getRollbackStats, RollbackUtils::mergeRollbackStat, parallelism);
    return addLogFilesFromPreviousFailedRollbacksToStat(context, mergedRollbackStatByPartitionPath, logPaths);
  }

  /**
   * Collect all file info that needs to be rolled back.
   */
  public List<HoodieRollbackStat> collectRollbackStats(HoodieEngineContext context, String instantTime, HoodieInstant instantToRollback,
                                                       List<HoodieRollbackRequest> rollbackRequests) {
    int parallelism = Math.max(Math.min(rollbackRequests.size(), config.getRollbackParallelism()), 1);
    context.setJobStatus(this.getClass().getSimpleName(), "Collect rollback stats for upgrade/downgrade: " + config.getTableName());
    // If not for conversion to HoodieRollbackInternalRequests, code fails. Using avro model (HoodieRollbackRequest) within spark.parallelize
    // is failing with com.esotericsoftware.kryo.KryoException
    // stack trace: https://gist.github.com/nsivabalan/b6359e7d5038484f8043506c8bc9e1c8
    // related stack overflow post: https://issues.apache.org/jira/browse/SPARK-3601. Avro deserializes list as GenericData.Array.
    List<SerializableHoodieRollbackRequest> serializableRequests = rollbackRequests.stream().map(SerializableHoodieRollbackRequest::new).collect(Collectors.toList());
    return context.reduceByKey(maybeDeleteAndCollectStats(context, instantTime, instantToRollback, serializableRequests, false, parallelism, new HashSet<>()),
        RollbackUtils::mergeRollbackStat, parallelism);
  }

  /**
   * May be delete interested files and collect stats or collect stats only.
   *
   * @param context           instance of {@link HoodieEngineContext} to use.
   * @param instantToRollback {@link HoodieInstant} of interest for which deletion or collect stats is requested.
   * @param rollbackRequests  List of {@link ListingBasedRollbackRequest} to be operated on.
   * @param doDelete          {@code true} if deletion has to be done. {@code false} if only stats are to be collected w/o performing any deletes.
   * @return stats collected with or w/o actual deletions.
   */
  List<Pair<String, HoodieRollbackStat>> maybeDeleteAndCollectStats(HoodieEngineContext context,
                                                                    String instantTime,
                                                                    HoodieInstant instantToRollback,
                                                                    List<SerializableHoodieRollbackRequest> rollbackRequests,
                                                                    boolean doDelete, int numPartitions,
                                                                    Set<String> additionalLogFiles) {
    return context.flatMap(rollbackRequests, (SerializableFunction<SerializableHoodieRollbackRequest, Stream<Pair<String, HoodieRollbackStat>>>) rollbackRequest -> {
      List<String> filesToBeDeleted = rollbackRequest.getFilesToBeDeleted();
      if (!filesToBeDeleted.isEmpty()) {
        List<HoodieRollbackStat> rollbackStats = deleteFiles(metaClient, filesToBeDeleted, doDelete);
        List<Pair<String, HoodieRollbackStat>> partitionToRollbackStats = new ArrayList<>();
        rollbackStats.forEach(entry -> partitionToRollbackStats.add(Pair.of(entry.getPartitionPath(), entry)));
        return partitionToRollbackStats.stream();
      } else if (!rollbackRequest.getLogBlocksToBeDeleted().isEmpty()) {
        HoodieLogFormat.Writer writer = null;
        final Path filePath;
        try {
          String partitionPath = rollbackRequest.getPartitionPath();
          String fileId = rollbackRequest.getFileId();
          String latestBaseInstant = rollbackRequest.getLatestBaseInstant();

          // Let's emit markers for rollback as well
          WriteMarkers writeMarkers = WriteMarkersFactory.get(config.getMarkersType(), table, instantTime);

          writer = HoodieLogFormat.newWriterBuilder()
              .onParentPath(FSUtils.getPartitionPath(metaClient.getBasePath(), rollbackRequest.getPartitionPath()))
              .withFileId(fileId)
              .overBaseCommit(latestBaseInstant)
              .withFs(metaClient.getFs())
              .withLogWriteCallback(getRollbackLogMarkerCallback(writeMarkers, partitionPath, fileId))
              .withFileExtension(HoodieLogFile.DELTA_EXTENSION).build();

          // generate metadata
          if (doDelete) {
            Map<HoodieLogBlock.HeaderMetadataType, String> header = generateHeader(instantToRollback.getTimestamp());
            // if update belongs to an existing log file
            // use the log file path from AppendResult in case the file handle may roll over
            filePath = writer.appendBlock(new HoodieCommandBlock(header)).logFile().getPath();
          } else {
            filePath = writer.getLogFile().getPath();
          }
        } catch (IOException | InterruptedException io) {
          throw new HoodieRollbackException("Failed to rollback for instant " + instantToRollback, io);
        } finally {
          try {
            if (writer != null) {
              writer.close();
            }
          } catch (IOException io) {
            throw new HoodieIOException("Error appending rollback block", io);
          }
        }

        // This step is intentionally done after writer is closed. Guarantees that
        // getFileStatus would reflect correct stats and FileNotFoundException is not thrown in
        // cloud-storage : HUDI-168
        Map<FileStatus, Long> filesToNumBlocksRollback = Collections.singletonMap(
            metaClient.getFs().getFileStatus(Objects.requireNonNull(filePath)),
            1L
        );

        return Collections.singletonList(
                Pair.of(rollbackRequest.getPartitionPath(),
                    HoodieRollbackStat.newBuilder()
                        .withPartitionPath(rollbackRequest.getPartitionPath())
                        .withRollbackBlockAppendResults(filesToNumBlocksRollback)
                        .withLogFilesFromFailedCommit(rollbackRequest.getLogBlocksToBeDeleted())
                        .build()))
            .stream();
      } else {
        return Collections.singletonList(
                Pair.of(rollbackRequest.getPartitionPath(),
                    HoodieRollbackStat.newBuilder()
                        .withPartitionPath(rollbackRequest.getPartitionPath())
                        .build()))
            .stream();
      }
    }, numPartitions);
  }

  private HoodieLogFileWriteCallback getRollbackLogMarkerCallback(final WriteMarkers writeMarkers, String partitionPath, String fileId) {
    return new HoodieLogFileWriteCallback() {
      @Override
      public boolean preLogFileOpen(HoodieLogFile logFileToAppend) {
        // there may be existed marker file if fs support append. So always return true;
        createAppendMarker(logFileToAppend);
        return true;
      }

      @Override
      public boolean preLogFileCreate(HoodieLogFile logFileToCreate) {
        return createAppendMarker(logFileToCreate);
      }

      private boolean createAppendMarker(HoodieLogFile logFileToAppend) {
        return writeMarkers.createIfNotExists(partitionPath, logFileToAppend.getFileName(), IOType.APPEND,
            config, fileId, metaClient.getActiveTimeline()).isPresent();
      }
    };
  }

  private List<HoodieRollbackStat> addLogFilesFromPreviousFailedRollbacksToStat(HoodieEngineContext context,
                                                                                List<HoodieRollbackStat> originalRollbackStats,
                                                                                Set<String> logPaths) {
    if (logPaths.isEmpty()) {
      // if rollback is not failed and re-attempted, we should not find any additional log files here.
      return originalRollbackStats;
    }

    final Path basePath = new Path(config.getBasePath());
    Map<String, List<String>> partitionPathToLogFiles = new HashMap<>();
    logPaths
        .stream()
        .map(logFilePath -> new Path(config.getBasePath(), logFilePath))
        .forEach(fullFilePath -> {
          String partitionPath = FSUtils.getRelativePartitionPath(basePath, fullFilePath.getParent());
          if (!partitionPathToLogFiles.containsKey(partitionPath)) {
            partitionPathToLogFiles.put(partitionPath, new ArrayList<>());
          }
          partitionPathToLogFiles.get(partitionPath).add(fullFilePath.getName());
        });

    // populate partitionPath -> List<log file name>
    List<Map.Entry<String, List<String>>> perPartitionLogFileMap = partitionPathToLogFiles.entrySet().stream().collect(Collectors.toList());
    HoodiePairData<String, List<String>> partitionPathToLogFilesHoodieData = context.parallelize(perPartitionLogFileMap).mapToPair(
        (SerializablePairFunction<Map.Entry<String, List<String>>, String, List<String>>) t -> Pair.of(t.getKey(), t.getValue()));

    // populate partitionPath -> HoodieRollbackStat
    List<Pair<String, HoodieRollbackStat>> partitionPathToRollbackStat = originalRollbackStats.stream().map(rollbackStat -> {
      return Pair.of(rollbackStat.getPartitionPath(), rollbackStat);
    }).collect(Collectors.toList());

    HoodiePairData<String, HoodieRollbackStat> partitionPathToRollbackStatsHoodieData = context.parallelize(partitionPathToRollbackStat).mapToPair(
        (SerializablePairFunction<Pair<String, HoodieRollbackStat>, String, HoodieRollbackStat>) t -> t);

    SerializableConfiguration serializableConfiguration = new SerializableConfiguration(table.getHadoopConf());

    // lets do left outer join and append missing log files to HoodieRollbackStat for each partition path.
    List<HoodieRollbackStat> finalRollbackStats = partitionPathToRollbackStatsHoodieData.leftOuterJoin(partitionPathToLogFilesHoodieData)
        .map((SerializableFunction<Pair<String, Pair<HoodieRollbackStat, Option<List<String>>>>, HoodieRollbackStat>) v1 -> {
          if (v1.getValue().getValue().isPresent()) {
            Path partitionPath = new Path(v1.getKey());
            HoodieRollbackStat rollbackStat = v1.getValue().getKey();
            List<String> missingLogFiles = v1.getValue().getRight().get();

            // fetch file sizes.
            FileSystem fs = partitionPath.getFileSystem(serializableConfiguration.get());
            Path fullPartitionPath = new Path(config.getBasePath(), partitionPath);
            List<Option<FileStatus>> fileStatusesOpt = FSUtils.getFileStatusesUnderPartition(fs,
                fullPartitionPath, missingLogFiles, true);
            List<FileStatus> fileStatuses = fileStatusesOpt.stream().filter(fileStatusOption -> fileStatusOption.isPresent())
                .map(fileStatusOption -> fileStatusOption.get()).collect(Collectors.toList());

            HashMap<FileStatus, Long> commandBlocksCount = new HashMap<>(rollbackStat.getCommandBlocksCount());
            fileStatuses.forEach(fileStatus -> commandBlocksCount.put(fileStatus, fileStatus.getLen()));

            return new HoodieRollbackStat(rollbackStat.getPartitionPath(), rollbackStat.getSuccessDeleteFiles(), rollbackStat.getFailedDeleteFiles(),
                commandBlocksCount, rollbackStat.getLogFilesFromFailedCommit());
          } else {
            return v1.getValue().getKey();
          }
        }).collectAsList();
    return finalRollbackStats;
  }

  /**
   * Common method used for cleaning out files during rollback.
   */
  protected List<HoodieRollbackStat> deleteFiles(HoodieTableMetaClient metaClient, List<String> filesToBeDeleted, boolean doDelete) throws IOException {
    return filesToBeDeleted.stream().map(fileToDelete -> {
      String basePath = metaClient.getBasePath();
      try {
        Path fullDeletePath = new Path(fileToDelete);
        String partitionPath = FSUtils.getRelativePartitionPath(new Path(basePath), fullDeletePath.getParent());
        boolean isDeleted = true;
        if (doDelete) {
          try {
            isDeleted = metaClient.getFs().delete(fullDeletePath);
          } catch (FileNotFoundException e) {
            // if first rollback attempt failed and retried again, chances that some files are already deleted.
            isDeleted = true;
          }
        }
        return HoodieRollbackStat.newBuilder()
            .withPartitionPath(partitionPath)
            .withDeletedFileResult(fullDeletePath.toString(), isDeleted)
            .build();
      } catch (IOException e) {
        LOG.error("Fetching file status for ");
        throw new HoodieIOException("Fetching file status for " + fileToDelete + " failed ", e);
      }
    }).collect(Collectors.toList());
  }

  protected Map<HoodieLogBlock.HeaderMetadataType, String> generateHeader(String commit) {
    // generate metadata
    Map<HoodieLogBlock.HeaderMetadataType, String> header = new HashMap<>(3);
    header.put(HoodieLogBlock.HeaderMetadataType.INSTANT_TIME, metaClient.getActiveTimeline().lastInstant().get().getTimestamp());
    header.put(HoodieLogBlock.HeaderMetadataType.TARGET_INSTANT_TIME, commit);
    header.put(HoodieLogBlock.HeaderMetadataType.COMMAND_BLOCK_TYPE,
        String.valueOf(HoodieCommandBlock.HoodieCommandBlockTypeEnum.ROLLBACK_BLOCK.ordinal()));
    return header;
  }
}
