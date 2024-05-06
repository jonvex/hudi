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

package org.apache.hudi.common.testutils.reader;

import org.apache.hudi.avro.HoodieAvroWriteSupport;
import org.apache.hudi.common.bloom.BloomFilter;
import org.apache.hudi.common.bloom.BloomFilterFactory;
import org.apache.hudi.common.bloom.BloomFilterTypeCode;
import org.apache.hudi.common.config.HoodieReaderConfig;
import org.apache.hudi.common.config.TypedProperties;
import org.apache.hudi.common.engine.LocalTaskContextSupplier;
import org.apache.hudi.common.engine.TaskContextSupplier;
import org.apache.hudi.common.model.DeleteRecord;
import org.apache.hudi.common.model.FileSlice;
import org.apache.hudi.common.model.HoodieAvroIndexedRecord;
import org.apache.hudi.common.model.HoodieBaseFile;
import org.apache.hudi.common.model.HoodieFileGroupId;
import org.apache.hudi.common.model.HoodieKey;
import org.apache.hudi.common.model.HoodieLogFile;
import org.apache.hudi.common.model.HoodieRecord;
import org.apache.hudi.common.table.log.HoodieLogFormat;
import org.apache.hudi.common.table.log.block.HoodieAvroDataBlock;
import org.apache.hudi.common.table.log.block.HoodieCDCDataBlock;
import org.apache.hudi.common.table.log.block.HoodieDataBlock;
import org.apache.hudi.common.table.log.block.HoodieDeleteBlock;
import org.apache.hudi.common.table.log.block.HoodieHFileDataBlock;
import org.apache.hudi.common.table.log.block.HoodieLogBlock;
import org.apache.hudi.common.table.log.block.HoodieParquetDataBlock;
import org.apache.hudi.common.testutils.HoodieTestDataGenerator;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.common.util.ReflectionUtils;
import org.apache.hudi.common.util.collection.Pair;
import org.apache.hudi.io.storage.HoodieAvroFileWriter;
import org.apache.hudi.io.storage.HoodieParquetConfig;
import org.apache.hudi.storage.HoodieStorage;
import org.apache.hudi.storage.HoodieStorageUtils;
import org.apache.hudi.storage.StorageConfiguration;
import org.apache.hudi.storage.StoragePath;

import org.apache.avro.Schema;
import org.apache.avro.generic.IndexedRecord;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.io.compress.Compression;
import org.apache.parquet.avro.AvroSchemaConverter;
import org.apache.parquet.hadoop.ParquetWriter;
import org.apache.parquet.hadoop.metadata.CompressionCodecName;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.UUID;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static org.apache.hudi.common.table.log.block.HoodieLogBlock.HoodieLogBlockType.DELETE_BLOCK;
import static org.apache.hudi.common.table.log.block.HoodieLogBlock.HoodieLogBlockType.PARQUET_DATA_BLOCK;
import static org.apache.hudi.common.testutils.FileCreateUtils.baseFileName;
import static org.apache.hudi.common.testutils.FileCreateUtils.logFileName;
import static org.apache.hudi.common.testutils.HoodieTestDataGenerator.AVRO_SCHEMA;
import static org.apache.hudi.common.testutils.reader.DataGenerationPlan.OperationType.DELETE;
import static org.apache.hudi.common.testutils.reader.DataGenerationPlan.OperationType.INSERT;

public class HoodieFileSliceTestUtils {
  public static final String FORWARD_SLASH = "/";
  public static final String PARQUET = ".parquet";

  public static final String DRIVER = "driver";
  public static final String PARTITION_PATH = "partition_path";
  public static final String RIDER = "rider";
  public static final String ROW_KEY = "_row_key";
  public static final String TIMESTAMP = "timestamp";
  public static final HoodieTestDataGenerator DATA_GEN =
      new HoodieTestDataGenerator(0xDEED);
  public static final TypedProperties PROPERTIES = new TypedProperties();

  static {
    PROPERTIES.setProperty(
        "hoodie.datasource.write.precombine.field", "timestamp");
  }

  // We use a number to represent a record key, and a (start, end) range
  // to represent a set of record keys between start <= k <= end.
  public static class KeyRange {
    public int start;
    public int end;

    public KeyRange(int start, int end) {
      this.start = start;
      this.end = end;
    }
  }

  private static Path generateBaseFilePath(
      String basePath,
      String fileId,
      String instantTime
  ) {
    return new Path(
        basePath + FORWARD_SLASH
        + baseFileName(instantTime, fileId, PARQUET));
  }

  private static Path generateLogFilePath(
      String basePath,
      String fileId,
      String instantTime,
      int version) {
    return new Path(
        basePath + FORWARD_SLASH + logFileName(
        instantTime, fileId, version));
  }

  // Note:
  // "start < end" means start <= k <= end.
  // "start == end" means k = start.
  // "start > end" means no keys.
  private static List<String> generateKeys(KeyRange range) {
    List<String> keys = new ArrayList<>();
    if (range.start == range.end) {
      keys.add(String.valueOf(range.start));
    } else {
      keys = IntStream
          .rangeClosed(range.start, range.end)
          .boxed()
          .map(String::valueOf).collect(Collectors.toList());
    }
    return keys;
  }

  private static List<IndexedRecord> generateRecords(DataGenerationPlan plan) {
    List<IndexedRecord> records = new ArrayList<>();
    List<String> keys = plan.getRecordKeys();
    for (String key : keys) {
      records.add(DATA_GEN.generateGenericRecord(
          key,
          plan.getPartitionPath(),
          RIDER + "." + UUID.randomUUID(),
          DRIVER + "." + UUID.randomUUID(),
          plan.getTimestamp(),
          plan.getOperationType() == DELETE,
          false
      ));
    }
    return records;
  }

  private static HoodieDataBlock getDataBlock(
      HoodieLogBlock.HoodieLogBlockType dataBlockType,
      List<IndexedRecord> records,
      Map<HoodieLogBlock.HeaderMetadataType, String> header,
      StoragePath logFilePath,
      HoodieStorage storage
  ) {
    return createDataBlock(
        dataBlockType,
        records.stream().map(HoodieAvroIndexedRecord::new)
            .collect(Collectors.toList()),
        header,
        logFilePath, storage);
  }

  private static HoodieDataBlock createDataBlock(
      HoodieLogBlock.HoodieLogBlockType dataBlockType,
      List<HoodieRecord> records,
      Map<HoodieLogBlock.HeaderMetadataType, String> header,
      StoragePath pathForReader,
      HoodieStorage storage
  ) {
    switch (dataBlockType) {
      case CDC_DATA_BLOCK:
        return new HoodieCDCDataBlock(
            records,
            header,
            HoodieRecord.RECORD_KEY_METADATA_FIELD);
      case AVRO_DATA_BLOCK:
        return new HoodieAvroDataBlock(
            records,
            false,
            header,
            HoodieRecord.RECORD_KEY_METADATA_FIELD);
      case HFILE_DATA_BLOCK:
        return new HoodieHFileDataBlock(
            records,
            header,
            Compression.Algorithm.GZ,
            pathForReader,
            HoodieReaderConfig.USE_NATIVE_HFILE_READER.defaultValue(),
            storage);
      case PARQUET_DATA_BLOCK:
        return new HoodieParquetDataBlock(
            records,
            false,
            header,
            HoodieRecord.RECORD_KEY_METADATA_FIELD,
            CompressionCodecName.GZIP,
            0.1,
            true,
            storage);
      default:
        throw new RuntimeException(
            "Unknown data block type " + dataBlockType);
    }
  }

  public static HoodieDeleteBlock getDeleteBlock(
      List<IndexedRecord> records,
      Map<HoodieLogBlock.HeaderMetadataType, String> header,
      Schema schema,
      Properties props
  ) {
    List<HoodieRecord> hoodieRecords = records.stream()
        .map(r -> {
          String rowKey = (String) r.get(r.getSchema().getField(ROW_KEY).pos());
          String partitionPath = (String) r.get(r.getSchema().getField(PARTITION_PATH).pos());
          return new HoodieAvroIndexedRecord(new HoodieKey(rowKey, partitionPath), r);
        })
        .collect(Collectors.toList());
    return new HoodieDeleteBlock(
        hoodieRecords.stream().map(
            r -> Pair.of(DeleteRecord.create(
                r.getKey(), r.getOrderingValue(schema, props)), -1L))
            .collect(Collectors.toList()),
        false,
        header
    );
  }

  public static HoodieBaseFile createBaseFile(
      String baseFilePath,
      List<IndexedRecord> records,
      Schema schema,
      String baseInstantTime
  ) throws IOException {
    StorageConfiguration conf = HoodieStorageUtils.getNewStorageConf();

    // TODO: Optimize these hard-coded parameters for test purpose. (HUDI-7214)
    BloomFilter filter = BloomFilterFactory.createBloomFilter(
        1000,
        0.0001,
        10000,
        BloomFilterTypeCode.DYNAMIC_V0.name());
    HoodieAvroWriteSupport<IndexedRecord> writeSupport = new HoodieAvroWriteSupport<>(
        new AvroSchemaConverter().convert(schema),
        schema,
        Option.of(filter),
        new Properties());
    HoodieParquetConfig<HoodieAvroWriteSupport> parquetConfig = new HoodieParquetConfig(
        writeSupport,
        CompressionCodecName.GZIP,
        ParquetWriter.DEFAULT_BLOCK_SIZE,
        ParquetWriter.DEFAULT_PAGE_SIZE,
        1024 * 1024 * 1024,
        conf,
        0.1,
        true);

    try (HoodieAvroFileWriter writer = (HoodieAvroFileWriter) ReflectionUtils.loadClass("org.apache.hudi.io.storage.HoodieAvroParquetWriter",
        new Class<?>[] {StoragePath.class, HoodieParquetConfig.class, String.class, TaskContextSupplier.class, boolean.class},
        new StoragePath(baseFilePath),
        parquetConfig,
        baseInstantTime,
        new LocalTaskContextSupplier(),
        true)) {
      for (IndexedRecord record : records) {
        writer.writeAvro(
            (String) record.get(schema.getField(ROW_KEY).pos()), record);
      }
    }
    return new HoodieBaseFile(baseFilePath);
  }

  public static HoodieLogFile createLogFile(
      HoodieStorage storage,
      String logFilePath,
      List<IndexedRecord> records,
      Schema schema,
      String fileId,
      String logInstantTime,
      int version,
      HoodieLogBlock.HoodieLogBlockType blockType
  ) throws InterruptedException, IOException {
    try (HoodieLogFormat.Writer writer =
             HoodieLogFormat.newWriterBuilder()
                 .onParentPath(new StoragePath(logFilePath).getParent())
                 .withFileExtension(HoodieLogFile.DELTA_EXTENSION)
                 .withFileId(fileId)
                 .withDeltaCommit(logInstantTime)
                 .withLogVersion(version)
                 .withStorage(storage).build()) {
      Map<HoodieLogBlock.HeaderMetadataType, String> header = new HashMap<>();
      header.put(HoodieLogBlock.HeaderMetadataType.INSTANT_TIME, logInstantTime);
      header.put(HoodieLogBlock.HeaderMetadataType.SCHEMA, schema.toString());

      if (blockType != DELETE_BLOCK) {
        HoodieDataBlock dataBlock = getDataBlock(
            blockType, records, header, new StoragePath(logFilePath), storage);
        writer.appendBlock(dataBlock);
      } else {
        HoodieDeleteBlock deleteBlock = getDeleteBlock(
            records, header, schema, PROPERTIES);
        writer.appendBlock(deleteBlock);
      }
    }
    return new HoodieLogFile(logFilePath);
  }

  /**
   * Based on provided parameters to generate a {@link FileSlice} object.
   */
  public static FileSlice generateFileSlice(
      HoodieStorage storage,
      String basePath,
      String fileId,
      String partitionPath,
      Schema schema,
      List<DataGenerationPlan> plans
  ) throws IOException, InterruptedException {
    assert (!plans.isEmpty());

    HoodieBaseFile baseFile = null;
    List<HoodieLogFile> logFiles = new ArrayList<>();

    // Generate a base file with records.
    DataGenerationPlan baseFilePlan = plans.get(0);
    if (!baseFilePlan.getRecordKeys().isEmpty()) {
      Path baseFilePath = generateBaseFilePath(
          basePath, fileId, baseFilePlan.getInstantTime());
      List<IndexedRecord> records = generateRecords(baseFilePlan);
      baseFile = createBaseFile(
          baseFilePath.toString(),
          records,
          schema,
          baseFilePlan.getInstantTime());
    }

    // Rest of plans are for log files.
    for (int i = 1; i < plans.size(); i++) {
      DataGenerationPlan logFilePlan = plans.get(i);
      if (logFilePlan.getRecordKeys().isEmpty()) {
        continue;
      }

      Path logFile = generateLogFilePath(
          basePath,fileId, logFilePlan.getInstantTime(), i);
      List<IndexedRecord> records = generateRecords(logFilePlan);
      HoodieLogBlock.HoodieLogBlockType blockType =
          logFilePlan.getOperationType() == DELETE ? DELETE_BLOCK : PARQUET_DATA_BLOCK;
      logFiles.add(createLogFile(
          storage,
          logFile.toString(),
          records,
          schema,
          fileId,
          logFilePlan.getInstantTime(),
          i,
          blockType));
    }

    // Assemble the FileSlice finally.
    HoodieFileGroupId fileGroupId = new HoodieFileGroupId(partitionPath, fileId);
    String baseInstantTime = baseFile == null ? null : baseFile.getCommitTime();
    return new FileSlice(fileGroupId, baseInstantTime, baseFile, logFiles);
  }

  /**
   * Generate a {@link FileSlice} object which contains a {@link HoodieBaseFile} only.
   */
  public static Option<FileSlice> getBaseFileOnlyFileSlice(
      HoodieStorage storage,
      KeyRange range,
      long timestamp,
      String basePath,
      String partitionPath,
      String fileId,
      String baseInstantTime
  ) throws IOException, InterruptedException {
    List<String> keys = generateKeys(range);
    List<DataGenerationPlan> plans = new ArrayList<>();
    DataGenerationPlan baseFilePlan = DataGenerationPlan
        .newBuilder()
        .withRecordKeys(keys)
        .withOperationType(INSERT)
        .withPartitionPath(partitionPath)
        .withTimeStamp(timestamp)
        .withInstantTime(baseInstantTime)
        .build();
    plans.add(baseFilePlan);

    return Option.of(generateFileSlice(
        storage,
        basePath,
        fileId,
        partitionPath,
        AVRO_SCHEMA,
        plans));
  }

  /**
   * Generate a regular {@link FileSlice} containing both a base file and a number of log files.
   */
  public static Option<FileSlice> getFileSlice(
      HoodieStorage storage,
      List<KeyRange> ranges,
      List<Long> timestamps,
      List<DataGenerationPlan.OperationType> operationTypes,
      List<String> instantTimes,
      String basePath,
      String partitionPath,
      String fileId
  ) throws IOException, InterruptedException {
    List<DataGenerationPlan> plans = new ArrayList<>();
    for (int i = 0; i < ranges.size(); i++) {
      List<String> keys = generateKeys(ranges.get(i));
      plans.add(DataGenerationPlan.newBuilder()
          .withOperationType(operationTypes.get(i))
          .withPartitionPath(partitionPath)
          .withRecordKeys(keys)
          .withTimeStamp(timestamps.get(i))
          .withInstantTime(instantTimes.get(i))
          .build());
    }

    return Option.of(generateFileSlice(
        storage,
        basePath,
        fileId,
        partitionPath,
        AVRO_SCHEMA,
        plans));
  }
}
