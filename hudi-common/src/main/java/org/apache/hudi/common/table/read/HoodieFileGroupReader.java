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

package org.apache.hudi.common.table.read;

import org.apache.hudi.common.config.HoodieMemoryConfig;
import org.apache.hudi.common.config.HoodieReaderConfig;
import org.apache.hudi.common.config.TypedProperties;
import org.apache.hudi.common.engine.HoodieReaderContext;
import org.apache.hudi.common.model.BaseFile;
import org.apache.hudi.common.model.FileSlice;
import org.apache.hudi.common.model.HoodieBaseFile;
import org.apache.hudi.common.model.HoodieLogFile;
import org.apache.hudi.common.model.HoodieRecord;
import org.apache.hudi.common.model.HoodieRecordMerger;
import org.apache.hudi.common.table.HoodieTableConfig;
import org.apache.hudi.common.table.log.HoodieMergedLogRecordReader;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.common.util.collection.CachingIterator;
import org.apache.hudi.common.util.collection.ClosableIterator;
import org.apache.hudi.common.util.collection.EmptyIterator;
import org.apache.hudi.common.util.collection.Pair;
import org.apache.hudi.exception.HoodieIOException;

import org.apache.avro.Schema;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import java.io.Closeable;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.function.UnaryOperator;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static org.apache.hudi.avro.AvroSchemaUtils.appendFieldsToSchemaDedupNested;
import static org.apache.hudi.avro.AvroSchemaUtils.findNestedField;
import static org.apache.hudi.common.fs.FSUtils.getRelativePartitionPath;
import static org.apache.hudi.common.util.ConfigUtils.getBooleanWithAltKeys;
import static org.apache.hudi.common.util.ConfigUtils.getIntWithAltKeys;

/**
 * A file group reader that iterates through the records in a single file group.
 * <p>
 * This should be used by the every engine integration, by plugging in a
 * {@link HoodieReaderContext<T>} implementation.
 *
 * @param <T> The type of engine-specific record representation, e.g.,{@code InternalRow}
 *            in Spark and {@code RowData} in Flink.
 */
public final class HoodieFileGroupReader<T> implements Closeable {
  private final HoodieReaderContext<T> readerContext;
  private final Option<HoodieBaseFile> hoodieBaseFileOption;
  private final List<HoodieLogFile> logFiles;
  private final Configuration hadoopConf;
  private final TypedProperties props;
  // Byte offset to start reading from the base file
  private final long start;
  // Length of bytes to read from the base file
  private final long length;
  // Core structure to store and process records.
  private final HoodieFileGroupRecordBuffer<T> recordBuffer;
  private final HoodieFileGroupReaderState readerState = new HoodieFileGroupReaderState();
  private ClosableIterator<T> baseFileIterator;
  private HoodieRecordMerger recordMerger;

  private final Schema dataSchema;

  // requestedSchema: the schema that the caller requests
  private final Schema requestedSchema;

  // requiredSchema: the requestedSchema with any additional columns required for merging etc
  private final Schema requiredSchema;

  private final HoodieTableConfig hoodieTableConfig;

  private final Option<UnaryOperator<T>> outputConverter;

  private boolean needMORMerge;
  private boolean needMerge;

  private final boolean shouldMergeWithPosition;

  public HoodieFileGroupReader(HoodieReaderContext<T> readerContext,
                               Configuration hadoopConf,
                               String tablePath,
                               String latestCommitTime,
                               FileSlice fileSlice,
                               Schema dataSchema,
                               Schema requestedSchema,
                               TypedProperties props,
                               HoodieTableConfig tableConfig,
                               long start,
                               long length,
                               boolean shouldUseRecordPosition) {
    this.readerContext = readerContext;
    this.hadoopConf = hadoopConf;
    this.hoodieBaseFileOption = fileSlice.getBaseFile();
    this.logFiles = fileSlice.getLogFiles().sorted(HoodieLogFile.getLogFileComparator()).collect(Collectors.toList());
    this.props = props;
    this.start = start;
    this.length = length;
    this.recordMerger = readerContext.getRecordMerger(tableConfig.getRecordMergerStrategy());
    this.readerState.tablePath = tablePath;
    this.readerState.latestCommitTime = latestCommitTime;
    this.dataSchema = dataSchema;
    this.requestedSchema = requestedSchema;
    this.hoodieTableConfig = tableConfig;
    this.shouldMergeWithPosition = shouldUseRecordPosition && readerContext.shouldUseRecordPositionMerging();
    this.requiredSchema = prepareSchema();
    if (!requestedSchema.equals(requiredSchema)) {
      this.outputConverter = Option.of(readerContext.projectRecord(requiredSchema, requestedSchema));
    } else {
      this.outputConverter = Option.empty();
    }
    this.readerState.baseFileAvroSchema = requiredSchema;
    this.readerState.logRecordAvroSchema = requiredSchema;
    this.readerState.mergeProps.putAll(props);
    this.recordBuffer = this.logFiles.isEmpty()
        ? null
        : this.shouldMergeWithPosition
        ? new HoodiePositionBasedFileGroupRecordBuffer<>(
        readerContext, requiredSchema, requiredSchema, Option.empty(), Option.empty(),
        recordMerger, props)
        : new HoodieKeyBasedFileGroupRecordBuffer<>(
        readerContext, requiredSchema, requiredSchema, Option.empty(), Option.empty(),
        recordMerger, props);
  }

  /**
   * Initialize internal iterators on the base and log files.
   */
  public void initRecordIterators() throws IOException {
    ClosableIterator<T> iter = makeBaseFileIterator();
    if (logFiles.isEmpty()) {
      this.baseFileIterator = CachingIterator.wrap(iter, readerContext);
    } else {
      this.baseFileIterator = iter;
      scanLogFiles();
      recordBuffer.setBaseFileIterator(baseFileIterator);
    }
  }

  private ClosableIterator<T> makeBaseFileIterator() throws IOException {
    if (!hoodieBaseFileOption.isPresent()) {
      return new EmptyIterator<>();
    }

    HoodieBaseFile baseFile = hoodieBaseFileOption.get();
    if (baseFile.getBootstrapBaseFile().isPresent()) {
      return makeBootstrapBaseFileIterator(baseFile);
    }

    return readerContext.getFileRecordIterator(baseFile.getHadoopPath(), start, length,
         dataSchema, requiredSchema, hadoopConf, this.needMerge);
  }

  private Schema generateRequiredSchema() {
    //might need to change this if other queries than mor have mandatory fields
    if (!needMORMerge) {
      return requestedSchema;
    }

    List<Schema.Field> addedFields = new ArrayList<>();
    for (String field : recordMerger.getMandatoryFieldsForMerging(hoodieTableConfig)) {
      if (!findNestedField(requestedSchema, field).isPresent()) {
        Option<Schema.Field> foundFieldOpt  = findNestedField(dataSchema, field);
        if (!foundFieldOpt.isPresent()) {
          throw new IllegalArgumentException("Field: " + field + " does not exist in the table schema");
        }
        Schema.Field foundField = foundFieldOpt.get();
        addedFields.add(foundField);
      }
    }

    if (addedFields.isEmpty()) {
      return requestedSchema;
    }

    return appendFieldsToSchemaDedupNested(requestedSchema, addedFields);
  }

  private Schema prepareSchema() {
    this.needMORMerge = !logFiles.isEmpty();
    Schema preReorderRequiredSchema = generateRequiredSchema();
    Pair<List<Schema.Field>, List<Schema.Field>> requiredFields = getDataAndMetaCols(preReorderRequiredSchema);
    boolean needBootstrapMerge = hoodieBaseFileOption.isPresent() && hoodieBaseFileOption.get().getBootstrapBaseFile().isPresent()
        && !requiredFields.getLeft().isEmpty() && !requiredFields.getRight().isEmpty();
    this.needMerge = needBootstrapMerge || this.needMORMerge;
    Schema preMergeSchema = needBootstrapMerge
        ? createSchemaFromFields(Stream.concat(requiredFields.getLeft().stream(), requiredFields.getRight().stream()).collect(Collectors.toList()))
        : preReorderRequiredSchema;
    return this.shouldMergeWithPosition && this.needMerge
        ? addPositionalMergeCol(preMergeSchema)
        : preMergeSchema;
  }

  private Schema addPositionalMergeCol(Schema input) {
    return appendFieldsToSchemaDedupNested(input, Collections.singletonList(getPositionalMergeField()));
  }

  private Schema.Field getPositionalMergeField() {
    return new Schema.Field(HoodiePositionBasedFileGroupRecordBuffer.ROW_INDEX_TEMPORARY_COLUMN_NAME,
        Schema.create(Schema.Type.LONG), "", 0L);
  }

  private static Pair<List<Schema.Field>, List<Schema.Field>> getDataAndMetaCols(Schema schema) {
    Map<Boolean, List<Schema.Field>> fieldsByMeta = schema.getFields().stream()
        //if there are no data fields, then we don't want to think the temp col is a data col
        .filter(f -> !Objects.equals(f.name(), HoodiePositionBasedFileGroupRecordBuffer.ROW_INDEX_TEMPORARY_COLUMN_NAME))
        .collect(Collectors.partitioningBy(f -> HoodieRecord.HOODIE_META_COLUMNS_WITH_OPERATION.contains(f.name())));
    return Pair.of(fieldsByMeta.getOrDefault(true, Collections.emptyList()),
        fieldsByMeta.getOrDefault(false, Collections.emptyList()));
  }

  private Schema createSchemaFromFields(List<Schema.Field> fields) {
    //fields have positions set, so we need to remove them due to avro setFields implementation
    for (int i = 0; i < fields.size(); i++) {
      Schema.Field curr = fields.get(i);
      fields.set(i, new Schema.Field(curr.name(), curr.schema(), curr.doc(), curr.defaultVal()));
    }
    Schema newSchema = Schema.createRecord(dataSchema.getName(), dataSchema.getDoc(), dataSchema.getNamespace(), dataSchema.isError());
    newSchema.setFields(fields);
    return newSchema;
  }

  private ClosableIterator<T> makeBootstrapBaseFileIterator(HoodieBaseFile baseFile) throws IOException {
    BaseFile dataFile = baseFile.getBootstrapBaseFile().get();
    Pair<List<Schema.Field>,List<Schema.Field>> requiredFields = getDataAndMetaCols(requiredSchema);
    Pair<List<Schema.Field>,List<Schema.Field>> allFields = getDataAndMetaCols(dataSchema);
    Option<Pair<ClosableIterator<T>,Schema>> dataFileIterator =
        makeBootstrapBaseFileIteratorHelper(requiredFields.getRight(), allFields.getRight(), dataFile);
    Option<Pair<ClosableIterator<T>,Schema>> skeletonFileIterator =
        makeBootstrapBaseFileIteratorHelper(requiredFields.getLeft(), allFields.getLeft(), baseFile);
    if (!dataFileIterator.isPresent() && !skeletonFileIterator.isPresent()) {
      throw new IllegalStateException("should not be here if only partition cols are required");
    } else if (!dataFileIterator.isPresent()) {
      return skeletonFileIterator.get().getLeft();
    } else if (!skeletonFileIterator.isPresent()) {
      return  dataFileIterator.get().getLeft();
    } else {
      return readerContext.mergeBootstrapReaders(skeletonFileIterator.get().getLeft(), skeletonFileIterator.get().getRight(),
          dataFileIterator.get().getLeft(), dataFileIterator.get().getRight());
    }
  }

  private Option<Pair<ClosableIterator<T>,Schema>> makeBootstrapBaseFileIteratorHelper(List<Schema.Field> requiredFields,
                                                                                       List<Schema.Field> allFields,
                                                                                       BaseFile file) throws IOException {
    if (requiredFields.isEmpty()) {
      return Option.empty();
    }
    if (needMerge && shouldMergeWithPosition) {
      requiredFields.add(getPositionalMergeField());
    }
    Schema requiredSchema = createSchemaFromFields(requiredFields);
    return Option.of(Pair.of(readerContext.getFileRecordIterator(file.getHadoopPath(), 0, file.getFileLen(),
        requiredSchema, createSchemaFromFields(allFields), hadoopConf, this.needMerge), requiredSchema));
  }

  /**
   * @return {@code true} if the next record exists; {@code false} otherwise.
   * @throws IOException on reader error.
   */
  public boolean hasNext() throws IOException {
    if (recordBuffer == null) {
      return baseFileIterator.hasNext();
    } else {
      return recordBuffer.hasNext();
    }
  }

  /**
   * @return The next record after calling {@link #hasNext}.
   */
  public T next() {
    T nextVal = recordBuffer == null ? baseFileIterator.next() : recordBuffer.next();
    if (outputConverter.isPresent()) {
      return outputConverter.get().apply(nextVal);
    }
    return nextVal;
  }

  private void scanLogFiles() {
    String path = readerState.tablePath;
    FileSystem fs = readerContext.getFs(path, hadoopConf);

    HoodieMergedLogRecordReader logRecordReader = HoodieMergedLogRecordReader.newBuilder()
        .withHoodieReaderContext(readerContext)
        .withFileSystem(fs)
        .withBasePath(readerState.tablePath)
        .withLogFiles(logFiles)
        .withLatestInstantTime(readerState.latestCommitTime)
        .withReaderSchema(readerState.logRecordAvroSchema)
        .withReadBlocksLazily(getBooleanWithAltKeys(props, HoodieReaderConfig.COMPACTION_LAZY_BLOCK_READ_ENABLE))
        .withReverseReader(false)
        .withBufferSize(getIntWithAltKeys(props, HoodieMemoryConfig.MAX_DFS_STREAM_BUFFER_SIZE))
        .withPartition(getRelativePartitionPath(
            new Path(readerState.tablePath), logFiles.get(0).getPath().getParent()))
        .withRecordMerger(recordMerger)
        .withRecordBuffer(recordBuffer)
        .build();
    logRecordReader.close();
  }

  @Override
  public void close() throws IOException {
    if (baseFileIterator != null) {
      baseFileIterator.close();
    }
    if (recordBuffer != null) {
      recordBuffer.close();
    }
  }

  public HoodieFileGroupReaderIterator<T> getClosableIterator() {
    return new HoodieFileGroupReaderIterator<>(this);
  }

  public static class HoodieFileGroupReaderIterator<T> implements ClosableIterator<T> {
    private final HoodieFileGroupReader<T> reader;

    public HoodieFileGroupReaderIterator(HoodieFileGroupReader<T> reader) {
      this.reader = reader;
    }

    @Override
    public boolean hasNext() {
      try {
        return reader.hasNext();
      } catch (IOException e) {
        throw new HoodieIOException("Failed to read record", e);
      }
    }

    @Override
    public T next() {
      return reader.next();
    }

    @Override
    public void close() {
      try {
        reader.close();
      } catch (IOException e) {
        throw new HoodieIOException("Failed to close the reader", e);
      }
    }
  }
}
