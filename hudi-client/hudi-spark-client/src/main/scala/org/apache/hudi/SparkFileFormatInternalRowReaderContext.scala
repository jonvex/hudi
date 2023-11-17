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

package org.apache.hudi

import org.apache.avro.Schema
import org.apache.avro.generic.IndexedRecord
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.Path
import org.apache.hudi.avro.HoodieAvroUtils
import org.apache.hudi.common.engine.HoodieReaderContext
import org.apache.hudi.common.fs.FSUtils
import org.apache.hudi.common.util.ValidationUtils.checkState
import org.apache.hudi.common.util.collection.{ClosableIterator, CloseableMappingIterator}
import org.apache.hudi.io.storage.{HoodieSparkFileReaderFactory, HoodieSparkParquetReader}
import org.apache.hudi.util.CloseableInternalRowIterator
import org.apache.spark.sql.{HoodieCatalystExpressionUtils, HoodieInternalRowUtils}
import org.apache.spark.sql.avro.HoodieAvroDeserializer
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.{JoinedRow, UnsafeProjection, UnsafeRow}
import org.apache.spark.sql.execution.datasources.PartitionedFile
import org.apache.spark.sql.execution.datasources.parquet.ParquetFileFormat
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.vectorized.{ColumnVector, ColumnarBatch}

import java.util.function.UnaryOperator
import scala.collection.mutable

/**
 * Implementation of {@link HoodieReaderContext} to read {@link InternalRow}s with
 * {@link ParquetFileFormat} on Spark.
 *
 * This uses Spark parquet reader to read parquet data files or parquet log blocks.
 *
 * @param baseFileReader  A reader that transforms a {@link PartitionedFile} to an iterator of
 *                        {@link InternalRow}. This is required for reading the base file and
 *                        not required for reading a file group with only log files.
 * @param partitionValues The values for a partition in which the file group lives.
 */
class SparkFileFormatInternalRowReaderContext(readerMaps: mutable.Map[Long, PartitionedFile => Iterator[InternalRow]],
                                              partitionValues: InternalRow) extends BaseSparkInternalRowReaderContext {
  lazy val sparkAdapter = SparkAdapterSupport.sparkAdapter
  lazy val sparkFileReaderFactory = new HoodieSparkFileReaderFactory
  val deserializerMap: mutable.Map[Schema, HoodieAvroDeserializer] = mutable.Map()

  override def getFileRecordIterator(filePath: Path,
                                     start: Long,
                                     length: Long,
                                     dataSchema: Schema,
                                     requiredSchema: Schema,
                                     conf: Configuration): ClosableIterator[InternalRow] = {
    val fileInfo = sparkAdapter.getSparkPartitionedFileUtils
      .createPartitionedFile(partitionValues, filePath, start, length)
    if (FSUtils.isLogFile(filePath)) {
      val structType: StructType = HoodieInternalRowUtils.getCachedSchema(requiredSchema)
      val projection: UnsafeProjection = HoodieInternalRowUtils.getCachedUnsafeProjection(structType, structType)
      new CloseableMappingIterator[InternalRow, UnsafeRow](
        sparkFileReaderFactory.newParquetFileReader(conf, filePath).asInstanceOf[HoodieSparkParquetReader]
          .getInternalRowIterator(dataSchema, requiredSchema),
        new java.util.function.Function[InternalRow, UnsafeRow] {
          override def apply(data: InternalRow): UnsafeRow = {
            // NOTE: We have to do [[UnsafeProjection]] of incoming [[InternalRow]] to convert
            //       it to [[UnsafeRow]] holding just raw bytes
            projection.apply(data)
          }
        }).asInstanceOf[ClosableIterator[InternalRow]]
    } else {
      val key = generateKey(dataSchema, requiredSchema)
      if (!readerMaps.contains(key)) {
        throw new IllegalStateException("schemas don't hash to a known reader")
      }
      new CloseableInternalRowIterator(readerMaps(key).apply(fileInfo))
    }
  }

  private def generateKey(dataSchema: Schema, requestedSchema: Schema): Long = {
    dataSchema.hashCode() + requestedSchema.hashCode()
  }

  /**
   * Converts an Avro record, e.g., serialized in the log files, to an [[InternalRow]].
   *
   * @param avroRecord The Avro record.
   * @return An [[InternalRow]].
   */
  override def convertAvroRecord(avroRecord: IndexedRecord): InternalRow = {
    val schema = avroRecord.getSchema
    val structType = HoodieInternalRowUtils.getCachedSchema(schema)
    val deserializer = deserializerMap.getOrElseUpdate(schema, {
      sparkAdapter.createAvroDeserializer(schema, structType)
    })
    deserializer.deserialize(avroRecord).get.asInstanceOf[InternalRow]
  }

  override def mergeBootstrapReaders(skeletonFileIterator: ClosableIterator[InternalRow],
                                     dataFileIterator: ClosableIterator[InternalRow]): ClosableIterator[InternalRow] = {
    doBootstrapMerge(skeletonFileIterator.asInstanceOf[ClosableIterator[Any]],
      dataFileIterator.asInstanceOf[ClosableIterator[Any]])
  }


  protected def doBootstrapMerge(skeletonFileIterator: ClosableIterator[Any], dataFileIterator: ClosableIterator[Any]): ClosableIterator[InternalRow] = {
    new ClosableIterator[Any] {
      val combinedRow = new JoinedRow()

      override def hasNext: Boolean = {
        checkState(dataFileIterator.hasNext == skeletonFileIterator.hasNext,
          "Bootstrap data-file iterator and skeleton-file iterator have to be in-sync!")
        dataFileIterator.hasNext && skeletonFileIterator.hasNext
      }

      override def next(): Any = {
        (skeletonFileIterator.next(), dataFileIterator.next()) match {
          case (s: ColumnarBatch, d: ColumnarBatch) =>
            val numCols = s.numCols() + d.numCols()
            val vecs: Array[ColumnVector] = new Array[ColumnVector](numCols)
            for (i <- 0 until numCols) {
              if (i < s.numCols()) {
                vecs(i) = s.column(i)
              } else {
                vecs(i) = d.column(i - s.numCols())
              }
            }
            assert(s.numRows() == d.numRows())
            sparkAdapter.makeColumnarBatch(vecs, s.numRows())
          case (_: ColumnarBatch, _: InternalRow) => throw new IllegalStateException("InternalRow ColumnVector mismatch")
          case (_: InternalRow, _: ColumnarBatch) => throw new IllegalStateException("InternalRow ColumnVector mismatch")
          case (s: InternalRow, d: InternalRow) => combinedRow(s, d)
        }
      }

      override def close(): Unit = {
        skeletonFileIterator.close()
        dataFileIterator.close()
      }
    }.asInstanceOf[ClosableIterator[InternalRow]]
  }

  override def projectRecord(from: Schema, to: Schema): UnaryOperator[InternalRow] = {
        val projection = HoodieCatalystExpressionUtils.generateUnsafeProjection(AvroConversionUtils.convertAvroSchemaToStructType(from),
          AvroConversionUtils.convertAvroSchemaToStructType(to))
    u: InternalRow => projection(u)
  }
}
