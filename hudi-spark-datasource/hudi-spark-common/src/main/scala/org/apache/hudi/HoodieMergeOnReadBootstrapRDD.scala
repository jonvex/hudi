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

package org.apache.hudi

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.mapred.JobConf
import org.apache.hudi.HoodieBaseRelation.BaseFileReader
import org.apache.hudi.HoodieMergeOnReadBootstrapRDD.CONFIG_INSTANTIATION_LOCK
import org.apache.hudi.common.util.ValidationUtils.checkState
import org.apache.hudi.hadoop.utils.HoodieRealtimeRecordReaderUtils.getMaxCompactionMemoryInBytes
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.HoodieCatalystExpressionUtils.generateUnsafeProjection
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.JoinedRow
import org.apache.spark.sql.types.StructType
import org.apache.spark.{Partition, SerializableWritable, TaskContext}

class HoodieMergeOnReadBootstrapRDD(@transient spark: SparkSession,
                                    @transient config: Configuration,
                                    bootstrapDataFileReader: BaseFileReader,
                                    bootstrapSkeletonFileReader: BaseFileReader,
                                    regularFileReader: BaseFileReader,
                                    tableSchema: HoodieTableSchema,
                                    requiredSchema: HoodieTableSchema,
                                    tableState: HoodieTableState,
                                    @transient splits: Seq[MergeOnReadBootstrapSplit])
  extends RDD[InternalRow](spark.sparkContext, Nil) {

  protected val maxCompactionMemoryInBytes: Long = getMaxCompactionMemoryInBytes(new JobConf(config))

  private val hadoopConfBroadcast = spark.sparkContext.broadcast(new SerializableWritable(config))

  override def compute(split: Partition, context: TaskContext): Iterator[InternalRow] = {
    val bootstrapPartition = split.asInstanceOf[MergeOnReadBootstrapPartition]

    if (log.isDebugEnabled) {
      if (bootstrapPartition.split.skeletonFile.isDefined) {
        logDebug("Got Split => Index: " + bootstrapPartition.index + ", Data File: "
          + bootstrapPartition.split.dataFile.filePath + ", Skeleton File: "
          + bootstrapPartition.split.skeletonFile.get.filePath)
      } else {
        logDebug("Got Split => Index: " + bootstrapPartition.index + ", Data File: "
          + bootstrapPartition.split.dataFile.filePath)
      }
    }

    bootstrapPartition.split.skeletonFile match {
      case Some(skeletonFile) =>
        // It is a bootstrap split. Check both skeleton and data files.
        val (iterator, schema) = if (bootstrapDataFileReader.schema.isEmpty) {
          // No data column to fetch, hence fetch only from skeleton file
          (bootstrapSkeletonFileReader.read(skeletonFile), bootstrapSkeletonFileReader.schema)
        } else if (bootstrapSkeletonFileReader.schema.isEmpty) {
          // No metadata column to fetch, hence fetch only from data file
          (bootstrapDataFileReader.read(bootstrapPartition.split.dataFile), bootstrapDataFileReader.schema)
        } else {
          // Fetch from both data and skeleton file, and merge
          val dataFileIterator = bootstrapDataFileReader.read(bootstrapPartition.split.dataFile)
          val skeletonFileIterator = bootstrapSkeletonFileReader.read(skeletonFile)
          val mergedSchema = StructType(bootstrapSkeletonFileReader.schema.fields ++ bootstrapDataFileReader.schema.fields)

          (merge(skeletonFileIterator, dataFileIterator), mergedSchema)
        }

        // NOTE: Here we have to project the [[InternalRow]]s fetched into the expected target schema.
        //       These could diverge for ex, when requested schema contains partition columns which might not be
        //       persisted w/in the data file, but instead would be parsed from the partition path. In that case
        //       output of the file-reader will have different ordering of the fields than the original required
        //       schema (for more details please check out [[ParquetFileFormat]] implementation).
        val unsafeProjection = generateUnsafeProjection(schema, requiredSchema.structTypeSchema)

        if (bootstrapPartition.split.logFiles.isEmpty) {
          iterator.map(unsafeProjection)
        } else {
          new RecordMergingFileIterator(HoodieMergeOnReadFileSplit(Some(bootstrapPartition.split.dataFile), bootstrapPartition.split.logFiles),
            iterator.map(unsafeProjection), schema, tableSchema, requiredSchema, tableState, getHadoopConf)
        }
      case _ =>
        // NOTE: Regular file-reader is already projected into the required schema
        new RecordMergingFileIterator(HoodieMergeOnReadFileSplit(Some(bootstrapPartition.split.dataFile), bootstrapPartition.split.logFiles),
          regularFileReader.read(bootstrapPartition.split.dataFile), regularFileReader.schema, tableSchema, requiredSchema, tableState, getHadoopConf)
    }
  }

  def merge(skeletonFileIterator: Iterator[InternalRow], dataFileIterator: Iterator[InternalRow]): Iterator[InternalRow] = {
    new Iterator[InternalRow] {
      private val combinedRow = new JoinedRow()

      override def hasNext: Boolean = {
        checkState(dataFileIterator.hasNext == skeletonFileIterator.hasNext,
          "Bootstrap data-file iterator and skeleton-file iterator have to be in-sync!")
        dataFileIterator.hasNext && skeletonFileIterator.hasNext
      }

      override def next(): InternalRow = {
        combinedRow(skeletonFileIterator.next(), dataFileIterator.next())
      }
    }
  }

  override protected def getPartitions: Array[Partition] = {
    splits.zipWithIndex.map(file => {
      if (file._1.skeletonFile.isDefined) {
        logDebug("Forming partition with => Index: " + file._2 + ", Files: " + file._1.dataFile.filePath
          + "," + file._1.skeletonFile.get.filePath)
        MergeOnReadBootstrapPartition(file._2, file._1)
      } else {
        logDebug("Forming partition with => Index: " + file._2 + ", File: " + file._1.dataFile.filePath)
        MergeOnReadBootstrapPartition(file._2, file._1)
      }
    }).toArray
  }

  private def getHadoopConf: Configuration = {
    val conf = hadoopConfBroadcast.value.value
    // TODO clean up, this lock is unnecessary
    CONFIG_INSTANTIATION_LOCK.synchronized {
      new Configuration(conf)
    }
  }
}

object HoodieMergeOnReadBootstrapRDD {
  val CONFIG_INSTANTIATION_LOCK = new Object()
}


case class MergeOnReadBootstrapPartition(index: Int, split: MergeOnReadBootstrapSplit) extends Partition
