/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.spark.sql.hudi.command.procedures

import org.apache.hudi.HoodieCLIUtils
import org.apache.hudi.common.model.{HoodieCommitMetadata, HoodieReplaceCommitMetadata, HoodieWriteStat}
import org.apache.hudi.common.table.HoodieTableMetaClient
import org.apache.hudi.common.table.timeline.{HoodieInstant, HoodieTimeline, TimelineLayout}
import org.apache.hudi.common.util.ClusteringUtils
import org.apache.hudi.exception.HoodieException

import org.apache.spark.sql.Row
import org.apache.spark.sql.types.{DataTypes, Metadata, StructField, StructType}

import java.util
import java.util.List
import java.util.function.Supplier

import scala.collection.JavaConverters._

class ShowCommitFilesProcedure() extends BaseProcedure with ProcedureBuilder {
  private val PARAMETERS = Array[ProcedureParameter](
    ProcedureParameter.required(0, "table", DataTypes.StringType),
    ProcedureParameter.optional(1, "limit", DataTypes.IntegerType, 10),
    ProcedureParameter.required(2, "instant_time", DataTypes.StringType)
  )

  private val OUTPUT_TYPE = new StructType(Array[StructField](
    StructField("action", DataTypes.StringType, nullable = true, Metadata.empty),
    StructField("partition_path", DataTypes.StringType, nullable = true, Metadata.empty),
    StructField("file_id", DataTypes.StringType, nullable = true, Metadata.empty),
    StructField("previous_commit", DataTypes.StringType, nullable = true, Metadata.empty),
    StructField("total_records_updated", DataTypes.LongType, nullable = true, Metadata.empty),
    StructField("total_records_written", DataTypes.LongType, nullable = true, Metadata.empty),
    StructField("total_bytes_written", DataTypes.LongType, nullable = true, Metadata.empty),
    StructField("total_errors", DataTypes.LongType, nullable = true, Metadata.empty),
    StructField("file_size", DataTypes.LongType, nullable = true, Metadata.empty)
  ))

  def parameters: Array[ProcedureParameter] = PARAMETERS

  def outputType: StructType = OUTPUT_TYPE

  override def call(args: ProcedureArgs): Seq[Row] = {
    super.checkArgs(PARAMETERS, args)

    val table = getArgValueOrDefault(args, PARAMETERS(0)).get.asInstanceOf[String]
    val limit = getArgValueOrDefault(args, PARAMETERS(1)).get.asInstanceOf[Int]
    val instantTime = getArgValueOrDefault(args, PARAMETERS(2)).get.asInstanceOf[String]

    val hoodieCatalogTable = HoodieCLIUtils.getHoodieCatalogTable(sparkSession, table)
    val basePath = hoodieCatalogTable.tableLocation
    val metaClient = createMetaClient(jsc, basePath)
    val activeTimeline = metaClient.getActiveTimeline
    val timeline = activeTimeline.getCommitsTimeline.filterCompletedInstants
    val hoodieInstantOption = getCommitForInstant(metaClient, timeline, instantTime)
    val commitMetadataOptional = getHoodieCommitMetadata(timeline, hoodieInstantOption)

    if (commitMetadataOptional.isEmpty) {
      throw new HoodieException(s"Commit $instantTime not found in Commits $timeline.")
    }

    val meta = commitMetadataOptional.get
    val rows = new util.ArrayList[Row]
    for (entry <- meta.getPartitionToWriteStats.entrySet.asScala) {
      val action: String = hoodieInstantOption.get.getAction
      val path: String = entry.getKey
      val stats: List[HoodieWriteStat] = entry.getValue
      for (stat <- stats.asScala) {
        rows.add(Row(action, path, stat.getFileId, stat.getPrevCommit, stat.getNumUpdateWrites,
          stat.getNumWrites, stat.getTotalWriteBytes, stat.getTotalWriteErrors, stat.getFileSizeInBytes))
      }
    }
    rows.stream().limit(limit).toArray().map(r => r.asInstanceOf[Row]).toList
  }

  override def build: Procedure = new ShowCommitFilesProcedure()

  private def getCommitForInstant(metaClient: HoodieTableMetaClient, timeline: HoodieTimeline, instantTime: String): Option[HoodieInstant] = {
    val instantGenerator = metaClient.getTimelineLayout.getInstantGenerator
    val instants: util.List[HoodieInstant] = util.Arrays.asList(
      instantGenerator.createNewInstant(HoodieInstant.State.COMPLETED, HoodieTimeline.COMMIT_ACTION, instantTime),
      instantGenerator.createNewInstant(HoodieInstant.State.COMPLETED, HoodieTimeline.REPLACE_COMMIT_ACTION, instantTime),
      instantGenerator.createNewInstant(HoodieInstant.State.COMPLETED, HoodieTimeline.CLUSTERING_ACTION, instantTime),
      instantGenerator.createNewInstant(HoodieInstant.State.COMPLETED, HoodieTimeline.DELTA_COMMIT_ACTION, instantTime))

    val hoodieInstant: Option[HoodieInstant] = instants.asScala.find((i: HoodieInstant) => timeline.containsInstant(i))
    hoodieInstant
  }

  private def getHoodieCommitMetadata(timeline: HoodieTimeline, hoodieInstant: Option[HoodieInstant]): Option[HoodieCommitMetadata] = {
    val layout = TimelineLayout.fromVersion(timeline.getTimelineLayoutVersion)
    if (hoodieInstant.isDefined) {
      if (ClusteringUtils.isClusteringOrReplaceCommitAction(hoodieInstant.get.getAction)) {

        Option(timeline.readReplaceCommitMetadata(hoodieInstant.get))
      } else {
        Option(timeline.readCommitMetadata(hoodieInstant.get))
      }
    } else {
      Option.empty
    }
  }
}

object ShowCommitFilesProcedure {
  val NAME = "show_commit_files"

  def builder: Supplier[ProcedureBuilder] = new Supplier[ProcedureBuilder] {
    override def get() = new ShowCommitFilesProcedure()
  }
}
