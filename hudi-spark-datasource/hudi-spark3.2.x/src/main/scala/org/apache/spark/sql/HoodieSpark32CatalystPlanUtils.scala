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

package org.apache.spark.sql

import org.apache.hudi.HoodieSparkUtils
import org.apache.hudi.common.util.ValidationUtils.checkArgument
import org.apache.spark.sql.catalyst.TableIdentifier
import org.apache.spark.sql.catalyst.analysis.AnalysisErrorAt
import org.apache.spark.sql.catalyst.analysis.ResolvedTable
import org.apache.spark.sql.catalyst.expressions.{Attribute, AttributeSet, Expression, ProjectionOverSchema}
import org.apache.spark.sql.catalyst.plans.logical.{LogicalPlan, MergeIntoTable}
import org.apache.spark.sql.connector.catalog.{Identifier, Table, TableCatalog}
import org.apache.spark.sql.execution.command.RepairTableCommand
import org.apache.spark.sql.types.StructType

object HoodieSpark32CatalystPlanUtils extends HoodieSpark3CatalystPlanUtils {

  def unapplyResolvedTable(plan: LogicalPlan): Option[(TableCatalog, Identifier, Table)] =
    plan match {
      case ResolvedTable(catalog, identifier, table, _) => Some((catalog, identifier, table))
      case _ => None
    }

  override def unapplyMergeIntoTable(plan: LogicalPlan): Option[(LogicalPlan, LogicalPlan, Expression)] = {
    plan match {
      case MergeIntoTable(targetTable, sourceTable, mergeCondition, _, _) =>
        Some((targetTable, sourceTable, mergeCondition))
      case _ => None
    }
  }

  override def applyMORBootstrapFileFormatProjection(plan: LogicalPlan): LogicalPlan = {
    plan match {
      case s@ScanOperation(_, _,
      l@LogicalRelation(fs: HadoopFsRelation, _, _, _)) if fs.fileFormat.isInstanceOf[MORBootstrapFileFormat] && !fs.fileFormat.asInstanceOf[MORBootstrapFileFormat].isProjected =>
        fs.fileFormat.asInstanceOf[MORBootstrapFileFormat].isProjected = true
        Project(l.resolve(fs.location.asInstanceOf[SparkHoodieTableFileIndex].schema, fs.sparkSession.sessionState.analyzer.resolver), s)
      case _ => plan
    }
  }

  override def projectOverSchema(schema: StructType, output: AttributeSet): ProjectionOverSchema = {
    val klass = classOf[ProjectionOverSchema]
    checkArgument(klass.getConstructors.length == 1)
    val ctor = klass.getConstructors.head

    val p = if (HoodieSparkUtils.gteqSpark3_2_2) {
      // Spark >= 3.2.2
      ctor.newInstance(schema, output)
    } else {
      // Spark 3.2.0 and 3.2.1
      ctor.newInstance(schema) // ProjectionOverSchema(schema)
    }

    p.asInstanceOf[ProjectionOverSchema]
  }

  override def isRepairTable(plan: LogicalPlan): Boolean = {
    plan.isInstanceOf[RepairTableCommand]
  }

  override def getRepairTableChildren(plan: LogicalPlan): Option[(TableIdentifier, Boolean, Boolean, String)] = {
    plan match {
      case rtc: RepairTableCommand =>
        Some((rtc.tableName, rtc.enableAddPartitions, rtc.enableDropPartitions, rtc.cmd))
      case _ =>
        None
    }
  }

  override def failAnalysisForMIT(a: Attribute, cols: String): Unit = {
    a.failAnalysis(s"cannot resolve ${a.sql} in MERGE command given columns [$cols]")
  }
}
