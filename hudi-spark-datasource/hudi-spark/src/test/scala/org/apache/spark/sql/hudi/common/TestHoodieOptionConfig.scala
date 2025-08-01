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

package org.apache.spark.sql.hudi.common

import org.apache.hudi.common.model.{HoodieRecordMerger, OverwriteWithLatestAvroPayload}
import org.apache.hudi.common.table.HoodieTableConfig
import org.apache.hudi.testutils.SparkClientFunctionalTestHarness

import org.apache.spark.sql.hudi.HoodieOptionConfig
import org.apache.spark.sql.types._
import org.junit.jupiter.api.Assertions.{assertEquals, assertTrue}
import org.junit.jupiter.api.Test
import org.scalatest.Matchers.intercept

class TestHoodieOptionConfig extends SparkClientFunctionalTestHarness {

  @Test
  def testWithDefaultSqlOptions(): Unit = {
    val ops1 = Map("primaryKey" -> "id")
    val with1 = HoodieOptionConfig.withDefaultSqlOptions(ops1)
    assertEquals(2, with1.size)
    assertEquals("id", with1("primaryKey"))
    assertEquals("cow", with1("type"))

    val ops2 = Map("primaryKey" -> "id",
      "preCombineField" -> "timestamp",
      "type" -> "mor",
      "payloadClass" -> classOf[OverwriteWithLatestAvroPayload].getName,
      "recordMergeStrategyId" -> HoodieRecordMerger.EVENT_TIME_BASED_MERGE_STRATEGY_UUID
    )
    val with2 = HoodieOptionConfig.withDefaultSqlOptions(ops2)
    assertTrue(ops2 == with2)
  }

  @Test
  def testMappingSqlOptionToTableConfig(): Unit = {
    val sqlOptions = Map("primaryKey" -> "id,addr",
      "preCombineField" -> "timestamp",
      "type" -> "mor",
      "hoodie.index.type" -> "INMEMORY",
      "hoodie.compact.inline" -> "true"
    )
    val tableConfigs = HoodieOptionConfig.mapSqlOptionsToTableConfigs(sqlOptions)

    assertTrue(tableConfigs.size == 5)
    assertTrue(tableConfigs(HoodieTableConfig.RECORDKEY_FIELDS.key) == "id,addr")
    assertTrue(tableConfigs(HoodieTableConfig.PRECOMBINE_FIELDS.key) == "timestamp")
    assertTrue(tableConfigs(HoodieTableConfig.TYPE.key) == "MERGE_ON_READ")
    assertTrue(tableConfigs("hoodie.index.type") == "INMEMORY")
    assertTrue(tableConfigs("hoodie.compact.inline") == "true")
  }

  @Test
  def testDeleteHoodieOptions(): Unit = {
    val sqlOptions = Map("primaryKey" -> "id,addr",
      "preCombineField" -> "timestamp",
      "type" -> "mor",
      "hoodie.index.type" -> "INMEMORY",
      "hoodie.compact.inline" -> "true",
      "key123" -> "value456"
    )
    val tableConfigs = HoodieOptionConfig.deleteHoodieOptions(sqlOptions)
    assertTrue(tableConfigs.size == 1)
    assertTrue(tableConfigs("key123") == "value456")
  }

  @Test
  def testExtractSqlOptions(): Unit = {
    val sqlOptions = Map("primaryKey" -> "id,addr",
      "preCombineField" -> "timestamp",
      "type" -> "mor",
      "hoodie.index.type" -> "INMEMORY",
      "hoodie.compact.inline" -> "true",
      "key123" -> "value456"
    )
    val tableConfigs = HoodieOptionConfig.extractSqlOptions(sqlOptions)
    assertTrue(tableConfigs.size == 3)
    assertTrue(tableConfigs.keySet == Set("primaryKey", "preCombineField", "type"))
  }

  @Test
  def testValidateTable(): Unit = {
    val baseSqlOptions = Map(
      "hoodie.datasource.write.hive_style_partitioning" -> "true",
      "hoodie.datasource.write.partitionpath.urlencode" -> "false",
      "hoodie.table.keygenerator.class" -> "org.apache.hudi.keygen.ComplexKeyGenerator"
    )

    val schema = StructType(
      Seq(StructField("id", IntegerType, true),
        StructField("name", StringType, true),
        StructField("timestamp", TimestampType, true),
        StructField("dt", StringType, true))
    )

    // primary field not found
    val sqlOptions2 = baseSqlOptions ++ Map(
      "primaryKey" -> "xxx",
      "type" -> "mor"
    )
    val e2 = intercept[IllegalArgumentException] {
      HoodieOptionConfig.validateTable(spark, schema, sqlOptions2)
    }
    assertTrue(e2.getMessage.contains("Can't find primaryKey"))

    // preCombine field not found
    val sqlOptions3 = baseSqlOptions ++ Map(
      "primaryKey" -> "id",
      "preCombineField" -> "ts",
      "type" -> "mor"
    )
    val e3 = intercept[IllegalArgumentException] {
      HoodieOptionConfig.validateTable(spark, schema, sqlOptions3)
    }
    assertTrue(e3.getMessage.contains("Can't find preCombineKey"))

    // miss type parameter
    val sqlOptions4 = baseSqlOptions ++ Map(
      "primaryKey" -> "id",
      "preCombineField" -> "timestamp"
    )
    val e4 = intercept[IllegalArgumentException] {
      HoodieOptionConfig.validateTable(spark, schema, sqlOptions4)
    }
    assertTrue(e4.getMessage.contains("No `type` is specified."))

    // type is invalid
    val sqlOptions5 = baseSqlOptions ++ Map(
      "primaryKey" -> "id",
      "preCombineField" -> "timestamp",
      "type" -> "abc"
    )
    val e5 = intercept[IllegalArgumentException] {
      HoodieOptionConfig.validateTable(spark, schema, sqlOptions5)
    }
    assertTrue(e5.getMessage.contains("'type' must be 'cow' or 'mor'"))

    // right options and schema
    val sqlOptions6 = baseSqlOptions ++ Map(
      "primaryKey" -> "id",
      "preCombineField" -> "timestamp",
      "type" -> "cow"
    )
    HoodieOptionConfig.validateTable(spark, schema, sqlOptions6)
  }

}
