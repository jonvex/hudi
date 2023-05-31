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

package org.apache.hudi.functional

import org.apache.hudi.common.testutils.HoodieTestDataGenerator
import org.apache.spark.sql
import org.apache.spark.sql.hudi.HoodieSparkSqlTestBase
import org.apache.hudi.common.testutils.RawTripTestPayload.recordsToStrings
import org.scalatest.Inspectors.forAll

import java.io.File
import scala.collection.JavaConversions._

@SparkSQLCoreFlow
class TestSparkSqlCoreFlow extends HoodieSparkSqlTestBase {
  val colsToCompare = "timestamp, _row_key, partition_path, rider, driver, begin_lat, begin_lon, end_lat, end_lon, fare.amount, fare.currency, _hoodie_is_deleted"
  def getWriteOptions(tableName: String, tableType: String, preCombine: Boolean, primaryKey: Boolean): String = {
    val typeString = if (tableType.equals("COPY_ON_WRITE")) {
      "cow"
    } else if (tableType.equals("MERGE_ON_READ")) {
      "mor"
    } else {
      tableType
    }

    val precombinField = if (preCombine) {
      "timestamp"
    } else {
      ""
    }

    val primaryKeyField = if (primaryKey) {
      "primaryKey = '_row_key',"
    } else {
      ""
    }

    s"""
       |tblproperties (
       |  type = '$typeString',
       |  $primaryKeyField
       |  preCombineField = '$precombinField',
       |  hoodie.bulkinsert.shuffle.parallelism = 4,
       |  hoodie.database.name = "databaseName",
       |  hoodie.delete.shuffle.parallelism = 2,
       |  hoodie.insert.shuffle.parallelism = 4,
       |  hoodie.table.name = "$tableName",
       |  hoodie.upsert.shuffle.parallelism = 4
       | )""".stripMargin
  }

  def createTable(tableName: String, writeOptions: String, tableBasePath: String): Unit = {


    spark.sql(
      s"""
         | create table $tableName (
         |  timestamp long,
         |  _row_key string,
         |  rider string,
         |  driver string,
         |  begin_lat double,
         |  begin_lon double,
         |  end_lat double,
         |  end_lon double,
         |  fare STRUCT<
         |    amount: double,
         |    currency: string >,
         |  _hoodie_is_deleted boolean,
         |  partition_path string
         |) using hudi
         | partitioned by (partition_path)
         | $writeOptions
         | location '$tableBasePath'
         |
    """.stripMargin)
  }

  def generateInserts(dataGen: HoodieTestDataGenerator, instantTime: String, n: Int): sql.DataFrame = {
    val recs = dataGen.generateInsertsNestedExample(instantTime, n)
    spark.read.json(spark.sparkContext.parallelize(recordsToStrings(recs), 2))
  }


  val paramsForInsert: List[String] = List(
    "COPY_ON_WRITE|upsert|true|true",
    "COPY_ON_WRITE|upsert|false|true",
    "COPY_ON_WRITE|strict|true|true",
    "COPY_ON_WRITE|strict|false|true",
    "COPY_ON_WRITE|non-strict|true|true",
    "COPY_ON_WRITE|non-strict|false|true",
    "MERGE_ON_READ|upsert|true|true",
    "MERGE_ON_READ|upsert|false|true",
    "MERGE_ON_READ|strict|true|true",
    "MERGE_ON_READ|strict|false|true",
    "MERGE_ON_READ|non-strict|true|true",
    "MERGE_ON_READ|non-strict|false|true",
    "COPY_ON_WRITE|upsert|false|false",
    "COPY_ON_WRITE|strict|false|false",
    "COPY_ON_WRITE|non-strict|false|false",
    "MERGE_ON_READ|upsert|false|false",
    "MERGE_ON_READ|strict|false|false",
    "MERGE_ON_READ|non-strict|false|false",
  )

  //extracts the params and runs each immutable user flow test
//  forAll(paramsForInsert) { (paramStr: String) =>
//    test(s"Insert flow with params: $paramStr") {
//      val splits = paramStr.split('|')
//      withTempDir { basePath =>
//        testInsert(basePath,
//          tableType = splits(0),
//          insertMode = splits(1),
//          withPrecombine = splits(2).toBoolean,
//          withPrimaryKey = splits(3).toBoolean)
//      }
//    }
//  }

  def testInsert(basePath: File, tableType: String,  insertMode: String, withPrecombine: Boolean, withPrimaryKey: Boolean): Unit = {
    val tableName = generateTableName
    val tableBasePath = basePath.getCanonicalPath + "/" + tableName
    val writeOptions = getWriteOptions(tableName, tableType, withPrecombine, withPrimaryKey)
    createTable(tableName, writeOptions, tableBasePath)
    //Insert Operation
    val dataGen = new HoodieTestDataGenerator(HoodieTestDataGenerator.TRIP_NESTED_EXAMPLE_SCHEMA, 0xDEED)
    val inputDf0 = generateInserts(dataGen, "000", 100)
    inputDf0.select("timestamp", "_row_key", "rider", "driver", "begin_lat", "begin_lon", "end_lat", "end_lon", "fare",
      "_hoodie_is_deleted", "partition_path").createOrReplaceTempView("insert_temp_table")
    spark.sql(s"set hoodie.metadata.enable=false")
    spark.sql(s"set hoodie.sql.insert.mode=$insertMode")
    spark.sql(s"insert into $tableName select * from insert_temp_table")
  }

  val paramsForCTAS: List[String] = List(
    "COPY_ON_WRITE|upsert|true|true",
    "COPY_ON_WRITE|upsert|false|true",
    "COPY_ON_WRITE|strict|true|true",
    "COPY_ON_WRITE|strict|false|true",
    "COPY_ON_WRITE|non-strict|true|true",
    "COPY_ON_WRITE|non-strict|false|true",
    "MERGE_ON_READ|upsert|true|true",
    "MERGE_ON_READ|upsert|false|true",
    "MERGE_ON_READ|strict|true|true",
    "MERGE_ON_READ|strict|false|true",
    "MERGE_ON_READ|non-strict|true|true",
    "MERGE_ON_READ|non-strict|false|true",
    "COPY_ON_WRITE|upsert|false|false",
    "COPY_ON_WRITE|strict|false|false",
    "COPY_ON_WRITE|non-strict|false|false",
    "MERGE_ON_READ|upsert|false|false",
    "MERGE_ON_READ|strict|false|false",
    "MERGE_ON_READ|non-strict|false|false",
  )

  //extracts the params and runs each immutable user flow test
  forAll(paramsForCTAS) { (paramStr: String) =>
    test(s"CTAS flow with params: $paramStr") {
      val splits = paramStr.split('|')
      withTempDir { basePath =>
        testCTAS(basePath,
          tableType = splits(0),
          insertMode = splits(1),
          withPrecombine = splits(2).toBoolean,
          withPrimaryKey = splits(3).toBoolean)
      }
    }
  }

  def testCTAS(basePath: File, tableType: String, insertMode: String, withPrecombine: Boolean, withPrimaryKey: Boolean): Unit = {
    val tableName = generateTableName
    val tableBasePath = basePath.getCanonicalPath + "/" + tableName
    //Insert Operation
    val dataGen = new HoodieTestDataGenerator(HoodieTestDataGenerator.TRIP_NESTED_EXAMPLE_SCHEMA, 0xDEED)
    val inputDf0 = generateInserts(dataGen, "000", 100)
    inputDf0.select("timestamp", "_row_key", "rider", "driver", "begin_lat", "begin_lon", "end_lat", "end_lon", "fare",
      "_hoodie_is_deleted", "partition_path").createOrReplaceTempView("insert_temp_table")
    val typeString = if (tableType.equals("COPY_ON_WRITE")) {
      "cow"
    } else if (tableType.equals("MERGE_ON_READ")) {
      "mor"
    } else {
      tableType
    }

    val precombinField = if (withPrecombine) {
      "timestamp"
    } else {
      ""
    }

    val primaryKeyField = if (withPrimaryKey) {
      "primaryKey = '_row_key',"
    } else {
      ""
    }

    spark.sql(s"set hoodie.sql.insert.mode=$insertMode")
    spark.sql(
      s"""
         | create table $tableName using hudi
         | partitioned by (partition_path)
         | tblproperties(
         |    type = '$typeString',
         |    $primaryKeyField
         |    preCombineField = '$precombinField',
         |    hoodie.database.name = "databaseName",
         |    hoodie.table.name = "$tableName",
         |    hoodie.sql.insert.mode = '$insertMode',
         |    hoodie.datasource.write.operation = 'upsert'
         | )
         | location '$tableBasePath'
         | as
         | select * from insert_temp_table
      """.stripMargin)
  }

}
