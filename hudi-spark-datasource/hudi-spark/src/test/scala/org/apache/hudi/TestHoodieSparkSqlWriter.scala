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

package org.apache.hudi

import org.apache.avro.Schema
import org.apache.commons.io.FileUtils
import org.apache.hudi.DataSourceWriteOptions._
import org.apache.hudi.HoodieSparkUtils.gteqSpark3_0
import org.apache.hudi.client.SparkRDDWriteClient
import org.apache.hudi.common.model._
import org.apache.hudi.common.table.{HoodieTableConfig, HoodieTableMetaClient, TableSchemaResolver}
import org.apache.hudi.common.testutils.HoodieTestDataGenerator
import org.apache.hudi.config.{HoodieBootstrapConfig, HoodieIndexConfig, HoodieWriteConfig}
import org.apache.hudi.exception.{HoodieException, SchemaCompatibilityException}
import org.apache.hudi.execution.bulkinsert.BulkInsertSortMode
import org.apache.hudi.functional.TestBootstrap
import org.apache.hudi.keygen.{ComplexKeyGenerator, NonpartitionedKeyGenerator, SimpleKeyGenerator}
import org.apache.hudi.testutils.DataSourceTestUtils
import org.apache.hudi.testutils.HoodieClientTestUtils.getSparkConfForTest
import org.apache.spark.SparkContext
import org.apache.spark.api.java.JavaSparkContext
import org.apache.spark.sql._
import org.apache.spark.sql.functions.{expr, lit}
import org.apache.spark.sql.hudi.HoodieSparkSessionExtension
import org.apache.spark.sql.hudi.command.SqlKeyGenerator
import org.junit.jupiter.api.Assertions.{assertEquals, assertFalse, assertTrue, fail}
import org.junit.jupiter.api.{AfterEach, BeforeEach, Test}
import org.junit.jupiter.params.ParameterizedTest
import org.junit.jupiter.params.provider.Arguments.arguments
import org.junit.jupiter.params.provider.{Arguments, CsvSource, EnumSource, MethodSource, ValueSource}
import org.mockito.ArgumentMatchers.any
import org.mockito.Mockito.{spy, times, verify}
import org.scalatest.Assertions.assertThrows
import org.scalatest.Matchers.{be, convertToAnyShouldWrapper, intercept}

import java.io.IOException
import java.time.Instant
import java.util.{Collections, Date, UUID}
import scala.collection.JavaConversions._
import scala.collection.JavaConverters

/**
 * Test suite for SparkSqlWriter class.
 */
class TestHoodieSparkSqlWriter {
  var spark: SparkSession = _
  var sqlContext: SQLContext = _
  var sc: SparkContext = _
  var tempPath: java.nio.file.Path = _
  var tempBootStrapPath: java.nio.file.Path = _
  var hoodieFooTableName = "hoodie_foo_tbl"
  var tempBasePath: String = _
  var commonTableModifier: Map[String, String] = Map()
  case class StringLongTest(uuid: String, ts: Long)

  /**
   * Setup method running before each test.
   */
  @BeforeEach
  def setUp(): Unit = {
    initSparkContext()
    tempPath = java.nio.file.Files.createTempDirectory("hoodie_test_path")
    tempBootStrapPath = java.nio.file.Files.createTempDirectory("hoodie_test_bootstrap")
    tempBasePath = tempPath.toAbsolutePath.toString
    commonTableModifier = getCommonParams(tempPath, hoodieFooTableName, HoodieTableType.COPY_ON_WRITE.name())
  }

  /**
   * Tear down method running after each test.
   */
  @AfterEach
  def tearDown(): Unit = {
    cleanupSparkContexts()
    FileUtils.deleteDirectory(tempPath.toFile)
    FileUtils.deleteDirectory(tempBootStrapPath.toFile)
  }

  /**
   * Utility method for initializing the spark context.
   *
   * TODO rebase this onto existing base class to avoid duplication
   */
  def initSparkContext(): Unit = {
    val sparkConf = getSparkConfForTest(getClass.getSimpleName)

    spark = SparkSession.builder()
      .withExtensions(new HoodieSparkSessionExtension)
      .config(sparkConf)
      .getOrCreate()

    sc = spark.sparkContext
    sc.setLogLevel("ERROR")
    sqlContext = spark.sqlContext
  }

  /**
   * Utility method for cleaning up spark resources.
   */
  def cleanupSparkContexts(): Unit = {
    if (sqlContext != null) {
      sqlContext.clearCache();
      sqlContext = null;
    }
    if (sc != null) {
      sc.stop()
      sc = null
    }
    if (spark != null) {
      spark.close()
    }
  }

  /**
   * Utility method for dropping all hoodie meta related columns.
   */
  def dropMetaFields(df: Dataset[Row]): Dataset[Row] = {
    df.drop(HoodieRecord.HOODIE_META_COLUMNS.get(0)).drop(HoodieRecord.HOODIE_META_COLUMNS.get(1))
      .drop(HoodieRecord.HOODIE_META_COLUMNS.get(2)).drop(HoodieRecord.HOODIE_META_COLUMNS.get(3))
      .drop(HoodieRecord.HOODIE_META_COLUMNS.get(4))
  }

  /**
   * Utility method for creating common params for writer.
   *
   * @param path               Path for hoodie table
   * @param hoodieFooTableName Name of hoodie table
   * @param tableType          Type of table
   * @return                   Map of common params
   */
  def getCommonParams(path: java.nio.file.Path, hoodieFooTableName: String, tableType: String): Map[String, String] = {
    Map("path" -> path.toAbsolutePath.toString,
      HoodieWriteConfig.TBL_NAME.key -> hoodieFooTableName,
      "hoodie.insert.shuffle.parallelism" -> "1",
      "hoodie.upsert.shuffle.parallelism" -> "1",
      DataSourceWriteOptions.TABLE_TYPE.key -> tableType,
      DataSourceWriteOptions.RECORDKEY_FIELD.key -> "_row_key",
      DataSourceWriteOptions.PARTITIONPATH_FIELD.key -> "partition",
      DataSourceWriteOptions.KEYGENERATOR_CLASS_NAME.key -> "org.apache.hudi.keygen.SimpleKeyGenerator")
  }

  /**
   * Utility method for converting list of Row to list of Seq.
   *
   * @param inputList list of Row
   * @return list of Seq
   */
  def convertRowListToSeq(inputList: java.util.List[Row]): Seq[Row] =
    JavaConverters.asScalaIteratorConverter(inputList.iterator).asScala.toSeq

  /**
   * Utility method for performing bulk insert  tests.
   *
   * @param sortMode           Bulk insert sort mode
   * @param populateMetaFields Flag for populating meta fields
   */
  def testBulkInsertWithSortMode(sortMode: BulkInsertSortMode, populateMetaFields: Boolean = true, enableOCCConfigs: Boolean = false): Unit = {
    //create a new table
    var fooTableModifier = commonTableModifier.updated("hoodie.bulkinsert.shuffle.parallelism", "4")
      .updated(DataSourceWriteOptions.OPERATION.key, DataSourceWriteOptions.BULK_INSERT_OPERATION_OPT_VAL)
      .updated(DataSourceWriteOptions.ENABLE_ROW_WRITER.key, "true")
      .updated(HoodieTableConfig.POPULATE_META_FIELDS.key(), String.valueOf(populateMetaFields))
      .updated(HoodieWriteConfig.BULK_INSERT_SORT_MODE.key(), sortMode.name())

    if (enableOCCConfigs) {
      fooTableModifier = fooTableModifier
        .updated("hoodie.write.concurrency.mode","optimistic_concurrency_control")
        .updated("hoodie.cleaner.policy.failed.writes","LAZY")
        .updated("hoodie.write.lock.provider","org.apache.hudi.client.transaction.lock.InProcessLockProvider")
    }

    // generate the inserts
    val schema = DataSourceTestUtils.getStructTypeExampleSchema
    val structType = AvroConversionUtils.convertAvroSchemaToStructType(schema)
    val inserts = DataSourceTestUtils.generateRandomRows(1000)

    // add some updates so that preCombine kicks in
    val toUpdateDataset = sqlContext.createDataFrame(DataSourceTestUtils.getUniqueRows(inserts, 40), structType)
    val updates = DataSourceTestUtils.updateRowsWithHigherTs(toUpdateDataset)
    val records = inserts.union(updates)
    val recordsSeq = convertRowListToSeq(records)
    val df = spark.createDataFrame(sc.parallelize(recordsSeq), structType)
    // write to Hudi
    HoodieSparkSqlWriter.write(sqlContext, SaveMode.Append, fooTableModifier, df)

    // collect all partition paths to issue read of parquet files
    val partitions = Seq(HoodieTestDataGenerator.DEFAULT_FIRST_PARTITION_PATH, HoodieTestDataGenerator.DEFAULT_SECOND_PARTITION_PATH,
      HoodieTestDataGenerator.DEFAULT_THIRD_PARTITION_PATH)
    // Check the entire dataset has all records still
    val fullPartitionPaths = new Array[String](3)
    for (i <- fullPartitionPaths.indices) {
      fullPartitionPaths(i) = String.format("%s/%s/*", tempBasePath, partitions(i))
    }
    // fetch all records from parquet files generated from write to hudi
    val actualDf = sqlContext.read.parquet(fullPartitionPaths(0), fullPartitionPaths(1), fullPartitionPaths(2))
    if (!populateMetaFields) {
      List(0, 1, 2, 3, 4).foreach(i => assertEquals(0, actualDf.select(HoodieRecord.HOODIE_META_COLUMNS.get(i)).filter(entry => !(entry.mkString(",").equals(""))).count()))
    }
    // remove metadata columns so that expected and actual DFs can be compared as is
    val trimmedDf = dropMetaFields(actualDf)
    assert(df.except(trimmedDf).count() == 0)
  }

  /**
   * Utility method for performing bulk insert  tests.
   */
  @Test
  def testParametersWithWriteDefaults(): Unit = {
    val originals = HoodieWriterUtils.parametersWithWriteDefaults(Map.empty)
    val rhsKey = "hoodie.right.hand.side.key"
    val rhsVal = "hoodie.right.hand.side.val"
    val modifier = Map(OPERATION.key -> INSERT_OPERATION_OPT_VAL, TABLE_TYPE.key -> MOR_TABLE_TYPE_OPT_VAL, rhsKey -> rhsVal)
    val modified = HoodieWriterUtils.parametersWithWriteDefaults(modifier)
    val matcher = (k: String, v: String) => modified(k) should be(v)
    originals foreach {
      case ("hoodie.datasource.write.operation", _) => matcher("hoodie.datasource.write.operation", INSERT_OPERATION_OPT_VAL)
      case ("hoodie.datasource.write.table.type", _) => matcher("hoodie.datasource.write.table.type", MOR_TABLE_TYPE_OPT_VAL)
      case (`rhsKey`, _) => matcher(rhsKey, rhsVal)
      case (k, v) => matcher(k, v)
    }
  }

  /**
   * Test case for invalid serializer provided.
   */
  @Test
  def testThrowExceptionInvalidSerializer(): Unit = {
    spark.stop()
    val session = SparkSession.builder().appName("hoodie_test").master("local").getOrCreate()
    try {
      val sqlContext = session.sqlContext
      val options = Map("path" -> "hoodie/test/path", HoodieWriteConfig.TBL_NAME.key -> "hoodie_test_tbl")
      val e = intercept[HoodieException](HoodieSparkSqlWriter.write(sqlContext, SaveMode.ErrorIfExists, options,
        session.emptyDataFrame))
      assert(e.getMessage.contains("spark.serializer"))
    } finally {
      session.stop()
      initSparkContext()
    }
  }

  /**
   * Test case for throw hoodie exception when there already exist a table
   * with different name with Append Save mode
   */
  @Test
  def testThrowExceptionAlreadyExistsWithAppendSaveMode(): Unit = {
    //create a new table
    val fooTableModifier = Map(
      "path" -> tempBasePath,
      HoodieWriteConfig.TBL_NAME.key -> hoodieFooTableName,
      "hoodie.datasource.write.recordkey.field" -> "uuid",
      "hoodie.insert.shuffle.parallelism" -> "4",
      "hoodie.upsert.shuffle.parallelism" -> "4")
    val dataFrame = spark.createDataFrame(Seq(StringLongTest(UUID.randomUUID().toString, new Date().getTime)))
    HoodieSparkSqlWriter.write(sqlContext, SaveMode.Append, fooTableModifier, dataFrame)

    //on same path try append with different("hoodie_bar_tbl") table name which should throw an exception
    val barTableModifier = Map(
      "path" -> tempBasePath,
      HoodieWriteConfig.TBL_NAME.key -> "hoodie_bar_tbl",
      "hoodie.datasource.write.recordkey.field" -> "uuid",
      "hoodie.insert.shuffle.parallelism" -> "4",
      "hoodie.upsert.shuffle.parallelism" -> "4")
    val dataFrame2 = spark.createDataFrame(Seq(StringLongTest(UUID.randomUUID().toString, new Date().getTime)))
    val tableAlreadyExistException = intercept[HoodieException](HoodieSparkSqlWriter.write(sqlContext, SaveMode.Append, barTableModifier, dataFrame2))
    assert(tableAlreadyExistException.getMessage.contains("Config conflict"))
    assert(tableAlreadyExistException.getMessage.contains(s"${HoodieWriteConfig.TBL_NAME.key}:\thoodie_bar_tbl\thoodie_foo_tbl"))

    //on same path try append with delete operation and different("hoodie_bar_tbl") table name which should throw an exception
    val deleteTableModifier = barTableModifier ++ Map(OPERATION.key -> "delete")
    val deleteCmdException = intercept[HoodieException](HoodieSparkSqlWriter.write(sqlContext, SaveMode.Append, deleteTableModifier, dataFrame2))
    assert(tableAlreadyExistException.getMessage.contains("Config conflict"))
    assert(tableAlreadyExistException.getMessage.contains(s"${HoodieWriteConfig.TBL_NAME.key}:\thoodie_bar_tbl\thoodie_foo_tbl"))
  }

  /**
    * Test case for Do not validate table config if save mode is set to Overwrite
    */
  @Test
  def testValidateTableConfigWithOverwriteSaveMode(): Unit = {
    //create a new table
    val tableModifier1 = Map("path" -> tempBasePath, HoodieWriteConfig.TBL_NAME.key -> hoodieFooTableName,
      "hoodie.datasource.write.recordkey.field" -> "uuid")
    val dataFrame = spark.createDataFrame(Seq(StringLongTest(UUID.randomUUID().toString, new Date().getTime)))
    HoodieSparkSqlWriter.write(sqlContext, SaveMode.Overwrite, tableModifier1, dataFrame)

    //on same path try write with different RECORDKEY_FIELD_NAME and Append SaveMode should throw an exception
    val tableModifier2 = Map("path" -> tempBasePath, HoodieWriteConfig.TBL_NAME.key -> hoodieFooTableName,
      "hoodie.datasource.write.recordkey.field" -> "ts")
    val dataFrame2 = spark.createDataFrame(Seq(StringLongTest(UUID.randomUUID().toString, new Date().getTime)))
    val hoodieException = intercept[HoodieException](HoodieSparkSqlWriter.write(sqlContext, SaveMode.Append, tableModifier2, dataFrame2))
    assert(hoodieException.getMessage.contains("Config conflict"))
    assert(hoodieException.getMessage.contains(s"RecordKey:\tts\tuuid"))

    //on same path try write with different RECORDKEY_FIELD_NAME and Overwrite SaveMode should be successful.
    assert(HoodieSparkSqlWriter.write(sqlContext, SaveMode.Overwrite, tableModifier2, dataFrame2)._1)
  }

  /**
   * Test case for do not let the parttitonpath field change
   */
  @Test
  def testChangePartitionPath(): Unit = {
    //create a new table
    val tableModifier1 = Map("path" -> tempBasePath, HoodieWriteConfig.TBL_NAME.key -> hoodieFooTableName,
      "hoodie.datasource.write.recordkey.field" -> "uuid", "hoodie.datasource.write.partitionpath.field" -> "ts")
    val dataFrame = spark.createDataFrame(Seq(StringLongTest(UUID.randomUUID().toString, new Date().getTime)))
    HoodieSparkSqlWriter.write(sqlContext, SaveMode.Overwrite, tableModifier1, dataFrame)

    //on same path try write with different partitionpath field and Append SaveMode should throw an exception
    val tableModifier2 = Map("path" -> tempBasePath, HoodieWriteConfig.TBL_NAME.key -> hoodieFooTableName,
      "hoodie.datasource.write.recordkey.field" -> "uuid", "hoodie.datasource.write.partitionpath.field" -> "uuid")
    val dataFrame2 = spark.createDataFrame(Seq(StringLongTest(UUID.randomUUID().toString, new Date().getTime)))
    val hoodieException = intercept[HoodieException](HoodieSparkSqlWriter.write(sqlContext, SaveMode.Append, tableModifier2, dataFrame2))
    assert(hoodieException.getMessage.contains("Config conflict"))
    assert(hoodieException.getMessage.contains(s"PartitionPath:\tuuid\tts"))

    //on same path try write with different partitionpath and Overwrite SaveMode should be successful.
    assert(HoodieSparkSqlWriter.write(sqlContext, SaveMode.Overwrite, tableModifier2, dataFrame2)._1)
  }

  /**
   * Test case for each bulk insert sort mode
   *
   * @param sortMode Bulk insert sort mode
   */
  @ParameterizedTest
  @EnumSource(value = classOf[BulkInsertSortMode])
  def testBulkInsertForSortMode(sortMode: BulkInsertSortMode): Unit = {
    testBulkInsertWithSortMode(sortMode, populateMetaFields = true)
  }

  @Test
  def testBulkInsertForSortModeWithOCC(): Unit = {
    testBulkInsertWithSortMode(BulkInsertSortMode.GLOBAL_SORT, populateMetaFields = true, true)
  }

  /**
   * Test case for Bulk insert with populating meta fields or
   * without populating meta fields.
   *
   * @param populateMetaFields Flag for populating meta fields
   */
  @ParameterizedTest
  @ValueSource(booleans = Array(true, false))
  def testBulkInsertForPopulateMetaFields(populateMetaFields: Boolean): Unit = {
    testBulkInsertWithSortMode(BulkInsertSortMode.NONE, populateMetaFields)
  }

  /**
   * Test case for disable and enable meta fields.
   */
  @Test
  def testDisableAndEnableMetaFields(): Unit = {
    testBulkInsertWithSortMode(BulkInsertSortMode.NONE, populateMetaFields = false)
    //create a new table
    val fooTableModifier = commonTableModifier.updated("hoodie.bulkinsert.shuffle.parallelism", "4")
      .updated(DataSourceWriteOptions.OPERATION.key, DataSourceWriteOptions.BULK_INSERT_OPERATION_OPT_VAL)
      .updated(DataSourceWriteOptions.ENABLE_ROW_WRITER.key, "true")
      .updated(HoodieWriteConfig.BULK_INSERT_SORT_MODE.key(), BulkInsertSortMode.NONE.name())
      .updated(HoodieTableConfig.POPULATE_META_FIELDS.key(), "true")

    // generate the inserts
    val schema = DataSourceTestUtils.getStructTypeExampleSchema
    val structType = AvroConversionUtils.convertAvroSchemaToStructType(schema)
    val inserts = DataSourceTestUtils.generateRandomRows(1000)
    val df = spark.createDataFrame(sc.parallelize(inserts), structType)
    try {
      // write to Hudi
      HoodieSparkSqlWriter.write(sqlContext, SaveMode.Append, fooTableModifier, df)
      fail("Should have thrown exception")
    } catch {
      case e: HoodieException => assertTrue(e.getMessage.startsWith("Config conflict"))
      case e: Exception => fail(e);
    }
  }

  /**
   * Test case for drop duplicates row writing for bulk_insert.
   */
  @Test
  def testDropDuplicatesRowForBulkInsert(): Unit = {
    try {
      //create a new table
      val fooTableModifier = commonTableModifier.updated("hoodie.bulkinsert.shuffle.parallelism", "4")
        .updated(DataSourceWriteOptions.OPERATION.key, DataSourceWriteOptions.BULK_INSERT_OPERATION_OPT_VAL)
        .updated(DataSourceWriteOptions.ENABLE_ROW_WRITER.key, "true")
        .updated(INSERT_DROP_DUPS.key, "true")

      // generate the inserts
      val schema = DataSourceTestUtils.getStructTypeExampleSchema
      val structType = AvroConversionUtils.convertAvroSchemaToStructType(schema)
      val records = DataSourceTestUtils.generateRandomRows(100)
      val recordsSeq = convertRowListToSeq(records)
      val df = spark.createDataFrame(spark.sparkContext.parallelize(recordsSeq), structType)
      // write to Hudi
      HoodieSparkSqlWriter.write(sqlContext, SaveMode.Append, fooTableModifier, df)
      fail("Drop duplicates with bulk insert in row writing should have thrown exception")
    } catch {
      case e: HoodieException => assertTrue(e.getMessage.contains("Dropping duplicates with bulk_insert in row writer path is not supported yet"))
    }
  }

  /**
   * Test case for insert dataset without precombine field.
   */
  @Test
  def testInsertDatasetWithoutPrecombineField(): Unit = {

    val fooTableModifier = commonTableModifier.updated(DataSourceWriteOptions.OPERATION.key, DataSourceWriteOptions.INSERT_OPERATION_OPT_VAL)
      .updated(DataSourceWriteOptions.INSERT_DROP_DUPS.key, "false")

    // generate the inserts
    val schema = DataSourceTestUtils.getStructTypeExampleSchema
    val structType = AvroConversionUtils.convertAvroSchemaToStructType(schema)
    val records = DataSourceTestUtils.generateRandomRows(100)
    val recordsSeq = convertRowListToSeq(records)
    val df = spark.createDataFrame(sc.parallelize(recordsSeq), structType)
    // write to Hudi
    HoodieSparkSqlWriter.write(sqlContext, SaveMode.Append, fooTableModifier - DataSourceWriteOptions.PRECOMBINE_FIELD.key, df)

    // collect all partition paths to issue read of parquet files
    val partitions = Seq(HoodieTestDataGenerator.DEFAULT_FIRST_PARTITION_PATH, HoodieTestDataGenerator.DEFAULT_SECOND_PARTITION_PATH,
      HoodieTestDataGenerator.DEFAULT_THIRD_PARTITION_PATH)
    // Check the entire dataset has all records still
    val fullPartitionPaths = new Array[String](3)
    for (i <- fullPartitionPaths.indices) {
      fullPartitionPaths(i) = String.format("%s/%s/*", tempBasePath, partitions(i))
    }

    // fetch all records from parquet files generated from write to hudi
    val actualDf = spark.sqlContext.read.parquet(fullPartitionPaths(0), fullPartitionPaths(1), fullPartitionPaths(2))
    // remove metadata columns so that expected and actual DFs can be compared as is
    val trimmedDf = dropMetaFields(actualDf)
    assert(df.except(trimmedDf).count() == 0)
  }

  /**
   * Test case for insert dataset without partitioning field
   */
  @Test
  def testInsertDatasetWithoutPartitionField(): Unit = {
    val tableOpts =
      commonTableModifier
        .updated(DataSourceWriteOptions.OPERATION.key, DataSourceWriteOptions.INSERT_OPERATION_OPT_VAL)

    // generate the inserts
    val schema = DataSourceTestUtils.getStructTypeExampleSchema
    val structType = AvroConversionUtils.convertAvroSchemaToStructType(schema)
    val records = DataSourceTestUtils.generateRandomRows(1)
    val recordsSeq = convertRowListToSeq(records)
    val df = spark.createDataFrame(sc.parallelize(recordsSeq), structType)

    // try write to Hudi
    assertThrows[IOException] {
      HoodieSparkSqlWriter.write(sqlContext, SaveMode.Append, tableOpts - DataSourceWriteOptions.PARTITIONPATH_FIELD.key, df)
    }
  }

  /**
   * Test case for bulk insert dataset with datasource impl multiple rounds.
   */
  @Test
  def testBulkInsertDatasetWithDatasourceImplMultipleRounds(): Unit = {

    val fooTableModifier = commonTableModifier.updated("hoodie.bulkinsert.shuffle.parallelism", "4")
      .updated(DataSourceWriteOptions.OPERATION.key, DataSourceWriteOptions.BULK_INSERT_OPERATION_OPT_VAL)
      .updated(DataSourceWriteOptions.ENABLE_ROW_WRITER.key, "true")
    val partitions = Seq(HoodieTestDataGenerator.DEFAULT_FIRST_PARTITION_PATH, HoodieTestDataGenerator.DEFAULT_SECOND_PARTITION_PATH,
      HoodieTestDataGenerator.DEFAULT_THIRD_PARTITION_PATH)
    val fullPartitionPaths = new Array[String](3)
    for (i <- 0 to 2) {
      fullPartitionPaths(i) = String.format("%s/%s/*", tempBasePath, partitions(i))
    }
    val schema = DataSourceTestUtils.getStructTypeExampleSchema
    val structType = AvroConversionUtils.convertAvroSchemaToStructType(schema)
    var totalExpectedDf = spark.createDataFrame(sc.emptyRDD[Row], structType)
    for (_ <- 0 to 2) {
      // generate the inserts
      val records = DataSourceTestUtils.generateRandomRows(200)
      val recordsSeq = convertRowListToSeq(records)
      val df = spark.createDataFrame(sc.parallelize(recordsSeq), structType)
      // write to Hudi
      HoodieSparkSqlWriter.write(sqlContext, SaveMode.Append, fooTableModifier, df)
      // Fetch records from entire dataset
      val actualDf = sqlContext.read.parquet(fullPartitionPaths(0), fullPartitionPaths(1), fullPartitionPaths(2))
      // remove metadata columns so that expected and actual DFs can be compared as is
      val trimmedDf = dropMetaFields(actualDf)
      // find total df (union from multiple rounds)
      totalExpectedDf = totalExpectedDf.union(df)
      // find mismatch between actual and expected df
      assert(totalExpectedDf.except(trimmedDf).count() == 0)
    }
  }

  /**
   * Test cases for basic HoodieSparkSqlWriter functionality with datasource insert
   * for different tableTypes, fileFormats and options for population meta fields.
   *
   * @param tableType          Type of table
   * @param baseFileFormat     File format
   * @param populateMetaFields Flag for populating meta fields
   */
  @ParameterizedTest
  @MethodSource(Array("testDatasourceInsert"))
  def testDatasourceInsertForTableTypeBaseFileMetaFields(tableType: String, populateMetaFields: Boolean, baseFileFormat: String): Unit = {
    val hoodieFooTableName = "hoodie_foo_tbl"
    val fooTableModifier = Map("path" -> tempBasePath,
      HoodieWriteConfig.TBL_NAME.key -> hoodieFooTableName,
      HoodieWriteConfig.BASE_FILE_FORMAT.key -> baseFileFormat,
      DataSourceWriteOptions.TABLE_TYPE.key -> tableType,
      HoodieWriteConfig.INSERT_PARALLELISM_VALUE.key -> "4",
      DataSourceWriteOptions.OPERATION.key -> DataSourceWriteOptions.INSERT_OPERATION_OPT_VAL,
      DataSourceWriteOptions.RECORDKEY_FIELD.key -> "_row_key",
      DataSourceWriteOptions.PARTITIONPATH_FIELD.key -> "partition",
      HoodieTableConfig.POPULATE_META_FIELDS.key() -> String.valueOf(populateMetaFields),
      DataSourceWriteOptions.KEYGENERATOR_CLASS_NAME.key -> classOf[SimpleKeyGenerator].getCanonicalName)
    val fooTableParams = HoodieWriterUtils.parametersWithWriteDefaults(fooTableModifier)
    // generate the inserts
    val schema = DataSourceTestUtils.getStructTypeExampleSchema
    val structType = AvroConversionUtils.convertAvroSchemaToStructType(schema)
    val modifiedSchema = AvroConversionUtils.convertStructTypeToAvroSchema(structType, "trip", "example.schema")
    val records = DataSourceTestUtils.generateRandomRows(100)
    val recordsSeq = convertRowListToSeq(records)
    val df = spark.createDataFrame(sc.parallelize(recordsSeq), structType)
    initializeMetaClientForBootstrap(fooTableParams, tableType, addBootstrapPath = false, initBasePath = true)
    val client = spy(DataSourceUtils.createHoodieClient(
      new JavaSparkContext(sc), modifiedSchema.toString, tempBasePath, hoodieFooTableName,
      mapAsJavaMap(fooTableParams)).asInstanceOf[SparkRDDWriteClient[HoodieRecordPayload[Nothing]]])

    HoodieSparkSqlWriter.write(sqlContext, SaveMode.Append, fooTableModifier, df, Option.empty, Option(client))
    // Verify that asynchronous compaction is not scheduled
    verify(client, times(0)).scheduleCompaction(any())
    // Verify that HoodieWriteClient is closed correctly
    verify(client, times(1)).close()

    // collect all partition paths to issue read of parquet files
    val partitions = Seq(HoodieTestDataGenerator.DEFAULT_FIRST_PARTITION_PATH,
      HoodieTestDataGenerator.DEFAULT_SECOND_PARTITION_PATH, HoodieTestDataGenerator.DEFAULT_THIRD_PARTITION_PATH)
    // Check the entire dataset has all records still
    val fullPartitionPaths = new Array[String](3)
    for (i <- fullPartitionPaths.indices) {
      fullPartitionPaths(i) = String.format("%s/%s/*", tempBasePath, partitions(i))
    }
    // fetch all records from parquet files generated from write to hudi
    var actualDf: DataFrame = null
    if (baseFileFormat.equalsIgnoreCase(HoodieFileFormat.PARQUET.name())) {
      actualDf = sqlContext.read.parquet(fullPartitionPaths(0), fullPartitionPaths(1), fullPartitionPaths(2))
    } else if (baseFileFormat.equalsIgnoreCase(HoodieFileFormat.ORC.name())) {
      actualDf = sqlContext.read.orc(fullPartitionPaths(0), fullPartitionPaths(1), fullPartitionPaths(2))
    }
    // remove metadata columns so that expected and actual DFs can be compared as is
    val trimmedDf = dropMetaFields(actualDf)
    assert(df.except(trimmedDf).count() == 0)
  }

  /**
   * Test cases for HoodieSparkSqlWriter functionality with datasource bootstrap
   * for different type of tables.
   *
   * @param tableType Type of table
   */
  @ParameterizedTest
  @ValueSource(strings = Array("COPY_ON_WRITE", "MERGE_ON_READ"))
  def testWithDatasourceBootstrapForTableType(tableType: String): Unit = {
    val srcPath = java.nio.file.Files.createTempDirectory("hoodie_bootstrap_source_path")
    try {
      val sourceDF = TestBootstrap.generateTestRawTripDataset(Instant.now.toEpochMilli, 0, 100, Collections.emptyList(), sc,
        spark.sqlContext)
      // Write source data non-partitioned
      sourceDF.write.format("parquet").mode(SaveMode.Overwrite).save(srcPath.toAbsolutePath.toString)

      val fooTableModifier = Map("path" -> tempBasePath,
        HoodieBootstrapConfig.BASE_PATH.key -> srcPath.toAbsolutePath.toString,
        HoodieWriteConfig.TBL_NAME.key -> hoodieFooTableName,
        DataSourceWriteOptions.TABLE_TYPE.key -> tableType,
        HoodieBootstrapConfig.PARALLELISM_VALUE.key -> "4",
        DataSourceWriteOptions.OPERATION.key -> DataSourceWriteOptions.BOOTSTRAP_OPERATION_OPT_VAL,
        DataSourceWriteOptions.RECORDKEY_FIELD.key -> "_row_key",
        DataSourceWriteOptions.PARTITIONPATH_FIELD.key -> "partition",
        HoodieBootstrapConfig.KEYGEN_CLASS_NAME.key -> classOf[NonpartitionedKeyGenerator].getCanonicalName)
      val fooTableParams = HoodieWriterUtils.parametersWithWriteDefaults(fooTableModifier)
      initializeMetaClientForBootstrap(fooTableParams, tableType, addBootstrapPath = true, initBasePath = false)

      val client = spy(DataSourceUtils.createHoodieClient(
        new JavaSparkContext(sc),
        null,
        tempBasePath,
        hoodieFooTableName,
        mapAsJavaMap(fooTableParams)).asInstanceOf[SparkRDDWriteClient[HoodieRecordPayload[Nothing]]])

      HoodieSparkSqlWriter.bootstrap(sqlContext, SaveMode.Append, fooTableModifier, spark.emptyDataFrame, Option.empty,
        Option(client))

      // Verify that HoodieWriteClient is closed correctly
      verify(client, times(1)).close()

      val ignoreResult = HoodieSparkSqlWriter.bootstrap(sqlContext, SaveMode.Ignore, fooTableModifier, spark.emptyDataFrame, Option.empty,
        Option(client))
      assertFalse(ignoreResult)
      verify(client, times(2)).close()

      // fetch all records from parquet files generated from write to hudi
      val actualDf = sqlContext.read.parquet(tempBasePath)
      assert(actualDf.count == 100)
    } finally {
      FileUtils.deleteDirectory(srcPath.toFile)
    }
  }

  def initializeMetaClientForBootstrap(fooTableParams : Map[String, String], tableType: String, addBootstrapPath : Boolean, initBasePath: Boolean) : Unit = {
    // when metadata is enabled, directly instantiating write client using DataSourceUtils.createHoodieClient
    // will hit a code which tries to instantiate meta client for data table. if table does not exist, it fails.
    // hence doing an explicit instantiation here.
    val tableMetaClientBuilder = HoodieTableMetaClient.withPropertyBuilder()
      .setTableType(tableType)
      .setTableName(hoodieFooTableName)
      .setRecordKeyFields(fooTableParams(DataSourceWriteOptions.RECORDKEY_FIELD.key))
      .setBaseFileFormat(fooTableParams.getOrElse(HoodieWriteConfig.BASE_FILE_FORMAT.key,
        HoodieTableConfig.BASE_FILE_FORMAT.defaultValue().name))
      .setArchiveLogFolder(HoodieTableConfig.ARCHIVELOG_FOLDER.defaultValue())
      .setPayloadClassName(PAYLOAD_CLASS_NAME.key)
      .setPreCombineField(fooTableParams.getOrElse(PRECOMBINE_FIELD.key, PRECOMBINE_FIELD.defaultValue()))
      .setPartitionFields(fooTableParams(DataSourceWriteOptions.PARTITIONPATH_FIELD.key))
      .setKeyGeneratorClassProp(fooTableParams.getOrElse(KEYGENERATOR_CLASS_NAME.key, KEYGENERATOR_CLASS_NAME.defaultValue()))
      if(addBootstrapPath) {
        tableMetaClientBuilder
          .setBootstrapBasePath(fooTableParams(HoodieBootstrapConfig.BASE_PATH.key))
      }
    if (initBasePath) {
      tableMetaClientBuilder.initTable(sc.hadoopConfiguration, tempBasePath)
    }
  }

  /**
   * Test cases for schema evolution in different types of tables.
   *
   * @param tableType Type of table
   */
  @ParameterizedTest
  @CsvSource(value = Array(
    "COPY_ON_WRITE,true",
    "COPY_ON_WRITE,false",
    "MERGE_ON_READ,true",
    "MERGE_ON_READ,false"
  ))
  def testSchemaEvolutionForTableType(tableType: String, allowColumnDrop: Boolean): Unit = {
    val opts = getCommonParams(tempPath, hoodieFooTableName, tableType) ++ Map(
      HoodieWriteConfig.SCHEMA_ALLOW_AUTO_EVOLUTION_COLUMN_DROP.key -> allowColumnDrop.toString
    )

    // Create new table
    // NOTE: We disable Schema Reconciliation by default (such that Writer's
    //       schema is favored over existing Table's schema)
    val noReconciliationOpts = opts.updated(DataSourceWriteOptions.RECONCILE_SCHEMA.key, "false")

    // Generate 1st batch
    val schema = DataSourceTestUtils.getStructTypeExampleSchema
    val structType = AvroConversionUtils.convertAvroSchemaToStructType(schema)
    var records = DataSourceTestUtils.generateRandomRows(10)
    var recordsSeq = convertRowListToSeq(records)

    val df1 = spark.createDataFrame(sc.parallelize(recordsSeq), structType)
    HoodieSparkSqlWriter.write(sqlContext, SaveMode.Overwrite, noReconciliationOpts, df1)

    val snapshotDF1 = spark.read.format("org.apache.hudi")
      .load(tempBasePath + "/*/*/*/*")
    assertEquals(10, snapshotDF1.count())

    assertEquals(df1.except(dropMetaFields(snapshotDF1)).count(), 0)

    // Generate 2d batch (consisting of updates so that log files are created for MOR table)
    val updatesSeq = convertRowListToSeq(DataSourceTestUtils.generateUpdates(records, 5))
    val df2 = spark.createDataFrame(sc.parallelize(updatesSeq), structType)
    HoodieSparkSqlWriter.write(sqlContext, SaveMode.Append, noReconciliationOpts, df2)

    val snapshotDF2 = spark.read.format("org.apache.hudi")
      .load(tempBasePath + "/*/*/*/*")
    assertEquals(10, snapshotDF2.count())

    // Ensure 2nd batch of updates matches.
    assertEquals(df2.intersect(dropMetaFields(snapshotDF2)).except(df2).count(), 0)

    // Generate 3d batch (w/ evolved schema w/ added column)
    val evolSchema = DataSourceTestUtils.getStructTypeExampleEvolvedSchema
    val evolStructType = AvroConversionUtils.convertAvroSchemaToStructType(evolSchema)
    records = DataSourceTestUtils.generateRandomRowsEvolvedSchema(5)
    recordsSeq = convertRowListToSeq(records)

    val df3 = spark.createDataFrame(sc.parallelize(recordsSeq), evolStructType)
    // write to Hudi with new column
    HoodieSparkSqlWriter.write(sqlContext, SaveMode.Append, noReconciliationOpts, df3)

    val snapshotDF3 = spark.read.format("org.apache.hudi")
      .load(tempBasePath + "/*/*/*/*")
    assertEquals(15, snapshotDF3.count())

    // Ensure 3d batch matches
    assertEquals(df3.intersect(dropMetaFields(snapshotDF3)).except(df3).count(), 0)

    // Generate 4th batch (with a previous schema, Schema Reconciliation ENABLED)
    //
    // NOTE: This time we enable Schema Reconciliation such that the final Table's schema
    //       is reconciled b/w incoming Writer's schema (old one, no new column) and the Table's
    //       one (new one, w/ new column).
    records = DataSourceTestUtils.generateRandomRows(10)
    recordsSeq = convertRowListToSeq(records)

    val reconciliationOpts = noReconciliationOpts ++ Map(DataSourceWriteOptions.RECONCILE_SCHEMA.key -> "true")

    val df4 = spark.createDataFrame(sc.parallelize(recordsSeq), structType)
    HoodieSparkSqlWriter.write(sqlContext, SaveMode.Append, reconciliationOpts, df4)

    val snapshotDF4 = spark.read.format("org.apache.hudi")
      .load(tempBasePath + "/*/*/*/*")

    assertEquals(25, snapshotDF4.count())

    // Evolve DF4 to match against the records read back
    val reshapedDF4 = df4.withColumn("new_field", lit(null).cast("string"))

    assertEquals(reshapedDF4.intersect(dropMetaFields(snapshotDF4)).except(reshapedDF4).count, 0)

    val fourthBatchActualSchema = fetchActualSchema()
    val fourthBatchExpectedSchema = {
      val (structName, nameSpace) = AvroConversionUtils.getAvroRecordNameAndNamespace(hoodieFooTableName)
      AvroConversionUtils.convertStructTypeToAvroSchema(evolStructType, structName, nameSpace)
    }

    assertEquals(fourthBatchExpectedSchema, fourthBatchActualSchema)

    // Generate 5th batch (with a previous schema, Schema Reconciliation DISABLED)
    //
    // NOTE: This time we disable Schema Reconciliation (again) such that incoming Writer's schema is taken
    //       as the Table's new schema (de-evolving schema back to where it was before 4th batch)
    records = DataSourceTestUtils.generateRandomRows(10)
    recordsSeq = convertRowListToSeq(records)

    val df5 = spark.createDataFrame(sc.parallelize(recordsSeq), structType)

    if (allowColumnDrop) {
      HoodieSparkSqlWriter.write(sqlContext, SaveMode.Append, noReconciliationOpts, df5)

      val snapshotDF5 = spark.read.format("org.apache.hudi")
        .load(tempBasePath + "/*/*/*/*")

      assertEquals(35, snapshotDF5.count())

      assertEquals(df5.intersect(dropMetaFields(snapshotDF5)).except(df5).count, 0)

      val fifthBatchActualSchema = fetchActualSchema()
      val fifthBatchExpectedSchema = {
        val (structName, nameSpace) = AvroConversionUtils.getAvroRecordNameAndNamespace(hoodieFooTableName)
        AvroConversionUtils.convertStructTypeToAvroSchema(df5.schema, structName, nameSpace)
      }

      assertEquals(fifthBatchExpectedSchema, fifthBatchActualSchema)
    } else {
      assertThrows[SchemaCompatibilityException] {
        HoodieSparkSqlWriter.write(sqlContext, SaveMode.Append, noReconciliationOpts, df5)
      }
    }
  }

  /**
   * Test case for incremental view with replacement.
   */
  @Test
  def testIncrementalViewWithReplacement(): Unit = {
    List(DataSourceWriteOptions.COW_TABLE_TYPE_OPT_VAL, DataSourceWriteOptions.MOR_TABLE_TYPE_OPT_VAL).foreach { tableType =>
      val baseBootStrapPath = tempBootStrapPath.toAbsolutePath.toString
      val options = Map(DataSourceWriteOptions.TABLE_TYPE.key -> tableType,
        DataSourceWriteOptions.PRECOMBINE_FIELD.key -> "col3",
        DataSourceWriteOptions.RECORDKEY_FIELD.key -> "keyid",
        DataSourceWriteOptions.PARTITIONPATH_FIELD.key -> "",
        DataSourceWriteOptions.KEYGENERATOR_CLASS_NAME.key -> "org.apache.hudi.keygen.NonpartitionedKeyGenerator",
        HoodieWriteConfig.TBL_NAME.key -> "hoodie_test")
      val df = spark.range(0, 1000).toDF("keyid")
        .withColumn("col3", expr("keyid"))
        .withColumn("age", lit(1))
        .withColumn("p", lit(2))

      df.write.format("hudi")
        .options(options)
        .option(DataSourceWriteOptions.OPERATION.key, "insert")
        .option("hoodie.insert.shuffle.parallelism", "4")
        .mode(SaveMode.Overwrite).save(tempBasePath)

      df.write.format("hudi")
        .options(options)
        .option(DataSourceWriteOptions.OPERATION.key, "insert_overwrite_table")
        .option("hoodie.insert.shuffle.parallelism", "4")
        .mode(SaveMode.Append).save(tempBasePath)

      val currentCommits = spark.read.format("hudi")
        .load(tempBasePath).select("_hoodie_commit_time").take(1).map(_.getString(0))
      val incrementalKeyIdNum = spark.read.format("hudi")
        .option(DataSourceReadOptions.QUERY_TYPE.key, DataSourceReadOptions.QUERY_TYPE_INCREMENTAL_OPT_VAL)
        .option(DataSourceReadOptions.BEGIN_INSTANTTIME.key, "0000")
        .option(DataSourceReadOptions.END_INSTANTTIME.key, currentCommits(0))
        .load(tempBasePath).select("keyid").orderBy("keyid").count
      assert(incrementalKeyIdNum == 1000)

      df.write.mode(SaveMode.Overwrite).save(baseBootStrapPath)
      spark.emptyDataFrame.write.format("hudi")
        .options(options)
        .option(HoodieBootstrapConfig.BASE_PATH.key, baseBootStrapPath)
        .option(HoodieBootstrapConfig.KEYGEN_CLASS_NAME.key, classOf[NonpartitionedKeyGenerator].getCanonicalName)
        .option(DataSourceWriteOptions.OPERATION.key, DataSourceWriteOptions.BOOTSTRAP_OPERATION_OPT_VAL)
        .option(HoodieBootstrapConfig.PARALLELISM_VALUE.key, "4")
        .mode(SaveMode.Overwrite).save(tempBasePath)
      df.write.format("hudi").options(options)
        .option(DataSourceWriteOptions.OPERATION.key, "insert_overwrite_table")
        .option("hoodie.insert.shuffle.parallelism", "4").mode(SaveMode.Append).save(tempBasePath)
      val currentCommitsBootstrap = spark.read.format("hudi")
        .load(tempBasePath).select("_hoodie_commit_time").take(1).map(_.getString(0))
      val incrementalKeyIdNumBootstrap = spark.read.format("hudi")
        .option(DataSourceReadOptions.QUERY_TYPE.key, DataSourceReadOptions.QUERY_TYPE_INCREMENTAL_OPT_VAL)
        .option(DataSourceReadOptions.BEGIN_INSTANTTIME.key, "0000")
        .option(DataSourceReadOptions.END_INSTANTTIME.key, currentCommitsBootstrap(0))
        .load(tempBasePath).select("keyid").orderBy("keyid").count
      assert(incrementalKeyIdNumBootstrap == 1000)
    }
  }

/**
 * Helper function for setting up table that has 3 different partitions
 * Used to test deleting partitions
 * @return dataframe to be used by testDeletePartitionsV2 and a map containing the table params
 */
  def deletePartitionSetup(): (DataFrame, Map[String,String]) = {
    var fooTableModifier = getCommonParams(tempPath, hoodieFooTableName, HoodieTableType.COPY_ON_WRITE.name())
    val schema = DataSourceTestUtils.getStructTypeExampleSchema
    val structType = AvroConversionUtils.convertAvroSchemaToStructType(schema)
    val records = DataSourceTestUtils.generateRandomRows(10)
    val recordsSeq = convertRowListToSeq(records)
    val df1 = spark.createDataFrame(sc.parallelize(recordsSeq), structType)
    // write to Hudi
    HoodieSparkSqlWriter.write(sqlContext, SaveMode.Overwrite, fooTableModifier, df1)
    val snapshotDF1 = spark.read.format("org.apache.hudi")
      .load(tempBasePath + "/*/*/*/*")
    assertEquals(10, snapshotDF1.count())
    // remove metadata columns so that expected and actual DFs can be compared as is
    val trimmedDf1 = dropMetaFields(snapshotDF1)
    assert(df1.except(trimmedDf1).count() == 0)
    // issue updates so that log files are created for MOR table
    val updatesSeq = convertRowListToSeq(DataSourceTestUtils.generateUpdates(records, 5))
    val updatesDf = spark.createDataFrame(sc.parallelize(updatesSeq), structType)
    // write updates to Hudi
    HoodieSparkSqlWriter.write(sqlContext, SaveMode.Append, fooTableModifier, updatesDf)
    val snapshotDF2 = spark.read.format("org.apache.hudi")
      .load(tempBasePath + "/*/*/*/*")
    assertEquals(10, snapshotDF2.count())
    // remove metadata columns so that expected and actual DFs can be compared as is
    val trimmedDf2 = dropMetaFields(snapshotDF2)
    // ensure 2nd batch of updates matches.
    assert(updatesDf.intersect(trimmedDf2).except(updatesDf).count() == 0)
    (df1, fooTableModifier)
  }

  /**
   * Test case for deletion of partitions.
   * @param usePartitionsToDeleteConfig Flag for if use partitions to delete config
   */
  @ParameterizedTest
  @ValueSource(booleans = Array(true, false))
  def testDeletePartitionsV2(usePartitionsToDeleteConfig: Boolean): Unit = {
    var (df1, fooTableModifier) = deletePartitionSetup()
    var recordsToDelete = spark.emptyDataFrame
    if (usePartitionsToDeleteConfig) {
      fooTableModifier = fooTableModifier.updated(DataSourceWriteOptions.PARTITIONS_TO_DELETE.key(),
        HoodieTestDataGenerator.DEFAULT_FIRST_PARTITION_PATH + "," + HoodieTestDataGenerator.DEFAULT_SECOND_PARTITION_PATH)
    } else {
      // delete partitions contains the primary key
      recordsToDelete = df1.filter(entry => {
        val partitionPath: String = entry.getString(1)
        partitionPath.equals(HoodieTestDataGenerator.DEFAULT_FIRST_PARTITION_PATH) ||
          partitionPath.equals(HoodieTestDataGenerator.DEFAULT_SECOND_PARTITION_PATH)
      })
    }

    fooTableModifier = fooTableModifier.updated(DataSourceWriteOptions.OPERATION.key(), WriteOperationType.DELETE_PARTITION.name())
    HoodieSparkSqlWriter.write(sqlContext, SaveMode.Append, fooTableModifier, recordsToDelete)
    val snapshotDF3 = spark.read.format("org.apache.hudi")
      .load(tempBasePath + "/*/*/*/*")
    assertEquals(0, snapshotDF3.filter(entry => {
      val partitionPath = entry.getString(3)
      !partitionPath.equals(HoodieTestDataGenerator.DEFAULT_THIRD_PARTITION_PATH)
    }).count())
  }

  /**
   * Test case for deletion of partitions using wildcards
   * @param partition the name of the partition(s) to delete
   */
  @ParameterizedTest
  @MethodSource(Array(
    "deletePartitionsWildcardTestParams"
  ))
  def testDeletePartitionsWithWildcard(partition: String, expectedPartitions: Seq[String]): Unit = {
    var (_, fooTableModifier) = deletePartitionSetup()
    fooTableModifier = fooTableModifier.updated(DataSourceWriteOptions.PARTITIONS_TO_DELETE.key(), partition)
    fooTableModifier = fooTableModifier.updated(DataSourceWriteOptions.OPERATION.key(), WriteOperationType.DELETE_PARTITION.name())
    val recordsToDelete = spark.emptyDataFrame
    HoodieSparkSqlWriter.write(sqlContext, SaveMode.Append, fooTableModifier, recordsToDelete)
    val snapshotDF3 = spark.read.format("org.apache.hudi")
      .load(tempBasePath + "/*/*/*/*")
    snapshotDF3.show()
    assertEquals(0, snapshotDF3.filter(entry => {
      val partitionPath = entry.getString(3)
      expectedPartitions.count(p => partitionPath.equals(p)) != 1
    }).count())
  }

  /**
   * Test case for non partition table with metatable support.
   */
  @ParameterizedTest
  @EnumSource(value = classOf[HoodieTableType])
  def testNonPartitionTableWithMetatableSupport(tableType: HoodieTableType): Unit = {
    val options = Map(DataSourceWriteOptions.TABLE_TYPE.key -> tableType.name,
      DataSourceWriteOptions.PRECOMBINE_FIELD.key -> "col3",
      DataSourceWriteOptions.RECORDKEY_FIELD.key -> "keyid",
      DataSourceWriteOptions.PARTITIONPATH_FIELD.key -> "",
      DataSourceWriteOptions.KEYGENERATOR_CLASS_NAME.key -> "org.apache.hudi.keygen.NonpartitionedKeyGenerator",
      HoodieWriteConfig.TBL_NAME.key -> "hoodie_test",
      "hoodie.insert.shuffle.parallelism" -> "1",
      "hoodie.metadata.enable" -> "true")
    val df = spark.range(0, 10).toDF("keyid")
      .withColumn("col3", expr("keyid"))
      .withColumn("age", expr("keyid + 1000"))
    df.write.format("hudi")
      .options(options.updated(DataSourceWriteOptions.OPERATION.key, "insert"))
      .mode(SaveMode.Overwrite).save(tempBasePath)
    // upsert same record again
    val df_update = spark.range(0, 10).toDF("keyid")
      .withColumn("col3", expr("keyid"))
      .withColumn("age", expr("keyid + 2000"))
    df_update.write.format("hudi")
      .options(options.updated(DataSourceWriteOptions.OPERATION.key, "upsert"))
      .mode(SaveMode.Append).save(tempBasePath)
    val df_result = spark.read.format("hudi").load(tempBasePath)
    assert(df_result.count() == 10)
    assert(df_result.where("age >= 2000").count() == 10)
  }

  /**
   * Test case for no need to specify hiveStylePartitioning/urlEncodePartitioning/KeyGenerator included in HoodieTableConfig except the first time write
   */
  @Test
  def testToWriteWithoutParametersIncludedInHoodieTableConfig(): Unit = {
    val _spark = spark
    import _spark.implicits._
    val df = Seq((1, "a1", 10, 1000L, "2021-10-16")).toDF("id", "name", "value", "ts", "dt")
    val options = Map(
      DataSourceWriteOptions.RECORDKEY_FIELD.key -> "id",
      DataSourceWriteOptions.PRECOMBINE_FIELD.key -> "ts",
      DataSourceWriteOptions.PARTITIONPATH_FIELD.key -> "dt"
    )

    // case 1: test table which created by sql
    val (tableName1, tablePath1) = ("hoodie_test_params_1", s"$tempBasePath" + "_1")
    spark.sql(
      s"""
         | create table $tableName1 (
         |   id int,
         |   name string,
         |   value int,
         |   ts long,
         |   dt string
         | ) using hudi
         | partitioned by (dt)
         | options (
         |  primaryKey = 'id',
         |  preCombineField = 'ts'
         | )
         | location '$tablePath1'
       """.stripMargin)
    val tableConfig1 = HoodieTableMetaClient.builder()
      .setConf(spark.sparkContext.hadoopConfiguration)
      .setBasePath(tablePath1).build().getTableConfig
    assert(tableConfig1.getHiveStylePartitioningEnable == "true")
    assert(tableConfig1.getUrlEncodePartitioning == "false")
    assert(tableConfig1.getKeyGeneratorClassName == classOf[SimpleKeyGenerator].getName)
    df.write.format("hudi")
      .options(options)
      .option(HoodieWriteConfig.TBL_NAME.key, tableName1)
      .mode(SaveMode.Append).save(tablePath1)
    val hudiDf = spark.read.format("hudi").load(tablePath1)
    assert(hudiDf.count() == 1)

    // case 2: test table which created by dataframe
    val (tableName2, tablePath2) = ("hoodie_test_params_2", s"$tempBasePath" + "_2")
    // the first write need to specify params
    df.write.format("hudi")
      .options(options)
      .option(HoodieWriteConfig.TBL_NAME.key, tableName2)
      .option(DataSourceWriteOptions.URL_ENCODE_PARTITIONING.key, "true")
      .option(HoodieWriteConfig.KEYGENERATOR_CLASS_NAME.key, classOf[SimpleKeyGenerator].getName)
      .mode(SaveMode.Overwrite).save(tablePath2)
    val tableConfig2 = HoodieTableMetaClient.builder()
      .setConf(spark.sparkContext.hadoopConfiguration)
      .setBasePath(tablePath2).build().getTableConfig
    assert(tableConfig2.getHiveStylePartitioningEnable == "false")
    assert(tableConfig2.getUrlEncodePartitioning == "true")
    assert(tableConfig2.getKeyGeneratorClassName == classOf[SimpleKeyGenerator].getName)

    val df2 = Seq((2, "a2", 20, 1000L, "2021-10-16")).toDF("id", "name", "value", "ts", "dt")
    // raise exception when use params which is not same with HoodieTableConfig
    val configConflictException = intercept[HoodieException] {
      df2.write.format("hudi")
        .options(options)
        .option(HoodieWriteConfig.TBL_NAME.key, tableName2)
        .option(HoodieWriteConfig.KEYGENERATOR_CLASS_NAME.key, classOf[ComplexKeyGenerator].getName)
        .mode(SaveMode.Append).save(tablePath2)
    }
    assert(configConflictException.getMessage.contains("Config conflict"))
    assert(configConflictException.getMessage.contains(s"KeyGenerator:\t${classOf[ComplexKeyGenerator].getName}\t${classOf[SimpleKeyGenerator].getName}"))

    // do not need to specify hiveStylePartitioning/urlEncodePartitioning/KeyGenerator params
    df2.write.format("hudi")
      .options(options)
      .option(HoodieWriteConfig.TBL_NAME.key, tableName2)
      .mode(SaveMode.Append).save(tablePath2)
    val data = spark.read.format("hudi").load(tablePath2)
    assert(data.count() == 2)
    assert(data.select("_hoodie_partition_path").map(_.getString(0)).distinct.collect.head == "2021-10-16")
  }

  @Test
  def testNonpartitonedWithReuseTableConfig(): Unit = {
    val _spark = spark
    import _spark.implicits._
    val df = Seq((1, "a1", 10, 1000, "2021-10-16")).toDF("id", "name", "value", "ts", "dt")
    val options = Map(
      DataSourceWriteOptions.RECORDKEY_FIELD.key -> "id",
      DataSourceWriteOptions.PRECOMBINE_FIELD.key -> "ts"
    )

    // case 1: When commit C1 specifies a key generator and commit C2 does not specify key generator
    val (tableName1, tablePath1) = ("hoodie_test_params_1", s"$tempBasePath" + "_1")

    // the first write need to specify KEYGENERATOR_CLASS_NAME params
    df.write.format("hudi")
      .options(options)
      .option(HoodieWriteConfig.TBL_NAME.key, tableName1)
      .option(HoodieWriteConfig.KEYGENERATOR_CLASS_NAME.key, classOf[NonpartitionedKeyGenerator].getName)
      .mode(SaveMode.Overwrite).save(tablePath1)

    val df2 = Seq((2, "a2", 20, 1000, "2021-10-16")).toDF("id", "name", "value", "ts", "dt")
    // In first commit, we explicitly over-ride it to Nonpartitioned, where as in 2nd batch, since re-using of table configs
    // come into play, no exception should be thrown even if we don't supply any key gen class.
    df2.write.format("hudi")
      .options(options)
      .option(HoodieWriteConfig.TBL_NAME.key, tableName1)
      .mode(SaveMode.Append).save(tablePath1)
  }

  @Test
  def testDefaultKeyGenToNonpartitoned(): Unit = {
    val _spark = spark
    import _spark.implicits._
    val df = Seq((1, "a1", 10, 1000, "2021-10-16")).toDF("id", "name", "value", "ts", "dt")
    val options = Map(
      DataSourceWriteOptions.RECORDKEY_FIELD.key -> "id",
      DataSourceWriteOptions.PRECOMBINE_FIELD.key -> "ts",
      DataSourceWriteOptions.PARTITIONPATH_FIELD.key -> "dt"
    )

    // case 1: When commit C1 does not specify key generator and commit C2 specifies a key generator
    val (tableName1, tablePath1) = ("hoodie_test_params_1", s"$tempBasePath" + "_1")

    // the first write need to specify KEYGENERATOR_CLASS_NAME params
    df.write.format("hudi")
      .options(options)
      .option(HoodieWriteConfig.TBL_NAME.key, tableName1)
      .mode(SaveMode.Overwrite).save(tablePath1)

    val df2 = Seq((2, "a2", 20, 1000, "2021-10-16")).toDF("id", "name", "value", "ts", "dt")
    // raise exception when NonpartitionedKeyGenerator is specified
    val configConflictException = intercept[HoodieException] {
      df2.write.format("hudi")
        .options(options)
        .option(HoodieWriteConfig.TBL_NAME.key, tableName1)
        .option(HoodieWriteConfig.KEYGENERATOR_CLASS_NAME.key, classOf[NonpartitionedKeyGenerator].getName)
        .mode(SaveMode.Append).save(tablePath1)
    }
    assert(configConflictException.getMessage.contains("Config conflict"))
    assert(configConflictException.getMessage.contains(s"KeyGenerator:\t${classOf[NonpartitionedKeyGenerator].getName}\t${classOf[SimpleKeyGenerator].getName}"))
  }


  @Test
  def testNoKeyGenToSimpleKeyGen(): Unit = {
    val _spark = spark
    import _spark.implicits._
    val df = Seq((1, "a1", 10, 1000, "2021-10-16")).toDF("id", "name", "value", "ts", "dt")
    val options = Map(
      DataSourceWriteOptions.RECORDKEY_FIELD.key -> "id",
      DataSourceWriteOptions.PRECOMBINE_FIELD.key -> "ts",
      DataSourceWriteOptions.PARTITIONPATH_FIELD.key -> "dt"
    )

    // case 1: When commit C1 specifies a key generator and commkt C2 does not specify key generator
    val (tableName1, tablePath1) = ("hoodie_test_params_1", s"$tempBasePath" + "_1")

    // the first write need to specify KEYGENERATOR_CLASS_NAME params
    df.write.format("hudi")
      .options(options)
      .option(HoodieWriteConfig.TBL_NAME.key, tableName1)
      .mode(SaveMode.Overwrite).save(tablePath1)

    val df2 = Seq((2, "a2", 20, 1000, "2021-10-16")).toDF("id", "name", "value", "ts", "dt")
    // No Exception Should be raised
    try {
      df2.write.format("hudi")
        .options(options)
        .option(HoodieWriteConfig.TBL_NAME.key, tableName1)
        .option(HoodieWriteConfig.KEYGENERATOR_CLASS_NAME.key, classOf[SimpleKeyGenerator].getName)
        .mode(SaveMode.Append).save(tablePath1)
    } catch {
      case _ => fail("Switching from no keygen to explicit SimpleKeyGenerator should not fail");
    }
  }

  @Test
  def testSimpleKeyGenToNoKeyGen(): Unit = {
    val _spark = spark
    import _spark.implicits._
    val df = Seq((1, "a1", 10, 1000, "2021-10-16")).toDF("id", "name", "value", "ts", "dt")
    val options = Map(
      DataSourceWriteOptions.RECORDKEY_FIELD.key -> "id",
      DataSourceWriteOptions.PRECOMBINE_FIELD.key -> "ts",
      DataSourceWriteOptions.PARTITIONPATH_FIELD.key -> "dt"
    )

    // case 1: When commit C1 specifies a key generator and commkt C2 does not specify key generator
    val (tableName1, tablePath1) = ("hoodie_test_params_1", s"$tempBasePath" + "_1")

    // the first write need to specify KEYGENERATOR_CLASS_NAME params
    df.write.format("hudi")
      .options(options)
      .option(HoodieWriteConfig.TBL_NAME.key, tableName1)
      .option(HoodieWriteConfig.KEYGENERATOR_CLASS_NAME.key, classOf[SimpleKeyGenerator].getName)
      .mode(SaveMode.Overwrite).save(tablePath1)

    val df2 = Seq((2, "a2", 20, 1000, "2021-10-16")).toDF("id", "name", "value", "ts", "dt")
    // No Exception Should be raised when default keygen is used
    try {
      df2.write.format("hudi")
        .options(options)
        .option(HoodieWriteConfig.TBL_NAME.key, tableName1)
        .mode(SaveMode.Append).save(tablePath1)
    } catch {
      case _ => fail("Switching from  explicit SimpleKeyGenerator to default keygen should not fail");
    }
  }

  @Test
  def testGetOriginKeyGenerator(): Unit = {
    // for dataframe write
    val m1 = Map(
      HoodieWriteConfig.KEYGENERATOR_CLASS_NAME.key -> classOf[ComplexKeyGenerator].getName
    )
    val kg1 = HoodieWriterUtils.getOriginKeyGenerator(m1)
    assertTrue(kg1 == classOf[ComplexKeyGenerator].getName)

    // for sql write
    val m2 = Map(
      HoodieWriteConfig.KEYGENERATOR_CLASS_NAME.key -> classOf[SqlKeyGenerator].getName,
      SqlKeyGenerator.ORIGINAL_KEYGEN_CLASS_NAME -> classOf[SimpleKeyGenerator].getName
    )
    val kg2 = HoodieWriterUtils.getOriginKeyGenerator(m2)
    assertTrue(kg2 == classOf[SimpleKeyGenerator].getName)
  }

  /**
   *
   * Test that you can't have consistent hashing bucket index on a COW table
   * */
  @Test
  def testCOWConsistentHashing(): Unit = {
    val _spark = spark
    import _spark.implicits._
    val df = Seq((1, "a1", 10, 1000, "2021-10-16")).toDF("id", "name", "value", "ts", "dt")
    val options = Map(
      DataSourceWriteOptions.RECORDKEY_FIELD.key -> "id",
      DataSourceWriteOptions.PRECOMBINE_FIELD.key -> "ts",
      DataSourceWriteOptions.PARTITIONPATH_FIELD.key -> "dt",
      HoodieIndexConfig.BUCKET_INDEX_ENGINE_TYPE.key -> "CONSISTENT_HASHING",
      HoodieIndexConfig.INDEX_TYPE.key -> "BUCKET"
    )

    val (tableName1, tablePath1) = ("hoodie_test_params_1", s"$tempBasePath" + "_1")
    val exc = intercept[HoodieException] {
      df.write.format("hudi")
        .options(options)
        .option(HoodieWriteConfig.TBL_NAME.key, tableName1)
        .option(HoodieWriteConfig.KEYGENERATOR_CLASS_NAME.key, classOf[NonpartitionedKeyGenerator].getName)
        .mode(SaveMode.Overwrite).save(tablePath1)
    }
    assert(exc.getMessage.contains("Consistent hashing bucket index does not work with COW table. Use simple bucket index or an MOR table."))
  }

  private def fetchActualSchema(): Schema = {
    val tableMetaClient = HoodieTableMetaClient.builder()
      .setConf(spark.sparkContext.hadoopConfiguration)
      .setBasePath(tempBasePath)
      .build()
    new TableSchemaResolver(tableMetaClient).getTableAvroSchema(false)
  }
}

object TestHoodieSparkSqlWriter {
  def testDatasourceInsert: java.util.stream.Stream[Arguments] = {
    val scenarios = Array(
      Seq("COPY_ON_WRITE", true),
      Seq("COPY_ON_WRITE", false),
      Seq("MERGE_ON_READ", true),
      Seq("MERGE_ON_READ", false)
    )

    val parquetScenarios = scenarios.map { _ :+ "parquet" }
    val orcScenarios = scenarios.map { _ :+ "orc" }

    // NOTE: Hudi doesn't support Orc in Spark < 3.0
    //       Please check HUDI-4496 for more details
    val targetScenarios = if (gteqSpark3_0) {
      parquetScenarios ++ orcScenarios
    } else {
      parquetScenarios
    }

    java.util.Arrays.stream(targetScenarios.map(as => arguments(as.map(_.asInstanceOf[AnyRef]):_*)))
  }

  def deletePartitionsWildcardTestParams(): java.util.stream.Stream[Arguments] = {
    java.util.stream.Stream.of(
      arguments("2015/03/*", Seq("2016/03/15")),
      arguments("*5/03/1*", Seq("2016/03/15")),
      arguments("2016/03/*", Seq("2015/03/16", "2015/03/17")))
  }

}
