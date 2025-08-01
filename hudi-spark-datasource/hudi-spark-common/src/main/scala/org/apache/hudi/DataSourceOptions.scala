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

import org.apache.hudi.DataSourceReadOptions.{QUERY_TYPE, QUERY_TYPE_READ_OPTIMIZED_OPT_VAL, QUERY_TYPE_SNAPSHOT_OPT_VAL}
import org.apache.hudi.HoodieConversionUtils.toScalaOption
import org.apache.hudi.common.config._
import org.apache.hudi.common.fs.ConsistencyGuardConfig
import org.apache.hudi.common.model.{HoodieTableType, WriteOperationType}
import org.apache.hudi.common.table.HoodieTableConfig
import org.apache.hudi.common.util.{Option, StringUtils}
import org.apache.hudi.common.util.ConfigUtils.{DELTA_STREAMER_CONFIG_PREFIX, IS_QUERY_AS_RO_TABLE, STREAMER_CONFIG_PREFIX}
import org.apache.hudi.common.util.ValidationUtils.checkState
import org.apache.hudi.config.{HoodieClusteringConfig, HoodieWriteConfig}
import org.apache.hudi.hive.{HiveSyncConfig, HiveSyncConfigHolder, HiveSyncTool}
import org.apache.hudi.keygen.{CustomKeyGenerator, NonpartitionedKeyGenerator, SimpleKeyGenerator}
import org.apache.hudi.keygen.KeyGenUtils.inferKeyGeneratorType
import org.apache.hudi.keygen.constant.KeyGeneratorOptions
import org.apache.hudi.keygen.factory.HoodieSparkKeyGeneratorFactory.{getKeyGeneratorClassNameFromType, inferKeyGeneratorTypeFromWriteConfig}
import org.apache.hudi.sync.common.HoodieSyncConfig
import org.apache.hudi.util.{JFunction, SparkConfigUtils}

import org.apache.spark.sql.execution.datasources.{DataSourceUtils => SparkDataSourceUtils}
import org.slf4j.LoggerFactory

import scala.collection.JavaConverters._
import scala.language.implicitConversions

/**
 * List of options that can be passed to the Hoodie datasource,
 * in addition to the hoodie client configs
 */

/**
 * Options supported for reading hoodie tables.
 */
object DataSourceReadOptions {

  val QUERY_TYPE_SNAPSHOT_OPT_VAL = "snapshot"
  val QUERY_TYPE_READ_OPTIMIZED_OPT_VAL = "read_optimized"
  val QUERY_TYPE_INCREMENTAL_OPT_VAL = "incremental"
  val QUERY_TYPE: ConfigProperty[String] = ConfigProperty
    .key("hoodie.datasource.query.type")
    .defaultValue(QUERY_TYPE_SNAPSHOT_OPT_VAL)
    .withAlternatives("hoodie.datasource.view.type")
    .withValidValues(QUERY_TYPE_SNAPSHOT_OPT_VAL, QUERY_TYPE_READ_OPTIMIZED_OPT_VAL, QUERY_TYPE_INCREMENTAL_OPT_VAL)
    .withDocumentation("Whether data needs to be read, in `" + QUERY_TYPE_INCREMENTAL_OPT_VAL + "` mode (new data since an instantTime) " +
      "(or) `" + QUERY_TYPE_READ_OPTIMIZED_OPT_VAL + "` mode (obtain latest view, based on base files) (or) `" + QUERY_TYPE_SNAPSHOT_OPT_VAL + "` mode " +
      "(obtain latest view, by merging base and (if any) log files)")

  val INCREMENTAL_FORMAT_LATEST_STATE_VAL = "latest_state"
  val INCREMENTAL_FORMAT_CDC_VAL = "cdc"
  val INCREMENTAL_FORMAT: ConfigProperty[String] = ConfigProperty
    .key("hoodie.datasource.query.incremental.format")
    .defaultValue(INCREMENTAL_FORMAT_LATEST_STATE_VAL)
    .withValidValues(INCREMENTAL_FORMAT_LATEST_STATE_VAL, INCREMENTAL_FORMAT_CDC_VAL)
    .markAdvanced()
    .sinceVersion("0.13.0")
    .withDocumentation("This config is used alone with the 'incremental' query type." +
      "When set to 'latest_state', it returns the latest records' values." +
      "When set to 'cdc', it returns the cdc data.")

  val REALTIME_SKIP_MERGE_OPT_VAL = HoodieReaderConfig.REALTIME_SKIP_MERGE
  val REALTIME_PAYLOAD_COMBINE_OPT_VAL = HoodieReaderConfig.REALTIME_PAYLOAD_COMBINE
  val REALTIME_MERGE: ConfigProperty[String] = HoodieReaderConfig.MERGE_TYPE

  val READ_PATHS: ConfigProperty[String] = ConfigProperty
    .key("hoodie.datasource.read.paths")
    .noDefaultValue()
    .markAdvanced()
    .withDocumentation("Comma separated list of file paths to read within a Hudi table.")

  val READ_PRE_COMBINE_FIELD = HoodieWriteConfig.PRECOMBINE_FIELD_NAME

  val ENABLE_HOODIE_FILE_INDEX: ConfigProperty[Boolean] = ConfigProperty
    .key("hoodie.file.index.enable")
    .defaultValue(true)
    .markAdvanced()
    .deprecatedAfter("0.11.0")
    .withDocumentation("Enables use of the spark file index implementation for Hudi, "
      + "that speeds up listing of large tables.")

  val START_OFFSET: ConfigProperty[String] = ConfigProperty
    .key("hoodie.datasource.streaming.startOffset")
    .defaultValue("earliest")
    .markAdvanced()
    .sinceVersion("0.13.0")
    .withDocumentation("Start offset to pull data from hoodie streaming source. allow earliest, latest, and " +
      "specified start instant time")

  val START_COMMIT: ConfigProperty[String] = ConfigProperty
    .key("hoodie.datasource.read.begin.instanttime")
    .noDefaultValue()
    .withDocumentation("Required when `" + QUERY_TYPE.key() + "` is set to `" + QUERY_TYPE_INCREMENTAL_OPT_VAL + "`. "
      + "Represents the completion time to start incrementally pulling data from. The completion time here need not necessarily "
      + "correspond to an instant on the timeline. New data written with completion_time >= START_COMMIT are fetched out. "
      + "For e.g: ‘20170901080000’ will get all new data written on or after Sep 1, 2017 08:00AM.")

  val END_COMMIT: ConfigProperty[String] = ConfigProperty
    .key("hoodie.datasource.read.end.instanttime")
    .noDefaultValue()
    .withDocumentation("Used when `" + QUERY_TYPE.key() + "` is set to `" + QUERY_TYPE_INCREMENTAL_OPT_VAL
      + "`. Represents the completion time to limit incrementally fetched data to. When not specified latest commit "
      + "completion time from timeline is assumed by default. When specified, new data written with "
      + "completion_time <= END_COMMIT are fetched out. "
      + "Point in time type queries make more sense with begin and end completion times specified.")

  val STREAMING_READ_TABLE_VERSION: ConfigProperty[String] = ConfigProperty
    .key("hoodie.datasource.read.streaming.table.version")
    .noDefaultValue()
    .withDocumentation("The table version assumed for streaming read")

  val INCREMENTAL_READ_TABLE_VERSION: ConfigProperty[String] = ConfigProperty
    .key("hoodie.datasource.read.incr.table.version")
    .noDefaultValue()
    .withDocumentation("The table version assumed for incremental read")

  val INCREMENTAL_READ_SCHEMA_USE_END_INSTANTTIME: ConfigProperty[String] = ConfigProperty
    .key("hoodie.datasource.read.schema.use.end.instanttime")
    .defaultValue("false")
    .markAdvanced()
    .withDocumentation("Uses end instant schema when incrementally fetched data to. Default: users latest instant schema.")

  val PUSH_DOWN_INCR_FILTERS: ConfigProperty[String] = ConfigProperty
    .key("hoodie.datasource.read.incr.filters")
    .defaultValue("")
    .markAdvanced()
    .withDocumentation("For use-cases like DeltaStreamer which reads from Hoodie Incremental table and applies "
      + "opaque map functions, filters appearing late in the sequence of transformations cannot be automatically "
      + "pushed down. This option allows setting filters directly on Hoodie Source.")

  val INCR_PATH_GLOB: ConfigProperty[String] = ConfigProperty
    .key("hoodie.datasource.read.incr.path.glob")
    .defaultValue("")
    .markAdvanced()
    .withDocumentation("For the use-cases like users only want to incremental pull from certain partitions "
      + "instead of the full table. This option allows using glob pattern to directly filter on path.")

  val INCREMENTAL_READ_SKIP_COMPACT: ConfigProperty[Boolean] = ConfigProperty
    .key("hoodie.datasource.read.incr.skip_compact")
    .defaultValue(false)
    .markAdvanced()
    .withDocumentation("Whether to skip compaction instants and avoid reading compacted base files for streaming "
      + "read to improve read performance.")

  val INCREMENTAL_READ_SKIP_CLUSTER: ConfigProperty[Boolean] = ConfigProperty
    .key("hoodie.datasource.read.incr.skip_cluster")
    .defaultValue(false)
    .markAdvanced()
    .withDocumentation("Whether to skip clustering instants to avoid reading base files of clustering operations "
      + "for streaming read to improve read performance.")

  val TIME_TRAVEL_AS_OF_INSTANT: ConfigProperty[String] = HoodieCommonConfig.TIMESTAMP_AS_OF

  val ENABLE_DATA_SKIPPING: ConfigProperty[Boolean] = ConfigProperty
    .key("hoodie.enable.data.skipping")
    .defaultValue(true)
    .markAdvanced()
    .sinceVersion("0.10.0")
    .withDocumentation("Enables data-skipping allowing queries to leverage indexes to reduce the search space by " +
      "skipping over files")

  val EXTRACT_PARTITION_VALUES_FROM_PARTITION_PATH: ConfigProperty[Boolean] =
    ConfigProperty.key("hoodie.datasource.read.extract.partition.values.from.path")
      .defaultValue(false)
      .markAdvanced()
      .sinceVersion("0.11.0")
      .withDocumentation("When set to true, values for partition columns (partition values) will be extracted" +
        " from physical partition path (default Spark behavior). When set to false partition values will be" +
        " read from the data file (in Hudi partition columns are persisted by default)." +
        " This config is a fallback allowing to preserve existing behavior, and should not be used otherwise.")

  val FILE_INDEX_LISTING_MODE_EAGER = "eager"
  val FILE_INDEX_LISTING_MODE_LAZY = "lazy"

  val FILE_INDEX_LISTING_MODE_OVERRIDE: ConfigProperty[String] =
    ConfigProperty.key("hoodie.datasource.read.file.index.listing.mode")
      .defaultValue(FILE_INDEX_LISTING_MODE_LAZY)
      .withValidValues(FILE_INDEX_LISTING_MODE_LAZY, FILE_INDEX_LISTING_MODE_EAGER)
      .markAdvanced()
      .sinceVersion("0.13.0")
      .withDocumentation("Overrides Hudi's file-index implementation's file listing mode: when set to 'eager'," +
        " file-index will list all partition paths and corresponding file slices w/in them eagerly, during initialization," +
        " prior to partition-pruning kicking in, meaning that all partitions will be listed including ones that might be " +
        " subsequently pruned out; when set to 'lazy', partitions and file-slices w/in them will be listed" +
        " lazily (ie when they actually accessed, instead of when file-index is initialized) allowing partition pruning" +
        " to occur before that, only listing partitions that has already been pruned. Please note that, this config" +
        " is provided purely to allow to fallback to behavior existing prior to 0.13.0 release, and will be deprecated" +
        " soon after.")

  val FILE_INDEX_LISTING_PARTITION_PATH_PREFIX_ANALYSIS_ENABLED: ConfigProperty[Boolean] =
    ConfigProperty.key("hoodie.datasource.read.file.index.listing.partition-path-prefix.analysis.enabled")
      .defaultValue(true)
      .markAdvanced()
      .sinceVersion("0.13.0")
      .withDocumentation("Controls whether partition-path prefix analysis is enabled w/in the file-index, allowing" +
        " to avoid necessity to recursively list deep folder structures of partitioned tables w/ multiple partition columns," +
        " by carefully analyzing provided partition-column predicates and deducing corresponding partition-path prefix from " +
        " them (if possible).")

  val INCREMENTAL_FALLBACK_TO_FULL_TABLE_SCAN: ConfigProperty[String] = ConfigProperty
    .key("hoodie.datasource.read.incr.fallback.fulltablescan.enable")
    .defaultValue("true")
    .markAdvanced()
    .withDocumentation("When doing an incremental query whether we should fall back to full table scans if file does not exist.")

  val SCHEMA_EVOLUTION_ENABLED: ConfigProperty[java.lang.Boolean] = HoodieCommonConfig.SCHEMA_EVOLUTION_ENABLE

  val INCREMENTAL_READ_HANDLE_HOLLOW_COMMIT: ConfigProperty[String] = HoodieCommonConfig.INCREMENTAL_READ_HANDLE_HOLLOW_COMMIT

  val CREATE_TIMELINE_RELATION: ConfigProperty[String] =
    ConfigProperty.key("hoodie.datasource.read.table.valued.function.timeline.relation")
      .defaultValue("false")
      .markAdvanced()
      .sinceVersion("1.0.0")
      .withDocumentation("When this is set, the relation created by DefaultSource is for a view representing" +
        " the result set of the table valued function hudi_query_timeline(...)")

  val TIMELINE_RELATION_ARG_ARCHIVED_TIMELINE:  ConfigProperty[String] =
    ConfigProperty.key("hoodie.datasource.read.table.valued.function.timeline.relation.archived")
      .defaultValue("false")
      .markAdvanced()
      .sinceVersion("1.0.0")
      .withDocumentation("When this is set, the result set of the table valued function hudi_query_timeline(...)" +
        " will include archived timeline")

  val CREATE_FILESYSTEM_RELATION: ConfigProperty[String] = ConfigProperty
    .key("hoodie.datasource.read.create.filesystem.relation")
    .defaultValue("false")
    .markAdvanced()
    .sinceVersion("1.0.0")
    .withDocumentation("When this is set, the relation created by DefaultSource is for a view representing" +
      " the result set of the table valued function hudi_filesystem_view(...)")

  val FILESYSTEM_RELATION_ARG_SUBPATH:  ConfigProperty[String] =
    ConfigProperty.key("hoodie.datasource.read.table.valued.function.filesystem.relation.subpath")
      .defaultValue("")
      .markAdvanced()
      .sinceVersion("1.0.0")
      .withDocumentation("A regex under the table's base path to get file system view information")

  val POLARIS_CATALOG_CLASS_NAME: ConfigProperty[String] = ConfigProperty
    .key("hoodie.spark.polaris.catalog.class")
    .defaultValue("org.apache.polaris.spark.SparkCatalog")
    .markAdvanced()
    .sinceVersion("1.1.0")
    .withDocumentation("Fully qualified class name of the catalog that is used by the Polaris spark client.")

  /** @deprecated Use {@link QUERY_TYPE} and its methods instead */
  @Deprecated
  val QUERY_TYPE_OPT_KEY = QUERY_TYPE.key()
  /** @deprecated Use {@link QUERY_TYPE} and its methods instead */
  @Deprecated
  val DEFAULT_QUERY_TYPE_OPT_VAL: String = QUERY_TYPE_SNAPSHOT_OPT_VAL
  /** @deprecated Use {@link REALTIME_MERGE} and its methods instead */
  @Deprecated
  val REALTIME_MERGE_OPT_KEY = REALTIME_MERGE.key()
  /** @deprecated Use {@link REALTIME_MERGE} and its methods instead */
  @Deprecated
  val DEFAULT_REALTIME_MERGE_OPT_VAL = REALTIME_PAYLOAD_COMBINE_OPT_VAL
  /** @deprecated Use {@link READ_PATHS} and its methods instead */
  @Deprecated
  val READ_PATHS_OPT_KEY = READ_PATHS.key()
  /** @deprecated Use {@link QUERY_TYPE} and its methods instead */
  @Deprecated
  val VIEW_TYPE_OPT_KEY = "hoodie.datasource.view.type"
  @Deprecated
  val VIEW_TYPE_READ_OPTIMIZED_OPT_VAL = "read_optimized"
  @Deprecated
  val VIEW_TYPE_INCREMENTAL_OPT_VAL = "incremental"
  @Deprecated
  val VIEW_TYPE_REALTIME_OPT_VAL = "realtime"
  @Deprecated
  val DEFAULT_VIEW_TYPE_OPT_VAL = VIEW_TYPE_READ_OPTIMIZED_OPT_VAL
  /** @deprecated Use {@link INCREMENTAL_READ_SCHEMA_USE_END_INSTANTTIME} and its methods instead */
  @Deprecated
  val INCREMENTAL_READ_SCHEMA_USE_END_INSTANTTIME_OPT_KEY = INCREMENTAL_READ_SCHEMA_USE_END_INSTANTTIME.key()
  /** @deprecated Use {@link INCREMENTAL_READ_SCHEMA_USE_END_INSTANTTIME} and its methods instead */
  @Deprecated
  val DEFAULT_INCREMENTAL_READ_SCHEMA_USE_END_INSTANTTIME_OPT_VAL = INCREMENTAL_READ_SCHEMA_USE_END_INSTANTTIME.defaultValue()
  /** @deprecated Use {@link PUSH_DOWN_INCR_FILTERS} and its methods instead */
  @Deprecated
  val PUSH_DOWN_INCR_FILTERS_OPT_KEY = PUSH_DOWN_INCR_FILTERS.key()
  /** @deprecated Use {@link PUSH_DOWN_INCR_FILTERS} and its methods instead */
  @Deprecated
  val DEFAULT_PUSH_DOWN_FILTERS_OPT_VAL = PUSH_DOWN_INCR_FILTERS.defaultValue()
  /** @deprecated Use {@link INCR_PATH_GLOB} and its methods instead */
  @Deprecated
  val INCR_PATH_GLOB_OPT_KEY = INCR_PATH_GLOB.key()
  /** @deprecated Use {@link INCR_PATH_GLOB} and its methods instead */
  @Deprecated
  val DEFAULT_INCR_PATH_GLOB_OPT_VAL = INCR_PATH_GLOB.defaultValue()
}

/**
 * Options supported for writing hoodie tables.
 */
object DataSourceWriteOptions {

  val BULK_INSERT_OPERATION_OPT_VAL = WriteOperationType.BULK_INSERT.value
  val INSERT_OPERATION_OPT_VAL = WriteOperationType.INSERT.value
  val UPSERT_OPERATION_OPT_VAL = WriteOperationType.UPSERT.value
  val DELETE_OPERATION_OPT_VAL = WriteOperationType.DELETE.value
  val DELETE_PARTITION_OPERATION_OPT_VAL = WriteOperationType.DELETE_PARTITION.value
  val BOOTSTRAP_OPERATION_OPT_VAL = WriteOperationType.BOOTSTRAP.value
  val INSERT_OVERWRITE_OPERATION_OPT_VAL = WriteOperationType.INSERT_OVERWRITE.value
  val INSERT_OVERWRITE_TABLE_OPERATION_OPT_VAL = WriteOperationType.INSERT_OVERWRITE_TABLE.value
  val OPERATION: ConfigProperty[String] = ConfigProperty
    .key("hoodie.datasource.write.operation")
    .defaultValue(UPSERT_OPERATION_OPT_VAL)
    .withValidValues(
      WriteOperationType.INSERT.value,
      WriteOperationType.INSERT_PREPPED.value,
      WriteOperationType.UPSERT.value,
      WriteOperationType.UPSERT_PREPPED.value,
      WriteOperationType.BULK_INSERT.value,
      WriteOperationType.BULK_INSERT_PREPPED.value,
      WriteOperationType.DELETE.value,
      WriteOperationType.DELETE_PREPPED.value,
      WriteOperationType.BOOTSTRAP.value,
      WriteOperationType.INSERT_OVERWRITE.value,
      WriteOperationType.CLUSTER.value,
      WriteOperationType.DELETE_PARTITION.value,
      WriteOperationType.INSERT_OVERWRITE_TABLE.value,
      WriteOperationType.COMPACT.value,
      WriteOperationType.ALTER_SCHEMA.value
    )
    .withDocumentation("Whether to do upsert, insert or bulk_insert for the write operation. " +
      "Use bulk_insert to load new data into a table, and there on use upsert/insert. " +
      "bulk insert uses a disk based write path to scale to load large inputs without need to cache it.")


  val COW_TABLE_TYPE_OPT_VAL = HoodieTableType.COPY_ON_WRITE.name
  val MOR_TABLE_TYPE_OPT_VAL = HoodieTableType.MERGE_ON_READ.name
  val TABLE_TYPE: ConfigProperty[String] = ConfigProperty
    .key("hoodie.datasource.write.table.type")
    .defaultValue(COW_TABLE_TYPE_OPT_VAL)
    .withValidValues(COW_TABLE_TYPE_OPT_VAL, MOR_TABLE_TYPE_OPT_VAL)
    .withAlternatives("hoodie.datasource.write.storage.type")
    .withDocumentation("The table type for the underlying data, for this write. This can’t change between writes.")

  /**
   * Config key with boolean value that indicates whether record being written during UPDATE or DELETE Spark SQL
   * operations are already prepped.
   */
  val SPARK_SQL_WRITES_PREPPED_KEY = "_hoodie.spark.sql.writes.prepped";

  /**
   * May be derive partition path from incoming df if not explicitly set.
   *
   * @param optParams Parameters to be translated
   * @return Parameters after translation
   */
  def mayBeDerivePartitionPath(optParams: Map[String, String]): Map[String, String] = {
    var translatedOptParams = optParams
    // translate the api partitionBy of spark DataFrameWriter to PARTITIONPATH_FIELD
    // we should set hoodie's partition path only if its not set by the user.
    if (optParams.contains(SparkDataSourceUtils.PARTITIONING_COLUMNS_KEY)
      && !optParams.contains(KeyGeneratorOptions.PARTITIONPATH_FIELD_NAME.key())) {
      val partitionColumns = optParams.get(SparkDataSourceUtils.PARTITIONING_COLUMNS_KEY)
        .map(SparkDataSourceUtils.decodePartitioningColumns)
        .getOrElse(Nil)
      val keyGeneratorClass = optParams.getOrElse(DataSourceWriteOptions.KEYGENERATOR_CLASS_NAME.key(),
        DataSourceWriteOptions.KEYGENERATOR_CLASS_NAME.defaultValue)

      keyGeneratorClass match {
        // CustomKeyGenerator needs special treatment, because it needs to be specified in a way
        // such as "field1:PartitionKeyType1,field2:PartitionKeyType2".
        // partitionBy can specify the partition like this: partitionBy("p1", "p2:SIMPLE", "p3:TIMESTAMP")
        case c if (c.nonEmpty && c == classOf[CustomKeyGenerator].getName) =>
          val partitionPathField = partitionColumns.map(e => {
            if (e.contains(":")) {
              e
            } else {
              s"$e:SIMPLE"
            }
          }).mkString(",")
          translatedOptParams = optParams ++ Map(PARTITIONPATH_FIELD.key -> partitionPathField)
        case c if (c.isEmpty || !keyGeneratorClass.equals(classOf[NonpartitionedKeyGenerator].getName)) =>
          // for any key gen other than NonPartitioned key gen, we can override the partition field config.
          val partitionPathField = partitionColumns.mkString(",")
          translatedOptParams = optParams ++ Map(PARTITIONPATH_FIELD.key -> partitionPathField)
        case _ => // no op incase of NonPartitioned Key gen.
      }
    }
    translatedOptParams
  }

  val TABLE_NAME: ConfigProperty[String] = ConfigProperty
    .key(HoodieTableConfig.HOODIE_WRITE_TABLE_NAME_KEY)
    .noDefaultValue()
    .markAdvanced()
    .withDocumentation("Table name for the datasource write. Also used to register the table into meta stores.")

  /**
   * Field used in preCombining before actual write. When two records have the same
   * key value, we will pick the one with the largest value for the precombine field,
   * determined by Object.compareTo(..)
   */
  val PRECOMBINE_FIELD = HoodieWriteConfig.PRECOMBINE_FIELD_NAME

  /**
   * Payload class used. Override this, if you like to roll your own merge logic, when upserting/inserting.
   * This will render any value set for `PRECOMBINE_FIELD_OPT_VAL` in-effective
   */
  val PAYLOAD_CLASS_NAME = HoodieWriteConfig.WRITE_PAYLOAD_CLASS_NAME

  /**
   * HoodieMerger will replace the payload to process the merge of data
   * and provide the same capabilities as the payload
   */
  val RECORD_MERGE_IMPL_CLASSES = HoodieWriteConfig.RECORD_MERGE_IMPL_CLASSES

  /**
   * Id of merger strategy
   */
  val RECORD_MERGE_STRATEGY_ID = HoodieWriteConfig.RECORD_MERGE_STRATEGY_ID

  val RECORD_MERGE_MODE = HoodieWriteConfig.RECORD_MERGE_MODE

  /**
   * Record key field. Value to be used as the `recordKey` component of `HoodieKey`. Actual value
   * will be obtained by invoking .toString() on the field value. Nested fields can be specified using
   * the dot notation eg: `a.b.c`
   *
   */
  val RECORDKEY_FIELD = KeyGeneratorOptions.RECORDKEY_FIELD_NAME

  /**
   * Secondary key field. Columns to be used as the secondary index columns. Actual value
   * will be obtained by invoking .toString() on the field value. Nested fields can be specified using
   * the dot notation eg: `a.b.c`
   */
  val SECONDARYKEY_COLUMN_NAME = KeyGeneratorOptions.SECONDARYKEY_COLUMN_NAME

  /**
   * Partition path field. Value to be used at the `partitionPath` component of `HoodieKey`. Actual
   * value obtained by invoking .toString()
   */
  val PARTITIONPATH_FIELD = KeyGeneratorOptions.PARTITIONPATH_FIELD_NAME

  /**
   * Flag to indicate whether to use Hive style partitioning.
   * If set true, the names of partition folders follow <partition_column_name>=<partition_value> format.
   * By default false (the names of partition folders are only partition values)
   */
  val HIVE_STYLE_PARTITIONING = KeyGeneratorOptions.HIVE_STYLE_PARTITIONING_ENABLE

  /**
   * Key generator class, that implements will extract the key out of incoming record.
   */
  val keyGeneratorInferFunc = JFunction.toJavaFunction((config: HoodieConfig) => {
    Option.of(DataSourceOptionsHelper.inferKeyGenClazz(config.getProps))
  })

  val KEYGENERATOR_CLASS_NAME: ConfigProperty[String] = ConfigProperty
    .key("hoodie.datasource.write.keygenerator.class")
    .defaultValue(classOf[SimpleKeyGenerator].getName)
    .withInferFunction(keyGeneratorInferFunc)
    .markAdvanced()
    .withDocumentation("Key generator class, that implements `org.apache.hudi.keygen.KeyGenerator`")

  val KEYGENERATOR_CONSISTENT_LOGICAL_TIMESTAMP_ENABLED: ConfigProperty[String] = KeyGeneratorOptions.KEYGENERATOR_CONSISTENT_LOGICAL_TIMESTAMP_ENABLED

  val ENABLE_ROW_WRITER: ConfigProperty[String] = ConfigProperty
    .key("hoodie.datasource.write.row.writer.enable")
    .defaultValue("true")
    .withInferFunction(
      JFunction.toJavaFunction((config: HoodieConfig) => {
        if (config.getString(OPERATION) == WriteOperationType.BULK_INSERT.value
          && !config.getBooleanOrDefault(HoodieTableConfig.POPULATE_META_FIELDS)
          && config.getBooleanOrDefault(HoodieWriteConfig.COMBINE_BEFORE_INSERT)) {
          // need to turn off row writing for BULK_INSERT without meta fields with turned on COMBINE_BEFORE_INSERT to prevent shortcutting and ignoring COMBINE_BEFORE_INSERT setting
          Option.of("false")
        } else {
          Option.empty()
        }
      })
    )
    .markAdvanced()
    .withDocumentation("When set to true, will perform write operations directly using the spark native " +
      "`Row` representation, avoiding any additional conversion costs.")

  /**
   * Enable the bulk insert for sql insert statement.
   */
  @Deprecated
  val SQL_ENABLE_BULK_INSERT: ConfigProperty[String] = ConfigProperty
    .key("hoodie.sql.bulk.insert.enable")
    .defaultValue("false")
    .markAdvanced()
    .deprecatedAfter("0.14.0")
    .withDocumentation("When set to true, the sql insert statement will use bulk insert. " +
      "This config is deprecated as of 0.14.0. Please use hoodie.spark.sql.insert.into.operation instead.")

  @Deprecated
  val SQL_INSERT_MODE: ConfigProperty[String] = ConfigProperty
    .key("hoodie.sql.insert.mode")
    .defaultValue("upsert")
    .markAdvanced()
    .deprecatedAfter("0.14.0")
    .withDocumentation("Insert mode when insert data to pk-table. The optional modes are: upsert, strict and non-strict." +
      "For upsert mode, insert statement do the upsert operation for the pk-table which will update the duplicate record." +
      "For strict mode, insert statement will keep the primary key uniqueness constraint which do not allow duplicate record." +
      "While for non-strict mode, hudi just do the insert operation for the pk-table. This config is deprecated as of 0.14.0. Please use " +
      "hoodie.spark.sql.insert.into.operation and hoodie.datasource.insert.dup.policy as you see fit.")

  val COMMIT_METADATA_KEYPREFIX: ConfigProperty[String] = ConfigProperty
    .key("hoodie.datasource.write.commitmeta.key.prefix")
    .defaultValue("_")
    .markAdvanced()
    .withDocumentation("Option keys beginning with this prefix, are automatically added to the commit/deltacommit metadata. " +
      "This is useful to store checkpointing information, in a consistent way with the hudi timeline")

  @Deprecated
  val INSERT_DROP_DUPS: ConfigProperty[String] = ConfigProperty
    .key("hoodie.datasource.write.insert.drop.duplicates")
    .defaultValue("false")
    .markAdvanced()
    .withDocumentation("If set to true, records from the incoming dataframe will not overwrite existing records with the same key during the write operation. " +
      "<br /> **Note** Just for Insert operation in Spark SQL writing since 0.14.0, users can switch to the config `hoodie.datasource.insert.dup.policy` instead " +
      "for a simplified duplicate handling experience. The new config will be incorporated into all other writing flows and this config will be fully deprecated " +
      "in future releases.");

  val PARTITIONS_TO_DELETE: ConfigProperty[String] = ConfigProperty
    .key("hoodie.datasource.write.partitions.to.delete")
    .noDefaultValue()
    .markAdvanced()
    .withDocumentation("Comma separated list of partitions to delete. Allows use of wildcard *")

  val STREAMING_RETRY_CNT: ConfigProperty[String] = ConfigProperty
    .key("hoodie.datasource.write.streaming.retry.count")
    .defaultValue("3")
    .markAdvanced()
    .withDocumentation("Config to indicate how many times streaming job should retry for a failed micro batch.")

  val STREAMING_RETRY_INTERVAL_MS: ConfigProperty[String] = ConfigProperty
    .key("hoodie.datasource.write.streaming.retry.interval.ms")
    .defaultValue("2000")
    .markAdvanced()
    .withDocumentation(" Config to indicate how long (by millisecond) before a retry should issued for failed microbatch")

  /**
   * By default false. If users prefer streaming progress over data integrity, can set this to true.
   */
  val STREAMING_IGNORE_FAILED_BATCH: ConfigProperty[String] = ConfigProperty
    .key("hoodie.datasource.write.streaming.ignore.failed.batch")
    .defaultValue("false")
    .markAdvanced()
    .withDocumentation("Config to indicate whether to ignore any non exception error (e.g. writestatus error)"
      + " within a streaming microbatch. Turning this on, could hide the write status errors while the spark checkpoint moves ahead." +
      "So, would recommend users to use this with caution.")

  val STREAMING_CHECKPOINT_IDENTIFIER: ConfigProperty[String] = ConfigProperty
    .key("hoodie.datasource.write.streaming.checkpoint.identifier")
    .defaultValue("default_single_writer")
    .markAdvanced()
    .sinceVersion("0.13.0")
    .withDocumentation("A stream identifier used for HUDI to fetch the right checkpoint(`batch id` to be more specific) "
      + "corresponding this writer. Please note that keep the identifier an unique value for different writer "
      + "if under multi-writer scenario. If the value is not set, will only keep the checkpoint info in the memory. "
      + "This could introduce the potential issue that the job is restart(`batch id` is lost) while spark checkpoint write fails, "
      + "causing spark will retry and rewrite the data.")

  val STREAMING_DISABLE_COMPACTION: ConfigProperty[String] = ConfigProperty
    .key("hoodie.datasource.write.streaming.disable.compaction")
    .defaultValue("false")
    .markAdvanced()
    .sinceVersion("0.14.0")
    .withDocumentation("By default for MOR table, async compaction is enabled with spark streaming sink. "
      + "By setting this config to true, we can disable it and the expectation is that, users will schedule and execute "
      + "compaction in a different process/job altogether. Some users may wish to run it separately to manage resources "
      + "across table services and regular ingestion pipeline and so this could be preferred on such cases.")

  val META_SYNC_CLIENT_TOOL_CLASS_NAME: ConfigProperty[String] = ConfigProperty
    .key("hoodie.meta.sync.client.tool.class")
    .defaultValue(classOf[HiveSyncTool].getName)
    .markAdvanced()
    .withDocumentation("Sync tool class name used to sync to metastore. Defaults to Hive.")

  @Deprecated
  val RECONCILE_SCHEMA: ConfigProperty[java.lang.Boolean] = HoodieCommonConfig.RECONCILE_SCHEMA

  val SET_NULL_FOR_MISSING_COLUMNS: ConfigProperty[String] = HoodieCommonConfig.SET_NULL_FOR_MISSING_COLUMNS

  val SPARK_SQL_INSERT_INTO_OPERATION: ConfigProperty[String] = ConfigProperty
    .key("hoodie.spark.sql.insert.into.operation")
    .defaultValue(WriteOperationType.INSERT.value())
    .withValidValues(WriteOperationType.BULK_INSERT.value(), WriteOperationType.INSERT.value(), WriteOperationType.UPSERT.value())
    .markAdvanced()
    .sinceVersion("0.14.0")
    .withDocumentation("Sql write operation to use with INSERT_INTO spark sql command. This comes with 3 possible values, bulk_insert, " +
      "insert and upsert. bulk_insert is generally meant for initial loads and is known to be performant compared to insert. But bulk_insert may not " +
      "do small file management. If you prefer hudi to automatically manage small files, then you can go with \"insert\". There is no precombine " +
      "(if there are duplicates within the same batch being ingested, same dups will be ingested) with bulk_insert and insert and there is no index " +
      "look up as well. If you may use INSERT_INTO for mutable dataset, then you may have to set this config value to \"upsert\". With upsert, you will " +
      "get both precombine and updates to existing records on storage is also honored. If not, you may see duplicates. ")

  val ENABLE_MERGE_INTO_PARTIAL_UPDATES: ConfigProperty[Boolean] = ConfigProperty
    .key("hoodie.spark.sql.merge.into.partial.updates")
    .defaultValue(true)
    .markAdvanced()
    .sinceVersion("1.0.0")
    .withDocumentation("Whether to write partial updates to the data blocks containing updates "
      + "in MOR tables with Spark SQL MERGE INTO statement. The data blocks containing partial "
      + "updates have a schema with a subset of fields compared to the full schema of the table.")

  val NONE_INSERT_DUP_POLICY = "none"
  val DROP_INSERT_DUP_POLICY = "drop"
  val FAIL_INSERT_DUP_POLICY = "fail"

  val INSERT_DUP_POLICY: ConfigProperty[String] = ConfigProperty
    .key("hoodie.datasource.insert.dup.policy")
    .defaultValue(NONE_INSERT_DUP_POLICY)
    .withValidValues(NONE_INSERT_DUP_POLICY, DROP_INSERT_DUP_POLICY, FAIL_INSERT_DUP_POLICY)
    .markAdvanced()
    .sinceVersion("0.14.0")
    .withDocumentation("**Note** This is only applicable to Spark SQL writing.<br />When operation type is set to \"insert\", users can optionally enforce a dedup policy. This policy will be employed "
      + " when records being ingested already exists in storage. Default policy is none and no action will be taken. Another option is to choose " +
      " \"drop\", on which matching records from incoming will be dropped and the rest will be ingested. Third option is \"fail\" which will " +
      "fail the write operation when same records are re-ingested. In other words, a given record as deduced by the key generation policy " +
      "can be ingested only once to the target table of interest.")

  // HIVE SYNC SPECIFIC CONFIGS
  // NOTE: DO NOT USE uppercase for the keys as they are internally lower-cased. Using upper-cases causes
  // unexpected issues with config getting reset
  /**
   * @deprecated Hive Specific Configs are moved to {@link HiveSyncConfig}
   */
  @Deprecated
  val HIVE_SYNC_ENABLED: ConfigProperty[String] = HiveSyncConfigHolder.HIVE_SYNC_ENABLED
  @Deprecated
  val META_SYNC_ENABLED: ConfigProperty[String] = HoodieSyncConfig.META_SYNC_ENABLED
  @Deprecated
  val HIVE_DATABASE: ConfigProperty[String] = HoodieSyncConfig.META_SYNC_DATABASE_NAME
  @Deprecated
  val HIVE_TABLE: ConfigProperty[String] = HoodieSyncConfig.META_SYNC_TABLE_NAME
  @Deprecated
  val HIVE_BASE_FILE_FORMAT: ConfigProperty[String] = HoodieSyncConfig.META_SYNC_BASE_FILE_FORMAT
  @Deprecated
  val HIVE_USER: ConfigProperty[String] = HiveSyncConfigHolder.HIVE_USER
  @Deprecated
  val HIVE_PASS: ConfigProperty[String] = HiveSyncConfigHolder.HIVE_PASS
  @Deprecated
  val HIVE_URL: ConfigProperty[String] = HiveSyncConfigHolder.HIVE_URL
  @Deprecated
  val METASTORE_URIS: ConfigProperty[String] = HiveSyncConfigHolder.METASTORE_URIS
  @Deprecated
  val HIVE_PARTITION_FIELDS: ConfigProperty[String] = HoodieSyncConfig.META_SYNC_PARTITION_FIELDS
  @Deprecated
  val HIVE_PARTITION_EXTRACTOR_CLASS: ConfigProperty[String] = HoodieSyncConfig.META_SYNC_PARTITION_EXTRACTOR_CLASS
  @Deprecated
  val HIVE_USE_PRE_APACHE_INPUT_FORMAT: ConfigProperty[String] = HiveSyncConfigHolder.HIVE_USE_PRE_APACHE_INPUT_FORMAT

  /** @deprecated Use {@link HIVE_SYNC_MODE} instead of this config from 0.9.0 */
  @Deprecated
  val HIVE_USE_JDBC: ConfigProperty[String] = HiveSyncConfigHolder.HIVE_USE_JDBC
  @Deprecated
  val HIVE_AUTO_CREATE_DATABASE: ConfigProperty[String] = HiveSyncConfigHolder.HIVE_AUTO_CREATE_DATABASE
  @Deprecated
  val HIVE_IGNORE_EXCEPTIONS: ConfigProperty[String] = HiveSyncConfigHolder.HIVE_IGNORE_EXCEPTIONS
  @Deprecated
  val HIVE_SKIP_RO_SUFFIX_FOR_READ_OPTIMIZED_TABLE: ConfigProperty[String] = HiveSyncConfigHolder.HIVE_SKIP_RO_SUFFIX_FOR_READ_OPTIMIZED_TABLE
  @Deprecated
  val HIVE_SUPPORT_TIMESTAMP_TYPE: ConfigProperty[String] = HiveSyncConfigHolder.HIVE_SUPPORT_TIMESTAMP_TYPE

  /**
   * Flag to indicate whether to use conditional syncing in HiveSync.
   * If set true, the Hive sync procedure will only run if partition or schema changes are detected.
   * By default true.
   */
  @Deprecated
  val HIVE_CONDITIONAL_SYNC: ConfigProperty[String] = HoodieSyncConfig.META_SYNC_CONDITIONAL_SYNC
  @Deprecated
  val HIVE_TABLE_PROPERTIES: ConfigProperty[String] = HiveSyncConfigHolder.HIVE_TABLE_PROPERTIES
  @Deprecated
  val HIVE_TABLE_SERDE_PROPERTIES: ConfigProperty[String] = HiveSyncConfigHolder.HIVE_TABLE_SERDE_PROPERTIES
  @Deprecated
  val HIVE_SYNC_AS_DATA_SOURCE_TABLE: ConfigProperty[String] = HiveSyncConfigHolder.HIVE_SYNC_AS_DATA_SOURCE_TABLE

  // Create table as managed table
  @Deprecated
  val HIVE_CREATE_MANAGED_TABLE: ConfigProperty[java.lang.Boolean] = HiveSyncConfigHolder.HIVE_CREATE_MANAGED_TABLE
  @Deprecated
  val HIVE_BATCH_SYNC_PARTITION_NUM: ConfigProperty[java.lang.Integer] = HiveSyncConfigHolder.HIVE_BATCH_SYNC_PARTITION_NUM
  @Deprecated
  val HIVE_SYNC_MODE: ConfigProperty[String] = HiveSyncConfigHolder.HIVE_SYNC_MODE
  @Deprecated
  val HIVE_SYNC_BUCKET_SYNC: ConfigProperty[java.lang.Boolean] = HiveSyncConfigHolder.HIVE_SYNC_BUCKET_SYNC
  @Deprecated
  val HIVE_SYNC_COMMENT: ConfigProperty[String] = HiveSyncConfigHolder.HIVE_SYNC_COMMENT;

  // Async Compaction - Enabled by default for MOR
  val ASYNC_COMPACT_ENABLE: ConfigProperty[String] = ConfigProperty
    .key("hoodie.datasource.compaction.async.enable")
    .defaultValue("true")
    .markAdvanced()
    .withDocumentation("Controls whether async compaction should be turned on for MOR table writing.")

  val INLINE_CLUSTERING_ENABLE = HoodieClusteringConfig.INLINE_CLUSTERING

  val ASYNC_CLUSTERING_ENABLE = HoodieClusteringConfig.ASYNC_CLUSTERING_ENABLE

  val KAFKA_AVRO_VALUE_DESERIALIZER_CLASS: ConfigProperty[String] = ConfigProperty
    .key(STREAMER_CONFIG_PREFIX + "source.kafka.value.deserializer.class")
    .defaultValue("io.confluent.kafka.serializers.KafkaAvroDeserializer")
    .withAlternatives(DELTA_STREAMER_CONFIG_PREFIX + "source.kafka.value.deserializer.class")
    .markAdvanced()
    .sinceVersion("0.9.0")
    .withDocumentation("This class is used by kafka client to deserialize the records")

  val DROP_PARTITION_COLUMNS: ConfigProperty[java.lang.Boolean] = HoodieTableConfig.DROP_PARTITION_COLUMNS

  val SPARK_SQL_OPTIMIZED_WRITES: ConfigProperty[String] = ConfigProperty
    .key("hoodie.spark.sql.optimized.writes.enable")
    .defaultValue("true")
    .markAdvanced()
    .sinceVersion("0.14.0")
    .withDocumentation("Controls whether spark sql prepped update and delete are enabled.")

  val OVERWRITE_MODE: ConfigProperty[String] = ConfigProperty
    .key("hoodie.datasource.overwrite.mode")
    .noDefaultValue()
    .withValidValues("STATIC", "DYNAMIC")
    .markAdvanced()
    .sinceVersion("0.14.0")
    .withDocumentation("Controls whether overwrite use dynamic or static mode, if not configured, " +
      "respect spark.sql.sources.partitionOverwriteMode")

  /** @deprecated Use {@link HIVE_USE_PRE_APACHE_INPUT_FORMAT} and its methods instead */
  @Deprecated
  val HIVE_USE_PRE_APACHE_INPUT_FORMAT_OPT_KEY = HiveSyncConfigHolder.HIVE_USE_PRE_APACHE_INPUT_FORMAT.key()
  /** @deprecated Use {@link HIVE_USE_JDBC} and its methods instead */
  @Deprecated
  val HIVE_USE_JDBC_OPT_KEY = HiveSyncConfigHolder.HIVE_USE_JDBC.key()
  /** @deprecated Use {@link HIVE_AUTO_CREATE_DATABASE} and its methods instead */
  @Deprecated
  val HIVE_AUTO_CREATE_DATABASE_OPT_KEY = HiveSyncConfigHolder.HIVE_AUTO_CREATE_DATABASE.key()
  /** @deprecated Use {@link HIVE_IGNORE_EXCEPTIONS} and its methods instead */
  @Deprecated
  val HIVE_IGNORE_EXCEPTIONS_OPT_KEY = HiveSyncConfigHolder.HIVE_IGNORE_EXCEPTIONS.key()
  /** @deprecated Use {@link STREAMING_IGNORE_FAILED_BATCH} and its methods instead */
  @Deprecated
  val STREAMING_IGNORE_FAILED_BATCH_OPT_KEY = STREAMING_IGNORE_FAILED_BATCH.key()
  /** @deprecated Use {@link STREAMING_IGNORE_FAILED_BATCH} and its methods instead */
  @Deprecated
  val DEFAULT_STREAMING_IGNORE_FAILED_BATCH_OPT_VAL = STREAMING_IGNORE_FAILED_BATCH.defaultValue()
  /** @deprecated Use {@link META_SYNC_CLIENT_TOOL_CLASS_NAME} and its methods instead */
  @Deprecated
  val META_SYNC_CLIENT_TOOL_CLASS = META_SYNC_CLIENT_TOOL_CLASS_NAME.key()
  /** @deprecated Use {@link META_SYNC_CLIENT_TOOL_CLASS_NAME} and its methods instead */
  @Deprecated
  val DEFAULT_META_SYNC_CLIENT_TOOL_CLASS = META_SYNC_CLIENT_TOOL_CLASS_NAME.defaultValue()
  /** @deprecated Use {@link HIVE_SYNC_ENABLED} and its methods instead */
  @Deprecated
  val HIVE_SYNC_ENABLED_OPT_KEY = HiveSyncConfigHolder.HIVE_SYNC_ENABLED.key()
  /** @deprecated Use {@link META_SYNC_ENABLED} and its methods instead */
  @Deprecated
  val META_SYNC_ENABLED_OPT_KEY = HoodieSyncConfig.META_SYNC_ENABLED.key()
  /** @deprecated Use {@link HIVE_DATABASE} and its methods instead */
  @Deprecated
  val HIVE_DATABASE_OPT_KEY = HoodieSyncConfig.META_SYNC_DATABASE_NAME.key()
  /** @deprecated Use {@link HIVE_TABLE} and its methods instead */
  @Deprecated
  val HIVE_TABLE_OPT_KEY = HoodieSyncConfig.META_SYNC_TABLE_NAME.key()
  /** @deprecated Use {@link HIVE_BASE_FILE_FORMAT} and its methods instead */
  @Deprecated
  val HIVE_BASE_FILE_FORMAT_OPT_KEY = HoodieSyncConfig.META_SYNC_BASE_FILE_FORMAT.key()
  /** @deprecated Use {@link HIVE_USER} and its methods instead */
  @Deprecated
  val HIVE_USER_OPT_KEY = HiveSyncConfigHolder.HIVE_USER.key()
  /** @deprecated Use {@link HIVE_PASS} and its methods instead */
  @Deprecated
  val HIVE_PASS_OPT_KEY = HiveSyncConfigHolder.HIVE_PASS.key()
  /** @deprecated Use {@link HIVE_URL} and its methods instead */
  @Deprecated
  val HIVE_URL_OPT_KEY = HiveSyncConfigHolder.HIVE_URL.key()
  /** @deprecated Use {@link HIVE_PARTITION_FIELDS} and its methods instead */
  @Deprecated
  val HIVE_PARTITION_FIELDS_OPT_KEY = HoodieSyncConfig.META_SYNC_PARTITION_FIELDS.key()
  /** @deprecated Use {@link HIVE_PARTITION_EXTRACTOR_CLASS} and its methods instead */
  @Deprecated
  val HIVE_PARTITION_EXTRACTOR_CLASS_OPT_KEY = HoodieSyncConfig.META_SYNC_PARTITION_EXTRACTOR_CLASS.key()

  /** @deprecated Use {@link KEYGENERATOR_CLASS_NAME} and its methods instead */
  @Deprecated
  val DEFAULT_KEYGENERATOR_CLASS_OPT_VAL = KEYGENERATOR_CLASS_NAME.defaultValue()
  /** @deprecated Use {@link KEYGENERATOR_CLASS_NAME} and its methods instead */
  @Deprecated
  val KEYGENERATOR_CLASS_OPT_KEY = HoodieWriteConfig.KEYGENERATOR_CLASS_NAME.key()
  /** @deprecated Use {@link ENABLE_ROW_WRITER} and its methods instead */
  @Deprecated
  val ENABLE_ROW_WRITER_OPT_KEY = ENABLE_ROW_WRITER.key()
  /** @deprecated Use {@link ENABLE_ROW_WRITER} and its methods instead */
  @Deprecated
  val DEFAULT_ENABLE_ROW_WRITER_OPT_VAL = ENABLE_ROW_WRITER.defaultValue()
  /** @deprecated Use {@link HIVE_STYLE_PARTITIONING} and its methods instead */
  @Deprecated
  val HIVE_STYLE_PARTITIONING_OPT_KEY = KeyGeneratorOptions.HIVE_STYLE_PARTITIONING_ENABLE.key()
  /** @deprecated Use {@link HIVE_STYLE_PARTITIONING} and its methods instead */
  @Deprecated
  val DEFAULT_HIVE_STYLE_PARTITIONING_OPT_VAL = HIVE_STYLE_PARTITIONING.defaultValue()

  val URL_ENCODE_PARTITIONING = KeyGeneratorOptions.URL_ENCODE_PARTITIONING
  /** @deprecated Use {@link URL_ENCODE_PARTITIONING} and its methods instead */
  @Deprecated
  val URL_ENCODE_PARTITIONING_OPT_KEY = KeyGeneratorOptions.URL_ENCODE_PARTITIONING.key()
  /** @deprecated Use {@link URL_ENCODE_PARTITIONING} and its methods instead */
  @Deprecated
  val DEFAULT_URL_ENCODE_PARTITIONING_OPT_VAL = URL_ENCODE_PARTITIONING.defaultValue()
  /** @deprecated Use {@link COMMIT_METADATA_KEYPREFIX} and its methods instead */
  @Deprecated
  val COMMIT_METADATA_KEYPREFIX_OPT_KEY = COMMIT_METADATA_KEYPREFIX.key()
  /** @deprecated Use {@link COMMIT_METADATA_KEYPREFIX} and its methods instead */
  @Deprecated
  val DEFAULT_COMMIT_METADATA_KEYPREFIX_OPT_VAL = COMMIT_METADATA_KEYPREFIX.defaultValue()
  /** @deprecated Use {@link INSERT_DROP_DUPS} and its methods instead */
  @Deprecated
  val INSERT_DROP_DUPS_OPT_KEY = INSERT_DROP_DUPS.key()
  /** @deprecated Use {@link INSERT_DROP_DUPS} and its methods instead */
  @Deprecated
  val DEFAULT_INSERT_DROP_DUPS_OPT_VAL = INSERT_DROP_DUPS.defaultValue()
  /** @deprecated Use {@link STREAMING_RETRY_CNT} and its methods instead */
  @Deprecated
  val STREAMING_RETRY_CNT_OPT_KEY = STREAMING_RETRY_CNT.key()
  /** @deprecated Use {@link STREAMING_RETRY_CNT} and its methods instead */
  @Deprecated
  val DEFAULT_STREAMING_RETRY_CNT_OPT_VAL = STREAMING_RETRY_CNT.defaultValue()
  /** @deprecated Use {@link STREAMING_RETRY_INTERVAL_MS} and its methods instead */
  @Deprecated
  val STREAMING_RETRY_INTERVAL_MS_OPT_KEY = STREAMING_RETRY_INTERVAL_MS.key()
  /** @deprecated Use {@link STREAMING_RETRY_INTERVAL_MS} and its methods instead */
  @Deprecated
  val DEFAULT_STREAMING_RETRY_INTERVAL_MS_OPT_VAL = STREAMING_RETRY_INTERVAL_MS.defaultValue()

  /** @deprecated Use {@link RECORDKEY_FIELD} and its methods instead */
  @Deprecated
  val RECORDKEY_FIELD_OPT_KEY = KeyGeneratorOptions.RECORDKEY_FIELD_NAME.key()
  /** @deprecated Use {@link RECORDKEY_FIELD} and its methods instead */
  @Deprecated
  val PARTITIONPATH_FIELD_OPT_KEY = KeyGeneratorOptions.PARTITIONPATH_FIELD_NAME.key()
  /** @deprecated Use {@link PARTITIONPATH_FIELD} and its methods instead */
  @Deprecated
  val DEFAULT_PARTITIONPATH_FIELD_OPT_VAL = null

  /** @deprecated Use {@link TABLE_NAME} and its methods instead */
  @Deprecated
  val TABLE_NAME_OPT_KEY = TABLE_NAME.key()
  /** @deprecated Use {@link PRECOMBINE_FIELD} and its methods instead */
  @Deprecated
  val PRECOMBINE_FIELD_OPT_KEY = HoodieWriteConfig.PRECOMBINE_FIELD_NAME.key()

  /** @deprecated Use {@link HoodieWriteConfig.WRITE_PAYLOAD_CLASS_NAME} and its methods instead */
  @Deprecated
  val PAYLOAD_CLASS_OPT_KEY = HoodieWriteConfig.WRITE_PAYLOAD_CLASS_NAME.key()

  /** @deprecated Use {@link TABLE_TYPE} and its methods instead */
  @Deprecated
  val TABLE_TYPE_OPT_KEY = TABLE_TYPE.key()
  /** @deprecated Use {@link TABLE_TYPE} and its methods instead */
  @Deprecated
  val DEFAULT_TABLE_TYPE_OPT_VAL = TABLE_TYPE.defaultValue()

  /** @deprecated Use {@link TABLE_TYPE} and its methods instead */
  @Deprecated
  val STORAGE_TYPE_OPT_KEY = "hoodie.datasource.write.storage.type"
  @Deprecated
  val COW_STORAGE_TYPE_OPT_VAL = HoodieTableType.COPY_ON_WRITE.name
  @Deprecated
  val MOR_STORAGE_TYPE_OPT_VAL = HoodieTableType.MERGE_ON_READ.name
  /** @deprecated Use {@link TABLE_TYPE} and its methods instead */
  @Deprecated
  val DEFAULT_STORAGE_TYPE_OPT_VAL = COW_STORAGE_TYPE_OPT_VAL

  /** @deprecated Use {@link OPERATION} and its methods instead */
  @Deprecated
  val OPERATION_OPT_KEY = OPERATION.key()
  /** @deprecated Use {@link OPERATION} and its methods instead */
  @Deprecated
  val DEFAULT_OPERATION_OPT_VAL = OPERATION.defaultValue()

  /** @deprecated Use {@link HIVE_SYNC_ENABLED} and its methods instead */
  @Deprecated
  val DEFAULT_HIVE_SYNC_ENABLED_OPT_VAL = HiveSyncConfigHolder.HIVE_SYNC_ENABLED.defaultValue()
  /** @deprecated Use {@link META_SYNC_ENABLED} and its methods instead */
  @Deprecated
  val DEFAULT_META_SYNC_ENABLED_OPT_VAL = HoodieSyncConfig.META_SYNC_ENABLED.defaultValue()
  /** @deprecated Use {@link HIVE_DATABASE} and its methods instead */
  @Deprecated
  val DEFAULT_HIVE_DATABASE_OPT_VAL = HoodieSyncConfig.META_SYNC_DATABASE_NAME.defaultValue()
  /** @deprecated Use {@link HIVE_TABLE} and its methods instead */
  @Deprecated
  val DEFAULT_HIVE_TABLE_OPT_VAL = HoodieSyncConfig.META_SYNC_TABLE_NAME.defaultValue()
  /** @deprecated Use {@link HIVE_BASE_FILE_FORMAT} and its methods instead */
  @Deprecated
  val DEFAULT_HIVE_BASE_FILE_FORMAT_OPT_VAL = HoodieSyncConfig.META_SYNC_BASE_FILE_FORMAT.defaultValue()
  /** @deprecated Use {@link HIVE_USER} and its methods instead */
  @Deprecated
  val DEFAULT_HIVE_USER_OPT_VAL = HiveSyncConfigHolder.HIVE_USER.defaultValue()
  /** @deprecated Use {@link HIVE_PASS} and its methods instead */
  @Deprecated
  val DEFAULT_HIVE_PASS_OPT_VAL = HiveSyncConfigHolder.HIVE_PASS.defaultValue()
  /** @deprecated Use {@link HIVE_URL} and its methods instead */
  @Deprecated
  val DEFAULT_HIVE_URL_OPT_VAL = HiveSyncConfigHolder.HIVE_URL.defaultValue()
  /** @deprecated Use {@link HIVE_PARTITION_FIELDS} and its methods instead */
  @Deprecated
  val DEFAULT_HIVE_PARTITION_FIELDS_OPT_VAL = HoodieSyncConfig.META_SYNC_PARTITION_FIELDS.defaultValue()
  /** @deprecated Use {@link HIVE_PARTITION_EXTRACTOR_CLASS} and its methods instead */
  @Deprecated
  val DEFAULT_HIVE_PARTITION_EXTRACTOR_CLASS_OPT_VAL = HoodieSyncConfig.META_SYNC_PARTITION_EXTRACTOR_CLASS.defaultValue()
  @Deprecated
  val DEFAULT_USE_PRE_APACHE_INPUT_FORMAT_OPT_VAL = "false"
  /** @deprecated Use {@link HIVE_USE_JDBC} and its methods instead */
  @Deprecated
  val DEFAULT_HIVE_USE_JDBC_OPT_VAL = HiveSyncConfigHolder.HIVE_USE_JDBC.defaultValue()
  /** @deprecated Use {@link HIVE_AUTO_CREATE_DATABASE} and its methods instead */
  @Deprecated
  val DEFAULT_HIVE_AUTO_CREATE_DATABASE_OPT_KEY = HiveSyncConfigHolder.HIVE_AUTO_CREATE_DATABASE.defaultValue()
  /** @deprecated Use {@link HIVE_IGNORE_EXCEPTIONS} and its methods instead */
  @Deprecated
  val DEFAULT_HIVE_IGNORE_EXCEPTIONS_OPT_KEY = HiveSyncConfigHolder.HIVE_IGNORE_EXCEPTIONS.defaultValue()
  /** @deprecated Use {@link HIVE_SKIP_RO_SUFFIX_FOR_READ_OPTIMIZED_TABLE} and its methods instead */
  @Deprecated
  val HIVE_SKIP_RO_SUFFIX = HiveSyncConfigHolder.HIVE_SKIP_RO_SUFFIX_FOR_READ_OPTIMIZED_TABLE.key()
  /** @deprecated Use {@link HIVE_SKIP_RO_SUFFIX_FOR_READ_OPTIMIZED_TABLE} and its methods instead */
  @Deprecated
  val DEFAULT_HIVE_SKIP_RO_SUFFIX_VAL = HiveSyncConfigHolder.HIVE_SKIP_RO_SUFFIX_FOR_READ_OPTIMIZED_TABLE.defaultValue()
  /** @deprecated Use {@link HIVE_SUPPORT_TIMESTAMP_TYPE} and its methods instead */
  @Deprecated
  val HIVE_SUPPORT_TIMESTAMP = HiveSyncConfigHolder.HIVE_SUPPORT_TIMESTAMP_TYPE.key()
  /** @deprecated Use {@link HIVE_SUPPORT_TIMESTAMP_TYPE} and its methods instead */
  @Deprecated
  val DEFAULT_HIVE_SUPPORT_TIMESTAMP = HiveSyncConfigHolder.HIVE_SUPPORT_TIMESTAMP_TYPE.defaultValue()
  /** @deprecated Use {@link ASYNC_COMPACT_ENABLE} and its methods instead */
  @Deprecated
  val ASYNC_COMPACT_ENABLE_OPT_KEY = ASYNC_COMPACT_ENABLE.key()
  /** @deprecated Use {@link ASYNC_COMPACT_ENABLE} and its methods instead */
  @Deprecated
  val DEFAULT_ASYNC_COMPACT_ENABLE_OPT_VAL = ASYNC_COMPACT_ENABLE.defaultValue()
  /** @deprecated Use {@link KAFKA_AVRO_VALUE_DESERIALIZER_CLASS} and its methods instead */
  @Deprecated
  val KAFKA_AVRO_VALUE_DESERIALIZER = KAFKA_AVRO_VALUE_DESERIALIZER_CLASS.key()
  @Deprecated
  val SCHEMA_PROVIDER_CLASS_PROP = STREAMER_CONFIG_PREFIX + "schemaprovider.class"
}

object DataSourceOptionsHelper {

  private val log = LoggerFactory.getLogger(DataSourceOptionsHelper.getClass)

  // put all the configs with alternatives here
  private val allConfigsWithAlternatives = List(
    DataSourceReadOptions.QUERY_TYPE,
    DataSourceWriteOptions.TABLE_TYPE,
    HoodieTableConfig.BASE_FILE_FORMAT,
    HoodieTableConfig.LOG_FILE_FORMAT
  )

  // put all the deprecated configs here
  val allDeprecatedConfigs: Set[String] = Set(
    ConsistencyGuardConfig.ENABLE.key
  )

  // maps the deprecated config name to its latest name
  val allAlternatives: Map[String, String] = {
    val alterMap = scala.collection.mutable.Map[String, String]()
    allConfigsWithAlternatives.foreach(cfg => cfg.getAlternatives.asScala.foreach(alternative => alterMap(alternative) = cfg.key))
    alterMap.toMap
  }

  val viewTypeValueMap: Map[String, String] = Map(
    DataSourceReadOptions.VIEW_TYPE_READ_OPTIMIZED_OPT_VAL -> DataSourceReadOptions.QUERY_TYPE_SNAPSHOT_OPT_VAL,
    DataSourceReadOptions.VIEW_TYPE_INCREMENTAL_OPT_VAL -> DataSourceReadOptions.QUERY_TYPE_INCREMENTAL_OPT_VAL,
    DataSourceReadOptions.VIEW_TYPE_REALTIME_OPT_VAL -> DataSourceReadOptions.QUERY_TYPE_SNAPSHOT_OPT_VAL)

  def translateConfigurations(optParams: Map[String, String]): Map[String, String] = {
    val translatedOpt = scala.collection.mutable.Map[String, String]() ++= optParams
    if (!SparkConfigUtils.containsConfigProperty(optParams, HoodieTableConfig.NAME) &&
      SparkConfigUtils.containsConfigProperty(optParams, DataSourceWriteOptions.TABLE_NAME)) {
      translatedOpt.put(HoodieTableConfig.NAME.key(),
        SparkConfigUtils.getStringWithAltKeys(optParams, DataSourceWriteOptions.TABLE_NAME))
    }
    optParams.keySet.foreach(opt => {
      if (allAlternatives.contains(opt) && !optParams.contains(allAlternatives(opt))) {
        log.warn(opt + " is deprecated and will be removed in a later release; Please use " + allAlternatives(opt))
        if (opt == DataSourceReadOptions.VIEW_TYPE_OPT_KEY) {
          // special handle for VIEW_TYPE, also need to translate its values
          translatedOpt ++= Map(allAlternatives(opt) -> viewTypeValueMap(optParams(opt)))
        } else {
          translatedOpt ++= Map(allAlternatives(opt) -> optParams(opt))
        }
      }
      if (allDeprecatedConfigs.contains(opt)) {
        log.warn(opt + " is deprecated and should never be used anymore")
      }
    })
    translatedOpt.toMap
  }

  /**
   * Some config keys differ from what user sets and whats part of table Config. this method assists in fetching the
   * right table config and populating write configs.
   * @param tableConfig table config of interest.
   * @param params incoming write params.
   * @return missing params that needs to be added to incoming write params
   */
  def fetchMissingWriteConfigsFromTableConfig(tableConfig: HoodieTableConfig, params: Map[String, String]) : Map[String, String] = {
    val missingWriteConfigs = scala.collection.mutable.Map[String, String]()
    if (!params.contains(KeyGeneratorOptions.RECORDKEY_FIELD_NAME.key()) && tableConfig.getRawRecordKeyFieldProp != null) {
      missingWriteConfigs ++= Map(KeyGeneratorOptions.RECORDKEY_FIELD_NAME.key() -> tableConfig.getRawRecordKeyFieldProp)
    }
    if (!params.contains(KeyGeneratorOptions.PARTITIONPATH_FIELD_NAME.key()) && HoodieTableConfig.getPartitionFieldPropForKeyGenerator(tableConfig).isPresent) {
      missingWriteConfigs ++= Map(KeyGeneratorOptions.PARTITIONPATH_FIELD_NAME.key() -> HoodieTableConfig.getPartitionFieldPropForKeyGenerator(tableConfig).get())
    }
    if (!params.contains(DataSourceWriteOptions.KEYGENERATOR_CLASS_NAME.key()) && tableConfig.getKeyGeneratorClassName != null) {
      missingWriteConfigs ++= Map(DataSourceWriteOptions.KEYGENERATOR_CLASS_NAME.key() -> tableConfig.getKeyGeneratorClassName)
    }
    if (!params.contains(HoodieWriteConfig.PRECOMBINE_FIELD_NAME.key()) && tableConfig.getPreCombineFieldsStr.isPresent) {
      missingWriteConfigs ++= Map(HoodieWriteConfig.PRECOMBINE_FIELD_NAME.key -> tableConfig.getPreCombineFieldsStr.orElse(null))
    }
    if (!params.contains(HoodieWriteConfig.WRITE_PAYLOAD_CLASS_NAME.key()) && tableConfig.getPayloadClass != null) {
      missingWriteConfigs ++= Map(HoodieWriteConfig.WRITE_PAYLOAD_CLASS_NAME.key() -> tableConfig.getPayloadClass)
    }
    if (!params.contains(HoodieWriteConfig.RECORD_MERGE_MODE.key()) && tableConfig.getRecordMergeMode != null) {
      missingWriteConfigs ++= Map(HoodieWriteConfig.RECORD_MERGE_MODE.key() -> tableConfig.getRecordMergeMode.name())
    }
    if (!params.contains(HoodieWriteConfig.RECORD_MERGE_STRATEGY_ID.key()) && tableConfig.getRecordMergeStrategyId != null) {
      missingWriteConfigs ++= Map(HoodieWriteConfig.RECORD_MERGE_STRATEGY_ID.key() -> tableConfig.getRecordMergeStrategyId)
    }
    if (!params.contains(DataSourceWriteOptions.TABLE_TYPE.key())) {
      missingWriteConfigs ++= Map(DataSourceWriteOptions.TABLE_TYPE.key() -> tableConfig.getTableType.name())
    }
    missingWriteConfigs.toMap
  }

  def parametersWithReadDefaults(parameters: Map[String, String]): Map[String, String] = {
    // First check if the ConfigUtils.IS_QUERY_AS_RO_TABLE has set by HiveSyncTool,
    // or else use query type from QUERY_TYPE.
    val paramsWithGlobalProps = DFSPropertiesConfiguration.getGlobalProps.asScala.toMap ++ parameters
    val queryType = paramsWithGlobalProps.get(IS_QUERY_AS_RO_TABLE)
      .map(is => if (is.toBoolean) QUERY_TYPE_READ_OPTIMIZED_OPT_VAL else QUERY_TYPE_SNAPSHOT_OPT_VAL)
      .getOrElse(paramsWithGlobalProps.getOrElse(QUERY_TYPE.key, QUERY_TYPE.defaultValue()))

    Map(
      QUERY_TYPE.key -> queryType
    ) ++ translateConfigurations(paramsWithGlobalProps)
  }

  def inferKeyGenClazz(props: TypedProperties): String = {
    getKeyGeneratorClassNameFromType(inferKeyGeneratorTypeFromWriteConfig(props))
  }

  def inferKeyGenClazz(recordsKeyFields: String, partitionFields: String): String = {
    getKeyGeneratorClassNameFromType(inferKeyGeneratorType(Option.ofNullable(recordsKeyFields), partitionFields))
  }

  /**
   * Returns optional list of precombine fields from the provided parameteres.
   */
  def getPreCombineFields(params: Map[String, String]): Option[java.util.List[String]] = params.get(DataSourceWriteOptions.PRECOMBINE_FIELD.key) match {
    // NOTE: This is required to compensate for cases when empty string is used to stub
    //       property value to avoid it being set with the default value
    // TODO(HUDI-3456) cleanup
    case Some(f) if !StringUtils.isNullOrEmpty(f) => Option.of(java.util.Arrays.asList(f.split(","): _*))
    case _ => Option.empty()
  }

  implicit def convert[T, U](prop: ConfigProperty[T])(implicit converter: T => U): ConfigProperty[U] = {
    checkState(prop.hasDefaultValue)
    var newProp: ConfigProperty[U] = ConfigProperty.key(prop.key())
      .defaultValue(converter(prop.defaultValue()))
      .withDocumentation(prop.doc())
      .withAlternatives(prop.getAlternatives.asScala.toSeq: _*)

    newProp = toScalaOption(prop.getSinceVersion) match {
      case Some(version) => newProp.sinceVersion(version)
      case None => newProp
    }
    newProp = toScalaOption(prop.getDeprecatedVersion) match {
      case Some(version) => newProp.deprecatedAfter(version)
      case None => newProp
    }

    newProp
  }
}
