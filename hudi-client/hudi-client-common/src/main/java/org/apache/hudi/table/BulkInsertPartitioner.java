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

package org.apache.hudi.table;

import org.apache.hudi.common.fs.FSUtils;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.config.HoodieWriteConfig;
import org.apache.hudi.execution.bulkinsert.BulkInsertSortMode;
import org.apache.hudi.io.WriteHandleFactory;
import org.apache.hudi.keygen.constant.KeyGeneratorOptions;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Set;

/**
 * Partitions the input records for bulk insert operation.
 * <p>
 * The actual implementation of {@link BulkInsertPartitioner} is determined by the bulk insert
 * sort mode, {@link BulkInsertSortMode}, specified by
 * {@code HoodieWriteConfig.BULK_INSERT_SORT_MODE} (`hoodie.bulkinsert.sort.mode`).
 */
public interface BulkInsertPartitioner<I> extends Serializable {

  /**
   * Partitions the input records based on the number of output partitions as a hint.
   * <p>
   * Note that, the number of output partitions may or may not be enforced, depending on the
   * specific implementation.
   *
   * @param records          Input Hoodie records.
   * @param outputPartitions Expected number of output partitions as a hint.
   * @return
   */
  I repartitionRecords(I records, int outputPartitions);

  /**
   * @return {@code true} if the records are sorted by partition-path; {@code false} otherwise.
   */
  boolean arePartitionRecordsSorted();

  /**
   * Return file group id prefix for the given data partition.
   * By defauult, return a new file group id prefix, so that incoming records will route to a fresh new file group
   *
   * @param partitionId data partition
   * @return
   */
  default String getFileIdPfx(int partitionId) {
    return FSUtils.createNewFileIdPfx();
  }

  /**
   * Return write handle factory for the given partition.
   *
   * @param partitionId data partition
   * @return
   */
  default Option<WriteHandleFactory> getWriteHandleFactory(int partitionId) {
    return Option.empty();
  }

  /*
   * If possible, we want to sort the data by partition path. Doing so will reduce the number of files written.
   **/
  static String[] prependPartitionPathColumn(String[] columnNames, HoodieWriteConfig config) {
    String partitionPath = config.getString(KeyGeneratorOptions.PARTITIONPATH_FIELD_NAME.key());
    if (partitionPath.isEmpty() || config.getMetadataConfig().populateMetaFields()) {
      //If we have meta fields we can just leverage those instead
      return columnNames;
    }
    ArrayList<String> sortCols = new ArrayList<>();
    Set<String> used = new HashSet<>();
    Arrays.stream(partitionPath.split(","))
        .map(String::trim)
        .filter(s -> !s.isEmpty())
        .forEach(col -> {
          sortCols.add(col);
          used.add(col);
        });
    for (String col : columnNames) {
      if (!used.contains(col)) {
        sortCols.add(col);
        used.add(col);
      }
    }
    return sortCols.toArray(new String[0]);
  }

}
