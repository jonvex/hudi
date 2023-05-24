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

package org.apache.hudi.io.storage;

import org.apache.hudi.common.util.Option;

import org.apache.avro.generic.IndexedRecord;

import java.util.Objects;

public class HoodieAvroBootstrapFileReader extends HoodieBootstrapFileReader<IndexedRecord> {

  public HoodieAvroBootstrapFileReader(HoodieFileReader<IndexedRecord> skeletonFileReader, HoodieFileReader<IndexedRecord> dataFileReader, Option<String[]> partitionFields, Object[] partitionValues) {
    super(skeletonFileReader, dataFileReader, partitionFields, partitionValues);
  }

  @Override
  protected void setPartitionField(int position, Object fieldValue, IndexedRecord row) {
    if (Objects.isNull(row.get(position))) {
      if (fieldValue.getClass().getName().equals("org.apache.spark.unsafe.types.UTF8String")) {
        row.put(position, String.valueOf(fieldValue));
      } else {
        row.put(position, fieldValue);
      }
    }
  }
}
