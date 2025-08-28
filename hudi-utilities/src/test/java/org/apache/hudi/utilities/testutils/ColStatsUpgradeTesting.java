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

package org.apache.hudi.utilities.testutils;

import org.apache.hudi.common.testutils.HoodieTestDataGenerator;
import org.apache.hudi.common.testutils.HoodieTestUtils;
import org.apache.hudi.storage.HoodieStorage;
import org.apache.hudi.storage.StoragePath;

import org.apache.avro.Schema;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.io.OutputStreamWriter;
import java.io.Writer;
import java.util.List;

import static org.apache.hudi.common.testutils.HoodieTestDataGenerator.TRIP_LOGICAL_TYPES_SCHEMA;
import static org.apache.hudi.common.testutils.HoodieTestDataGenerator.recordsToStrings;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class ColStatsUpgradeTesting {

  @Test
  public void generate() throws IOException {
    generateTestAssets("/tmp/colstats-upgrade-test");
  }

  public void generateTestAssets(String assetDirectory) throws IOException {
    HoodieStorage storage = HoodieTestUtils.getDefaultStorage();
    StoragePath directory = new StoragePath(assetDirectory);
    if (!storage.exists(directory)) {
      assertTrue(storage.createDirectory(directory));
    }
    Schema schema = HoodieTestDataGenerator.AVRO_TRIP_LOGICAL_TYPES_SCHEMA;

    StoragePath schemaFile = new StoragePath(directory, "schema.avsc");

    try (Writer writer = new OutputStreamWriter(storage.create(schemaFile))) {
      writer.write(schema.toString(true));
      writer.write("\n");
    }

    StoragePath propsFile = new StoragePath(directory, "hudi.properties");
    try (Writer writer = new OutputStreamWriter(storage.create(propsFile))) {
      writer.write("hoodie.table.name=trips_logical_types_json\n");
      writer.write("hoodie.datasource.write.table.type=MERGE_ON_READ\n");
      writer.write("hoodie.datasource.write.recordkey.field=uuid\n");
      writer.write("hoodie.datasource.write.partitionpath.field=partition_path\n");
      writer.write("hoodie.datasource.write.keygenerator.class=org.apache.hudi.keygen.SimpleKeyGenerator\n");
      writer.write("hoodie.datasource.write.precombine.field=timestamp\n");
      writer.write("hoodie.cleaner.policy=KEEP_LATEST_COMMITS\n");
      writer.write("hoodie.cleaner.commits.retained=2\n");
      writer.write("hoodie.upsert.shuffle.parallelism=2\n");
      writer.write("hoodie.insert.shuffle.parallelism=2\n");
    }

    HoodieTestDataGenerator datagen = new HoodieTestDataGenerator();

    for (int i = 0; i < 5; i++) {
      StoragePath dataFile = new StoragePath(directory, "data_" + i + ".json");
      List<String> records = recordsToStrings(i == 0
          ? datagen.generateInsertsAsPerSchema("00" + i, 20, TRIP_LOGICAL_TYPES_SCHEMA)
          : datagen.generateUniqueUpdatesAsPerSchema("00" + i, 10, TRIP_LOGICAL_TYPES_SCHEMA));
      try (Writer writer = new OutputStreamWriter(storage.create(dataFile))) {
        for (String record : records) {
          writer.write(record);
          writer.write("\n");
        }
      }
    }
  }
}
