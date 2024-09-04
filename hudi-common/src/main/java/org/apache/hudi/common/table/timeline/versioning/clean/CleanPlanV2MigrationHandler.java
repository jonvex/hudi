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

package org.apache.hudi.common.table.timeline.versioning.clean;

import org.apache.hudi.avro.model.HoodieCleanFileInfo;
import org.apache.hudi.avro.model.HoodieCleanerPlan;
import org.apache.hudi.common.fs.FSUtils;
import org.apache.hudi.common.table.HoodieTableMetaClient;
import org.apache.hudi.common.table.timeline.versioning.AbstractMigratorBase;
import org.apache.hudi.common.util.collection.Pair;
import org.apache.hudi.storage.StoragePath;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

/**
 * Migration handler for clean plan in version 2.
 */
public class CleanPlanV2MigrationHandler extends AbstractMigratorBase<HoodieCleanerPlan> {

  public static final Integer VERSION = 2;

  public CleanPlanV2MigrationHandler(HoodieTableMetaClient metaClient) {
    super(metaClient);
  }

  @Override
  public Integer getManagedVersion() {
    return VERSION;
  }

  @Override
  public HoodieCleanerPlan upgradeFrom(HoodieCleanerPlan plan) {
    return upgradeFromV2(plan, metaClient, VERSION);
  }

  static HoodieCleanerPlan upgradeFromV2(HoodieCleanerPlan plan, HoodieTableMetaClient metaClient, Integer version) {
    Map<String, List<HoodieCleanFileInfo>> filePathsPerPartition =
        plan.getFilesToBeDeletedPerPartition().entrySet().stream().map(e -> Pair.of(e.getKey(), e.getValue().stream()
            .map(v -> new HoodieCleanFileInfo(
                new StoragePath(FSUtils.constructAbsolutePath(metaClient.getBasePath(), e.getKey()), v).toString(), false))
            .collect(Collectors.toList()))).collect(Collectors.toMap(Pair::getKey, Pair::getValue));
    return new HoodieCleanerPlan(plan.getEarliestInstantToRetain(), plan.getLastCompletedCommitTimestamp(),
        plan.getPolicy(), new HashMap<>(), version, filePathsPerPartition, new ArrayList<>(), Collections.emptyMap());
  }

  @Override
  public HoodieCleanerPlan downgradeFrom(HoodieCleanerPlan input) {
    return new HoodieCleanerPlan(input.getEarliestInstantToRetain(), input.getLastCompletedCommitTimestamp(),
        input.getPolicy(), input.getFilesToBeDeletedPerPartition(), VERSION,
        input.getFilePathsToBeDeletedPerPartition(), input.getPartitionsToBeDeleted(), input.getExtraMetadata());
  }
}
