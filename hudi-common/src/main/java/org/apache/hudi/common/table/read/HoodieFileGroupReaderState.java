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

package org.apache.hudi.common.table.read;

import org.apache.hudi.common.model.HoodieRecordMerger;

/**
 * A class holding the state that is needed by {@code HoodieFileGroupReader},
 * e.g., schema, merging strategy, etc.
 */
public class HoodieFileGroupReaderState<T> {
  public HoodieFileGroupReaderSchemaHandler<T> schemaHandler;
  public String tablePath;
  public String latestCommitTime;
  public HoodieRecordMerger recordMerger;
  public boolean hasLogFiles;
  public boolean hasBootstrapBaseFile;
  public boolean needsBootstrapMerge;



}
