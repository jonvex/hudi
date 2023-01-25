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

package org.apache.hudi.common.table.timeline.dto;

import org.apache.hudi.common.model.HoodieFileGroup;
import org.apache.hudi.common.table.HoodieTableMetaClient;
import org.apache.hudi.common.util.Option;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;

import javax.annotation.Nullable;

import java.util.List;
import java.util.stream.Collectors;

/**
 * The data transfer object of file group.
 */
@JsonIgnoreProperties(ignoreUnknown = true)
public class FileGroupDTO {

  @JsonProperty("partition")
  String partition;

  @JsonProperty("fileId")
  String id;

  @JsonProperty("slices")
  List<FileSliceDTO> slices;

  @JsonProperty("timeline")
  TimelineDTO timeline;

  @JsonProperty("firstActiveInstant")
  @Nullable
  InstantDTO firstActiveInstant;

  public static FileGroupDTO fromFileGroup(HoodieFileGroup fileGroup) {
    FileGroupDTO dto = new FileGroupDTO();
    dto.partition = fileGroup.getPartitionPath();
    dto.id = fileGroup.getFileGroupId().getFileId();
    dto.slices = fileGroup.getAllRawFileSlices().map(FileSliceDTO::fromFileSlice).collect(Collectors.toList());
    dto.timeline = TimelineDTO.fromTimeline(fileGroup.getTimeline());
    dto.firstActiveInstant = InstantDTO.fromInstant(fileGroup.getFirstActiveInstant().isPresent() ? fileGroup.getFirstActiveInstant().get() : null);
    return dto;
  }

  public static HoodieFileGroup toFileGroup(FileGroupDTO dto, HoodieTableMetaClient metaClient) {
    HoodieFileGroup fileGroup =
        new HoodieFileGroup(dto.partition, dto.id, TimelineDTO.toTimeline(dto.timeline, metaClient),
            dto.firstActiveInstant == null ? Option.empty() : Option.of(InstantDTO.toInstant(dto.firstActiveInstant)));
    dto.slices.stream().map(FileSliceDTO::toFileSlice).forEach(fileSlice -> fileGroup.addFileSlice(fileSlice));
    return fileGroup;
  }
}
