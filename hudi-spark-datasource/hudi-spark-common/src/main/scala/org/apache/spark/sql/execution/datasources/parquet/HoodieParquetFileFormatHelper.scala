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

package org.apache.spark.sql.execution.datasources.parquet

import org.apache.hadoop.conf.Configuration
import org.apache.parquet.hadoop.metadata.FileMetaData
import org.apache.spark.sql.types.{ArrayType, DataType, MapType, StructField, StructType}

object HoodieParquetFileFormatHelper {

  def buildImplicitSchemaChangeInfo(hadoopConf: Configuration,
                                    parquetFileMetaData: FileMetaData,
                                    requiredSchema: StructType): (java.util.Map[Integer, org.apache.hudi.common.util.collection.Pair[DataType, DataType]], StructType) = {
    val implicitTypeChangeInfo: java.util.Map[Integer, org.apache.hudi.common.util.collection.Pair[DataType, DataType]] = new java.util.HashMap()
    val convert = new ParquetToSparkSchemaConverter(hadoopConf)
    val fileStruct = convert.convert(parquetFileMetaData.getSchema)
    val fileStructMap = fileStruct.fields.map(f => (f.name, f.dataType)).toMap
    val sparkRequestStructFields = requiredSchema.map(f => {
      val requiredType = f.dataType
      if (fileStructMap.contains(f.name)) {
        isDataTypeEqual(requiredType, fileStructMap(f.name)) match {
          case None => f
          case Some(castedType) =>
            implicitTypeChangeInfo.put(new Integer(requiredSchema.fieldIndex(f.name)), org.apache.hudi.common.util.collection.Pair.of(castedType, fileStructMap(f.name)))
            StructField(f.name, fileStructMap(f.name), f.nullable)
        }
      } else {
        f
      }
    })
    (implicitTypeChangeInfo, StructType(sparkRequestStructFields))
  }

  def isDataTypeEqual(requiredType: DataType, fileType: DataType): Option[DataType] = (requiredType, fileType) match {
    case (requiredType, fileType) if requiredType == fileType => None

    case (ArrayType(rt, _), ArrayType(ft, _)) =>
      // Do not care about nullability as schema evolution require fields to be nullable
      isDataTypeEqual(rt, ft)

    case (MapType(requiredKey, requiredValue, _), MapType(fileKey, fileValue, _)) =>
      // Likewise, do not care about nullability as schema evolution require fields to be nullable
      (isDataTypeEqual(requiredKey, fileKey), isDataTypeEqual(requiredValue, fileValue)) match {
        case (None, None) => None
        case (None, Some(castedValue)) => Some(MapType(requiredKey, castedValue))
        case (Some(castedKey), None) => Some(MapType(castedKey, requiredValue))
        case (Some(castedKey), Some(castedValue)) => Some(MapType(castedKey, castedValue))
      }

    case (StructType(requiredFields), StructType(fileFields)) =>
      // Find fields that are in requiredFields and fileFields as they might not be the same during add column + change column operations
      val commonFieldNames = requiredFields.map(_.name) intersect fileFields.map(_.name)

      // Need to match by name instead of StructField as name will stay the same whilst type may change
      val fileFilteredFields = fileFields.filter(f => commonFieldNames.contains(f.name)).sortWith(_.name < _.name)
      val requiredFilteredFields = requiredFields.filter(f => commonFieldNames.contains(f.name)).sortWith(_.name < _.name)

      // Sorting ensures that the same field names are being compared for type differences
      val fieldZip = requiredFilteredFields.zip(fileFilteredFields)
      val options  = fieldZip.map(fields => fields._1.name -> isDataTypeEqual(fields._1.dataType, fields._2.dataType)).toMap
      if (options.values.forall(_.isEmpty)) {
        None
      } else {
        Some(StructType(requiredFields.collect{
          case f if options.contains(f.name) => options(f.name) match {
            case Some(casted) => StructField(f.name, casted, f.nullable)
            case None => f
          }
        }))
      }

    case _ => Some(requiredType)
  }
}
