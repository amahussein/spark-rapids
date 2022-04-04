/*
 * Copyright (c) 2022, NVIDIA CORPORATION.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.spark.rapids.tool.status

import org.apache.spark.SparkConf
import org.apache.spark.internal.config
import org.apache.spark.rapids.tool.config.RapidsToolConf
import org.apache.spark.sql.rapids.tool.profiling.EventsProcessor
import org.apache.spark.status.AppStatusStore
import org.apache.spark.status.api.v1.ApplicationInfo
import org.apache.spark.util.kvstore.KVStore

class RapidsAppStatusStore(
    val appStore: AppStatusStore,
    val store: KVStore,
    val listener: Option[EventsProcessor] = None) {
  val sparkProperties = appStore.environmentInfo().sparkProperties.toMap
  def applicationInfo(): ApplicationInfo = appStore.applicationInfo()
  def getRapidsParams: Map[String, String] = {
    sparkProperties.filterKeys { key =>
      key.startsWith("spark.rapids") || key.startsWith("spark.executorEnv.UCX") ||
        key.startsWith("spark.shuffle.manager") || key.startsWith("spark.shuffle.service.enabled")
    }
  }
  def getRapidsJARInfo(conf: SparkConf): Seq[(String, String)] = {
    appStore.environmentInfo().classpathEntries.filter(_._1 matches(
      conf.get(RapidsToolConf.RAPIDS_JARS_REGEX).toString()))
  }
  def isRapidsEnabled(): Boolean = {
    sparkProperties.getOrElse(config.PLUGINS.key, "").contains("com.nvidia.spark.SQLPlugin") &&
      sparkProperties.getOrElse("spark.rapids.sql.enabled", "true").toBoolean
  }
  def getGeneralConfig: Map[String, String] = {
    Map("Plugin Enabled" -> isRapidsEnabled().toString)
  }

  def createRapidsProfAppInfo(logLines: Seq[String]): Unit = {
    val myAppID = applicationInfo().id
  }
}
