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

package org.apache.spark.rapids.tool.ui

import org.apache.spark.rapids.tool.status.RapidsAppStatusStore
import org.apache.spark.ui.{SparkUI, SparkUITab}

class ProfCompareTab(
    val parent: SparkUI,
    val rapidsTab: RapidsTab,
    store: RapidsAppStatusStore) extends SparkUITab(parent, "compare") {
  val historyAppLimit = 1024
  attachPage(new ProfHistoryPage(this, parent.conf, store))
  attachPage(new ProfComparePage(this, parent.conf, store))
  parent.attachTab(this)
  parent.addStaticHandler(ProfCompareTab.STATIC_RESOURCE_DIR, "/static/rapids")
}


object ProfCompareTab {
  private val STATIC_RESOURCE_DIR = "org/apache/spark/rapids/tool/ui/static"
}