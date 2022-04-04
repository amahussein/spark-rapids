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

import javax.servlet.http.HttpServletRequest

import scala.xml.Node

import org.apache.spark.SparkConf
import org.apache.spark.internal.Logging
import org.apache.spark.rapids.tool.status.RapidsAppStatusStore
import org.apache.spark.ui.{UIUtils => SparkUIUtils, WebUIPage}

class RapidsPage(
    parent: RapidsTab,
    conf: SparkConf,
    rapidsStore: RapidsAppStatusStore) extends WebUIPage("") with Logging {
  private def jvmRow(kv: (String, String)) = <tr><td>{kv._1}</td><td>{kv._2}</td></tr>
  private def propertyHeader = Seq("Name", "Value")
  private def headerClasses = Seq("sorttable_alpha", "sorttable_alpha")

  override def render(request: HttpServletRequest): Seq[Node] = {
    val appInfo = rapidsStore.applicationInfo
    val (rapidsAppInfoObj, rapidsCollectInfoObj, rapidsSummaryInfo) =
      parent.getRapidsProfileInfoForApp(request, appInfo.id)
    val rapidsAppInfo = rapidsSummaryInfo.appInfo.head
    val rapidsAppPropsInfo = rapidsSummaryInfo.rapidsProps

    val rapidsInfo = Map(
      "plugin Enabled" -> rapidsAppInfo.pluginEnabled.toString,
      "spark Version" -> rapidsAppInfo.sparkVersion)
    val runtimeInformationTable = SparkUIUtils.listingTable(
      propertyHeader, jvmRow, rapidsInfo.toSeq.sorted, fixedWidth = true,
      headerClasses = headerClasses)

    val content =
      <script src={SparkUIUtils.prependBaseUri(
        request, "/static/historypage-common.js")}></script> ++
        <script src={SparkUIUtils.prependBaseUri(
          request, "/static/utils.js")}></script>
          <div>
            <div class="container-fluid">
              <span>
                <span class="collapse-aggregated-runtimeInformation collapse-table"
                      onClick="collapseTable('collapse-aggregated-runtimeInformation',
              'aggregated-runtimeInformation')">
                  <h4>
                    <span class="collapse-table-arrow arrow-open"></span>
                    <a>Runtime Information</a>
                  </h4>
                </span>
                <div class="aggregated-runtimeInformation collapsible-table">
                  {runtimeInformationTable}
                </div>
              </span>
            </div>
          </div>

    SparkUIUtils.headerSparkPage(
      request, "RAPIDS", content, parent, useDataTables = true)
  }
}
