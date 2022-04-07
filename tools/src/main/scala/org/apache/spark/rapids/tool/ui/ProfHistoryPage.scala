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

class ProfHistoryPage(
    parent: ProfCompareTab,
    conf: SparkConf,
    rapidsStore: RapidsAppStatusStore) extends WebUIPage("") with Logging {

  val  displayApplications = true
  val maxApplications = 1000
  val requestedIncomplete = false
  val threadDumpEnabled = false
  override def render(request: HttpServletRequest): Seq[Node] = {
    val content = {
      <a href={makePageLink(request)}>{"Update applications list"}</a>
      <script src={SparkUIUtils.prependBaseUri(
          request, "/static/historypage-common.js")}></script> ++
      <script src={SparkUIUtils.prependBaseUri(
          request, "/static/utils.js")}></script>
      <div>
        <div class="container-fluid">
          {
            <p>Last updated: <span id="time-zone"></span></p>
          }
          {
            <script src={SparkUIUtils.prependBaseUri(
                  request, "/static/dataTables.rowsGroup.js")}></script> ++
            <div id="profile-history-summary"></div> ++
            <script src={SparkUIUtils.prependBaseUri(
                  request, "/static/rapids/rapids-profile-historypage.js")}></script> ++
            <script>setCurrentAppID('{parent.rapidsTab.currAppID}')</script> ++
            <script>setRapidsHistoryAppLimit({parent.historyAppLimit})</script>
          }
        </div>
      </div>
    }

    SparkUIUtils.headerSparkPage(
      request, "List of Applications", content, parent, useDataTables = true)
  }

  private def makePageLink(request: HttpServletRequest): String = {
    SparkUIUtils.prependBaseUri(request, parent.basePath + "/compare")
  }
}
