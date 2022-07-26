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

import scala.xml.{Node, Unparsed}

import com.nvidia.spark.rapids.tool.profiling.ProfileResult

import org.json4s.DefaultFormats
import org.json4s.jackson.Serialization

import org.apache.spark.SparkConf
import org.apache.spark.internal.Logging
import org.apache.spark.rapids.tool.status.RapidsAppStatusStore
import org.apache.spark.ui.{UIUtils => SparkUIUtils, WebUIPage}

class ProfComparePage(
    parent: ProfCompareTab,
    conf: SparkConf,
    rapidsStore: RapidsAppStatusStore) extends WebUIPage("results") with Logging {

  private def getJsonData(profResultData: Seq[ProfileResult]): String = {
    implicit val formats = DefaultFormats
    val jSonString = Serialization.write(profResultData)
    val groupJsonArrayAsStr =
      s"""
         |${jSonString}
        """.stripMargin
    groupJsonArrayAsStr
  }

  override def render(request: HttpServletRequest): Seq[Node] = {
    val appIDParameter = Option(request.getParameter("id"))
    val currAppID = parent.rapidsTab.currAppID
    val profileResSummary =
      parent.getRapidsProfileComparisonForApps(request, Seq[String](currAppID, appIDParameter.get))
    val content = {
      <a href={makePageLink(request)}>{"Show List of All Applications"}</a>
      <script src={SparkUIUtils.prependBaseUri(
        request, "/static/historypage-common.js")}></script> ++
      <script src={SparkUIUtils.prependBaseUri(request, "/static/utils.js")}></script> ++
        <div>
          <div class="container-fluid">
            <span>
              <span class="collapse-aggregated-dataSourceReport collapse-table"
                    onClick="collapseTable('collapse-aggregated-dataSourceReport',
                      'aggregated-dataSourceReport')">
                <h4>
                  <span class="collapse-table-arrow arrow-open"></span>
                  <a>DataSource Information</a>
                </h4>
              </span>
              <div class="aggregated-dataSourceReport collapsible-table">
                {
                <script src={SparkUIUtils.prependBaseUri(
                  request, "/static/dataTables.rowsGroup.js")}></script> ++
                  <div id="datasource-report"></div> ++
                  <script src={SparkUIUtils.prependBaseUri(
                    request, "/static/rapids/datasource-report.js")}></script> ++
                  <script>enableAppIndexForDS()</script> ++
                  <script type="text/javascript">
                    {Unparsed(s"setDataSourceInfoArr(${getJsonData(profileResSummary.dsInfo)});")}
                  </script>
                }
              </div>
              <span class="collapse-aggregated-jobinfoReport collapse-table"
                    onClick="collapseTable('collapse-aggregated-jobinfoReport',
                      'aggregated-jobinfoReport')">
                <h4>
                  <span class="collapse-table-arrow arrow-open"></span>
                  <a>Job Information</a>
                </h4>
              </span>
              <div class="aggregated-jobinfoReport collapsible-table">
                {
                <script src={SparkUIUtils.prependBaseUri(
                  request, "/static/dataTables.rowsGroup.js")}></script> ++
                  <div id="jobinfo-report"></div> ++
                  <script src={SparkUIUtils.prependBaseUri(
                    request, "/static/rapids/jobinfo-report.js")}></script> ++
                  <script>enableAppIndexForJobs()</script> ++
                  <script type="text/javascript">
                    {Unparsed(s"setJobInfoArr(${getJsonData(profileResSummary.jobInfo)});")}
                  </script>
                }
              </div>
            </span>
          </div>
        </div>
    }


    SparkUIUtils.headerSparkPage(
      request, s"Profile Comparison Report $currAppID Vs ${appIDParameter.mkString}",
      content, parent, useDataTables = true)
  }
  private def makePageLink(request: HttpServletRequest): String = {
    SparkUIUtils.prependBaseUri(request, parent.basePath + "/compare")
  }

}
