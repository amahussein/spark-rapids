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

import com.nvidia.spark.rapids.tool.profiling.{ApplicationSummaryInfo, ProfileResult}
import org.json4s.DefaultFormats
import org.json4s.jackson.Serialization
import org.apache.spark.SparkConf

import org.apache.spark.internal.Logging
import org.apache.spark.rapids.tool.status.RapidsAppStatusStore
import org.apache.spark.ui.{WebUIPage, UIUtils => SparkUIUtils}
import org.apache.spark.util.Utils

class RapidsPage(
    parent: RapidsTab,
    conf: SparkConf,
    rapidsStore: RapidsAppStatusStore) extends WebUIPage("") with Logging {
  private def jvmRow(kv: (String, String)) = <tr><td>{kv._1}</td><td>{kv._2}</td></tr>
  private def propertyRow(kv: (String, String)) = <tr><td>{kv._1}</td><td>{kv._2}</td></tr>
  private def propertyHeader = Seq("Name", "Value")
  private def headerClasses = Seq("sorttable_alpha", "sorttable_alpha")

  private def dataSourceInfoJson(appsummaryInfo: ApplicationSummaryInfo): String = {
    implicit val formats = DefaultFormats
    val jSonString = Serialization.write(appsummaryInfo.dsInfo)
    val groupJsonArrayAsStr =
      s"""
         |${jSonString}
        """.stripMargin
    groupJsonArrayAsStr
  }

  private def jobInfoJson(appsummaryInfo: ApplicationSummaryInfo): String = {
    implicit val formats = DefaultFormats
    val jSonString = Serialization.write(appsummaryInfo.jobInfo)
    val groupJsonArrayAsStr =
      s"""
         |${jSonString}
        """.stripMargin
    groupJsonArrayAsStr
  }

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
    val appInfo = rapidsStore.applicationInfo
    val (rapidsAppInfoObj, rapidsCollectInfoObj, rapidsSummary) =
      parent.getRapidsProfileInfoForApp(request, appInfo.id)
    val rapidsAppInfo = rapidsSummary.appInfo.head
    val rapidsAppPropsInfo = rapidsSummary.rapidsProps
    val tableRows = rapidsAppPropsInfo.map(_.convertToSeq).map(a => a.head -> a.last)
    val rapidsInfo = Map(
      "plugin Enabled" -> rapidsAppInfo.pluginEnabled.toString,
      "spark Version" -> rapidsAppInfo.sparkVersion)
    val runtimeInformationTable = SparkUIUtils.listingTable(
      propertyHeader, jvmRow, rapidsInfo.toSeq.sorted, fixedWidth = true,
      headerClasses = headerClasses)
    val rapidsPropertiesTable = SparkUIUtils.listingTable(propertyHeader, propertyRow,
      Utils.redact(conf, tableRows.sorted), fixedWidth = true,
      headerClasses = headerClasses)
    val content = {
      <script src={SparkUIUtils.prependBaseUri(
        request, "/static/historypage-common.js")}></script> ++
      <script src={SparkUIUtils.prependBaseUri(request, "/static/utils.js")}></script> ++
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
                <span class="collapse-aggregated-sparkProperties collapse-table"
                      onClick="collapseTable('collapse-aggregated-sparkProperties',
            'aggregated-sparkProperties')">
                  <h4>
                    <span class="collapse-table-arrow arrow-open"></span>
                    <a>Spark Rapids parameters set explicitly</a>
                  </h4>
                </span>
                <div class="aggregated-sparkProperties collapsible-table">
                  {rapidsPropertiesTable}
                </div>
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
                  <script type="text/javascript">
                      {Unparsed(s"setDataSourceInfoArr(${getJsonData(rapidsSummary.dsInfo)});")}
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
                  <script type="text/javascript">
                    {Unparsed(s"setJobInfoArr(${getJsonData(rapidsSummary.jobInfo)});")}
                  </script>
                }
                </div>
              </span>
            </div>
          </div>
    }

    SparkUIUtils.headerSparkPage(
      request, "RAPIDS", content, parent, useDataTables = true)
  }
}
