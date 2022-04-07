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

import com.nvidia.spark.rapids.tool.profiling.{Analysis, ApplicationSummaryInfo, CollectInformation, HealthCheck}

import org.apache.spark.rapids.tool.status.{RapidsAppInfoLogProcessor, RapidsAppStatusStore, UIUtils}
import org.apache.spark.ui.{SparkUI, SparkUITab}

class RapidsTab(
    val parent: SparkUI,
    store: RapidsAppStatusStore) extends SparkUITab(parent, "RAPIDS") {

  attachPage(new RapidsPage(this, parent.conf, store))
  parent.attachTab(this)
  parent.addStaticHandler(RapidsTab.STATIC_RESOURCE_DIR, "/static/rapids")

  lazy val currAppID = store.appStore.applicationInfo().id

  def getRapidsProfileInfoForApp(
      request: HttpServletRequest, appID: String):
        (RapidsAppInfoLogProcessor, CollectInformation, ApplicationSummaryInfo) = {
    val logLines = UIUtils.getSeqAppLogEventsByRest(request, appID)
    val appProcessContainer = new RapidsAppInfoLogProcessor()
    appProcessContainer.processLogLines(logLines)
    val apps = Seq(appProcessContainer)
    val collect = new CollectInformation(apps)
    val appInfo = collect.getAppInfo
    val dsInfo = collect.getDataSourceInfo
    val execInfo = collect.getExecutorInfo
    val jobInfo = collect.getJobInfo
    val rapidsProps = collect.getProperties(rapidsOnly = true)
    val sparkProps = collect.getProperties(rapidsOnly = false)
    val rapidsJar = collect.getRapidsJARInfo
    val sqlMetrics = collect.getSQLPlanMetrics
    val analysis = new Analysis(apps)
    val jsMetAgg = analysis.jobAndStageMetricsAggregation()
    val sqlTaskAggMetrics = analysis.sqlMetricsAggregation()
    val durAndCpuMet = analysis.sqlMetricsAggregationDurationAndCpuTime()
    val skewInfo = analysis.shuffleSkewCheck()
    val healthCheck = new HealthCheck(apps)
    val failedTasks = healthCheck.getFailedTasks
    val failedStages = healthCheck.getFailedStages
    val failedJobs = healthCheck.getFailedJobs
    val removedBMs = healthCheck.getRemovedBlockManager
    val removedExecutors = healthCheck.getRemovedExecutors
    val unsupportedOps = healthCheck.getPossibleUnsupportedSQLPlan
    (appProcessContainer, collect, ApplicationSummaryInfo(appInfo, dsInfo, execInfo, jobInfo,
      rapidsProps, rapidsJar, sqlMetrics, jsMetAgg, sqlTaskAggMetrics, durAndCpuMet, skewInfo,
      failedTasks, failedStages, failedJobs, removedBMs, removedExecutors, unsupportedOps,
      sparkProps))
  }
}

object RapidsTab {
  private val STATIC_RESOURCE_DIR = "org/apache/spark/rapids/tool/ui/static"
}