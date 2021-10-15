/*
 * Copyright (c) 2021, NVIDIA CORPORATION.
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

package com.nvidia.spark.rapids.tool.profiling

import com.nvidia.spark.rapids.tool.EventLogPathProcessor
import org.apache.hadoop.conf.Configuration

import org.apache.spark.internal.Logging

/**
 * A profiling tool to parse Spark Event Log
 */
object ProfileMain extends Logging {
  /**
   * Entry point from spark-submit running this as the driver.
   */
  def main(args: Array[String]) {
    val exitCode = mainInternal(new ProfileArgs(args))
    if (exitCode != 0) {
      System.exit(exitCode)
    }
  }

  /**
   * Entry point for tests
   */
  def mainInternal(appArgs: ProfileArgs): Int = {

    // Parsing args
    val eventlogPaths = appArgs.eventlog()
    val filterN = appArgs.filterCriteria
    val matchEventLogs = appArgs.matchEventLogs
    val hadoopConf = new Configuration()

    // Get the event logs required to process
    val (eventLogFsFiltered, _) = EventLogPathProcessor.processAllPaths(filterN.toOption,
      matchEventLogs.toOption, eventlogPaths, hadoopConf)
    if (eventLogFsFiltered.isEmpty) {
      logWarning("No event logs to process after checking paths, exiting!")
      return 0
    }
    val profiler = new Profiler(hadoopConf, appArgs)
    profiler.profile(eventLogFsFiltered)
    0
  }
}
