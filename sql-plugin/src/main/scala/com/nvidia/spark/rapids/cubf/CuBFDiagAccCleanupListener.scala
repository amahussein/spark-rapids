/*
 * Copyright (c) 2026, NVIDIA CORPORATION.
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

package com.nvidia.spark.rapids.cubf

import org.apache.spark.internal.Logging
import org.apache.spark.scheduler.{SparkListener, SparkListenerEvent}
import org.apache.spark.sql.execution.ui.SparkListenerSQLExecutionEnd

/** Clears cuBF diagnostic accumulator cache entries when a SQL execution completes. */
class CuBFDiagAccCleanupListener extends SparkListener with Logging {

  override def onOtherEvent(event: SparkListenerEvent): Unit = {
    event match {
      case e: SparkListenerSQLExecutionEnd =>
        val removed = CuBFDiagPairMetric.removeForExecution(e.executionId)
        if (removed.total > 0) {
          logDebug(s"[CuBF-DiagAcc] SQLExecution ${e.executionId} ended, removed " +
            s"${removed.build} build and ${removed.probe} probe accumulator cache entries")
        }
      case _ => // ignore
    }
  }
}
