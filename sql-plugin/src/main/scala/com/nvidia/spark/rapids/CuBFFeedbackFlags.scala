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

package com.nvidia.spark.rapids

import scala.util.control.NonFatal

import org.apache.spark.sql.internal.SQLConf

/**
 * Read-only mirror of CuBF runtime-feedback flags owned by the planner module.
 *
 * The executor uses these keys to decide whether to wire optional metrics accumulators.
 */
object CuBFFeedbackFlags {

  private[rapids] val RUNTIME_FEEDBACK_ENABLED_KEY =
    "spark.rapids.sql.cuBloomFilter.runtimeFeedback.enabled"

  private[rapids] val RUNTIME_FEEDBACK_INSTRUMENTATION_ENABLED_KEY =
    "spark.rapids.sql.cuBloomFilter.runtimeFeedback.instrumentation.enabled"

  /** True only when all feedback flags parse as true; malformed values fail closed. */
  def isEnabled(conf: SQLConf): Boolean =
    flagEnabled(conf, RUNTIME_FEEDBACK_ENABLED_KEY) &&
      flagEnabled(conf, RUNTIME_FEEDBACK_INSTRUMENTATION_ENABLED_KEY)

  private def flagEnabled(conf: SQLConf, key: String): Boolean = {
    try conf.getConfString(key, "false").toBoolean
    catch { case NonFatal(_) => false }
  }
}
