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

package org.apache.spark.rapids.tool.config

import org.apache.spark.internal.config.ConfigBuilder

object RapidsToolConf {

  val RAPIDS_JARS_REGEX = ConfigBuilder("spark.rapids.tool.jar.regex")
    .doc("Regex to decide which jars are part of the spark-rapids plugin.")
    .regexConf
    .createWithDefault("(.*rapids-4-spark.*jar)|(.*cudf.*jar)".r)
}
