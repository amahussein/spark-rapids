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

package org.apache.spark.rapids.tool.status

import java.io.{InputStream, IOException}
import java.net.{HttpURLConnection, URL}
import java.nio.charset.StandardCharsets
import java.util.zip.ZipInputStream
import javax.servlet.http.HttpServletRequest

import scala.collection.mutable.ListBuffer
import scala.io.{Codec, Source}

import org.apache.commons.io.IOUtils

import org.apache.spark.internal.Logging
import org.apache.spark.ui.{UIUtils => SparkUIUtils}

object UIUtils extends Logging {
  def constructAPIPath(request: HttpServletRequest) : Option[String] = {
    val path = SparkUIUtils.uiRoot(request)
    if (path.isEmpty) {
      val alternatePath = request.getRequestURL
      var ind = alternatePath.indexOf("history")
      if (ind > 0) {
        return Some(request.getRequestURL.substring(0, ind - 1))
      }
    }
    None
  }

  def constructURLForLogFile(request: HttpServletRequest, appID: String) : URL = {
    val baseURL = constructAPIPath(request).getOrElse("http://localhost:18080")
    new URL(s"$baseURL/api/v1/applications/$appID/logs")
  }

  def connectAndGetInputStream(url: URL): (Int, Option[InputStream], Option[String]) = {
    val connection = url.openConnection().asInstanceOf[HttpURLConnection]
    connection.setRequestMethod("GET")
    connection.connect()
    val code = connection.getResponseCode()
    val inStream = try {
      Option(connection.getInputStream())
    } catch {
      case io: IOException => None
    }
    val errString = try {
      val err = Option(connection.getErrorStream())
      err.map(IOUtils.toString(_, StandardCharsets.UTF_8))
    } catch {
      case io: IOException => None
    }
    (code, inStream, errString)
  }

  def getContentAndCode(url: URL): (Int, Option[String], Option[String]) = {
    val (code, in, errString) = connectAndGetInputStream(url)
    val inString = in.map(IOUtils.toString(_, StandardCharsets.UTF_8))
    (code, inString, errString)
  }

  def getAppLogEventsByRest(request: HttpServletRequest, appID: String): Iterator[String] = {
    val (code, resultOpt, error) = connectAndGetInputStream(constructURLForLogFile(request, appID))
    val zipStream = new ZipInputStream(resultOpt.get)
    var allLines = collection.mutable.ListBuffer[Iterator[String]]()
    var entry = zipStream.getNextEntry
    while (entry != null) {
      allLines += Source.fromInputStream(zipStream)(Codec.UTF8).getLines()
      entry = zipStream.getNextEntry
    }
    logInfo(s"getAppLogEventsByRest Found All the Logs")
    allLines.foldLeft(Iterator[String]())(_ ++ _)
  }

  def getSeqAppLogEventsByRest(request: HttpServletRequest, appID: String): Seq[String] = {
    val (code, resultOpt, error) = connectAndGetInputStream(constructURLForLogFile(request, appID))
    val zipStream = new ZipInputStream(resultOpt.get)
    var allFiles = new ListBuffer[String]()
    var entry = zipStream.getNextEntry
    while (entry != null) {
      val allLines = Source.fromInputStream(zipStream)(Codec.UTF8).getLines().toList
      //val actual = new String(ByteStreams.toByteArray(zipStream), StandardCharsets.UTF_8)
      allFiles ++= allLines
      entry = zipStream.getNextEntry
    }
    logInfo(s"Found All the Logs ${allFiles.size}")
    allFiles
  }

//  def getSeqSparkLogEventsByRest(
//      request: HttpServletRequest, appID: String): Seq[SparkListenerEvent] = {
//    val (code, resultOpt, error) = connectAndGetInputStream(constructURLForLogFile(request, appID))
//    val zipStream = new ZipInputStream(resultOpt.get)
//    var allFiles = new ListBuffer[String]()
//    var entry = zipStream.getNextEntry
//    while (entry != null) {
//      val actual = new String(ByteStreams.toByteArray(zipStream), StandardCharsets.UTF_8)
//      allFiles += actual
//      entry = zipStream.getNextEntry
//    }
//    logInfo(s"Found All the Logs")
//    allFiles
//  }
}
