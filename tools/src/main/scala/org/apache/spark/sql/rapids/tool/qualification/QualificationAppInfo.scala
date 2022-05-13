/*
 * Copyright (c) 2021-2022, NVIDIA CORPORATION.
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

package org.apache.spark.sql.rapids.tool.qualification

import scala.collection.mutable.{ArrayBuffer, HashMap}

import com.nvidia.spark.rapids.tool.EventLogInfo
import com.nvidia.spark.rapids.tool.planparser.{ExecInfo, PlanInfo, SQLPlanParser}
import com.nvidia.spark.rapids.tool.profiling._
import com.nvidia.spark.rapids.tool.qualification._
import org.apache.hadoop.conf.Configuration

import org.apache.spark.internal.Logging
import org.apache.spark.scheduler.{SparkListener, SparkListenerEvent}
import org.apache.spark.sql.execution.SparkPlanInfo
import org.apache.spark.sql.execution.ui.SparkPlanGraph
import org.apache.spark.sql.rapids.tool.{AppBase, GpuEventLogException, ToolUtils}

class QualificationAppInfo(
    eventLogInfo: Option[EventLogInfo],
    hadoopConf: Option[Configuration] = None,
    pluginTypeChecker: PluginTypeChecker,
    readScorePercent: Int)
  extends AppBase(eventLogInfo, hadoopConf) with Logging {

  var appId: String = ""
  var lastJobEndTime: Option[Long] = None
  var lastSQLEndTime: Option[Long] = None
  var longestSQLDuration: Long = 0
  val writeDataFormat: ArrayBuffer[String] = ArrayBuffer[String]()

  var appInfo: Option[QualApplicationInfo] = None
  val sqlStart: HashMap[Long, QualSQLExecutionInfo] = HashMap[Long, QualSQLExecutionInfo]()

  // The duration of the SQL execution, in ms.
  val sqlDurationTime: HashMap[Long, Long] = HashMap.empty[Long, Long]

  val sqlIDToTaskEndSum: HashMap[Long, StageTaskQualificationSummary] =
    HashMap.empty[Long, StageTaskQualificationSummary]
  val stageIdToTaskEndSum: HashMap[Long, StageTaskQualificationSummary] =
    HashMap.empty[Long, StageTaskQualificationSummary]

  val stageIdToSqlID: HashMap[Int, Long] = HashMap.empty[Int, Long]
  val jobIdToSqlID: HashMap[Int, Long] = HashMap.empty[Int, Long]
  val sqlIDtoJobFailures: HashMap[Long, ArrayBuffer[Int]] = HashMap.empty[Long, ArrayBuffer[Int]]

  val notSupportFormatAndTypes: HashMap[String, Set[String]] = HashMap[String, Set[String]]()
  var sqlPlans: HashMap[Long, SparkPlanInfo] = HashMap.empty[Long, SparkPlanInfo]

  private lazy val eventProcessor =  new QualificationEventProcessor(this)

  /**
   * Get the event listener the qualification tool uses to process Spark events.
   * Install this listener in Spark.
   *
   * {{{
   *   spark.sparkContext.addSparkListener(listener)
   * }}}
   * @return SparkListener
   */
  def getEventListener: SparkListener = {
    eventProcessor
  }

  processEvents()

  override def processEvent(event: SparkListenerEvent): Boolean = {
    eventProcessor.processAnyEvent(event)
    false
  }

  // time in ms
  private def calculateAppDuration(startTime: Long): Option[Long] = {
    if (startTime > 0) {
      val estimatedResult =
        this.appEndTime match {
          case Some(t) => this.appEndTime
          case None =>
            if (lastSQLEndTime.isEmpty && lastJobEndTime.isEmpty) {
              None
            } else {
              logWarning(s"Application End Time is unknown for $appId, estimating based on" +
                " job and sql end times!")
              // estimate the app end with job or sql end times
              val sqlEndTime = if (this.lastSQLEndTime.isEmpty) 0L else this.lastSQLEndTime.get
              val jobEndTime = if (this.lastJobEndTime.isEmpty) 0L else lastJobEndTime.get
              val maxEndTime = math.max(sqlEndTime, jobEndTime)
              if (maxEndTime == 0) None else Some(maxEndTime)
            }
        }
      ProfileUtils.OptionLongMinusLong(estimatedResult, startTime)
    } else {
      None
    }
  }

  /**
   * The score starts out based on the over all task time spent in SQL dataframe
   * operations and then can only decrease from there based on if it has operations not
   * supported by the plugin.
   */
  private def calculateScore(readScoreRatio: Double, sqlDataframeTaskDuration: Long): Double = {
    // the readScorePercent is an integer representation of percent
    val ratioForReadScore = readScorePercent / 100.0
    val ratioForRestOfScore = 1.0 - ratioForReadScore
    // get the part of the duration that will apply to the read score
    val partForReadScore = sqlDataframeTaskDuration * ratioForReadScore
    // calculate the score for the read part based on the read format score
    val readScore = partForReadScore * readScoreRatio
    // get the rest of the duration that doesn't apply to the read score
    val scoreRestPart = sqlDataframeTaskDuration * ratioForRestOfScore
    scoreRestPart + readScore
  }

  // if the SQL contains a dataset, then duration for it is 0
  // for the SQL dataframe duration
  private def calculateSqlDataframeDuration: Long = {
    val validSums =
      sqlDurationTime.filterNot { case (sqlID, dur) =>
      sqlIDToDataSetOrRDDCase.contains(sqlID) || dur == -1
    }
    var sum = 0L;
    validSums.values.foreach { v =>
      longestSQLDuration = Math max (v, longestSQLDuration)
      sum += v;
    }
    sum
  }

  private def calculateNonSqlDataframeWallClockDuration: Long = {
    sqlDurationTime.filter { case (sqlID, dur) =>
      sqlIDToDataSetOrRDDCase.contains(sqlID) || dur == -1
    }.values.sum
  }

  // The total task time for all tasks that ran during SQL dataframe
  // operations.  if the SQL contains a dataset, it isn't counted.
  private def calculateTaskDataframeDuration: Long = {
    val validSums = sqlIDToTaskEndSum.filterNot { case (sqlID, _) =>
      sqlIDToDataSetOrRDDCase.contains(sqlID) || sqlDurationTime.getOrElse(sqlID, -1) == -1
    }
    validSums.values.map(dur => dur.totalTaskDuration).sum
  }

  // Assume that overhead is the all time windows that do not overlap with a running job.
  private def calculateJobOverHeadTime(startTime: Long): Long = {
    // Simple algorithm:
    // 1- sort all jobs by start/endtime.
    // 2- Initialize Time(p) = app.StartTime
    // 3- loop on the sorted seq. if the job.startTime is larger than the current Time(p):
    //    then this must be considered a gap
    // 4- Update Time(p) at the end of each iteration: Time(p+1) = Max(Time(p), job.endTime)
    val sortedJobs = jobIdToInfo.values.toSeq.sortBy(_.startTime)
    var pivot = startTime
    var overhead : Long = 0

    sortedJobs.foreach(job => {
      val timeDiff = job.startTime - pivot
      if (timeDiff > 0) {
        overhead += timeDiff
      }
      // if jobEndTime is not set, use job.startTime
      pivot = Math max(pivot, job.endTime.getOrElse(job.startTime))
    })
    logWarning(s"Calculated Overhead: ${overhead}")
    overhead
  }

  private def getSQLDurationProblematic: Long = {
    probNotDataset.keys.map { sqlId =>
      sqlDurationTime.getOrElse(sqlId, 0L)
    }.sum
  }

  // Look at the total task times for all jobs/stages that aren't SQL or
  // SQL but dataset or rdd
  private def calculateNonSQLTaskDataframeDuration(taskDFDuration: Long): Long = {
    val allTaskTime = stageIdToTaskEndSum.values.map(_.totalTaskDuration).sum

    // TODO make more efficient
    val res = allTaskTime - taskDFDuration
    assert(res >= 0)
    logWarning(s"non sql task duration is: $res task df duraton is $taskDFDuration all " +
      s"is $allTaskTime")
    res
  }

  private def calculateCpuTimePercent: Double = {
    val validSums = sqlIDToTaskEndSum.filterNot { case (sqlID, _) =>
      sqlIDToDataSetOrRDDCase.contains(sqlID) || sqlDurationTime.getOrElse(sqlID, -1) == -1
    }
    val totalCpuTime = validSums.values.map { dur =>
      dur.executorCPUTime
    }.sum
    val totalRunTime = validSums.values.map { dur =>
      dur.executorRunTime
    }.sum
    ToolUtils.calculateDurationPercent(totalCpuTime, totalRunTime)
  }

  private def calculateSQLUnsupportedDuration(all: Seq[Set[StageQualSummaryInfo]]): Long = {
    all.flatMap(_.map(_.unsupportedDur)).sum
  }

  // TODO calculate speedup_factor - which is average of operator factors???
  // For now it is a helper to generate random values for the POC. Returns rounded value
  private def calculateSpeedupFactor(all: Seq[Set[StageQualSummaryInfo]]): Int = {
    val allSpeedupFactors = all.flatMap(_.map(_.averageSpeedup))
    val res = SQLPlanParser.averageSpeedup(allSpeedupFactors)
    logWarning(s"average speedup factor is: $res from: ${allSpeedupFactors.mkString(",")}")
    res
  }

  private def getAllReadFileFormats: String = {
    dataSourceInfo.map { ds =>
      s"${ds.format.toLowerCase()}[${ds.schema}]"
    }.mkString(":")
  }

  // For the read score we look at all the read formats and datatypes for each
  // format and for each read give it a value 0.0 - 1.0 depending on whether
  // the format is supported and if the data types are supported. We then sum
  // those together and divide by the total number.  So if none of the data types
  // are supported, the score would be 0.0 and if all formats and datatypes are
  // supported the score would be 1.0.
  private def calculateReadScoreRatio(): Double = {
    if (dataSourceInfo.size == 0) {
      1.0
    } else {
      val readFormatSum = dataSourceInfo.map { ds =>
        val (readScore, nsTypes) = pluginTypeChecker.scoreReadDataTypes(ds.format, ds.schema)
        if (nsTypes.nonEmpty) {
          val currentFormat = notSupportFormatAndTypes.get(ds.format).getOrElse(Set.empty[String])
          notSupportFormatAndTypes(ds.format) = (currentFormat ++ nsTypes)
        }
        readScore
      }.sum
      readFormatSum / dataSourceInfo.size
    }
  }

  private def getStageToExec(execInfos: Seq[ExecInfo]): Map[Int, Seq[ExecInfo]] = {
    execInfos.flatMap { execInfo =>
      if (execInfo.stages.size > 1) {
        execInfo.stages.map((_, execInfo))
      } else if (execInfo.stages.size < 1) {
        // we don't know what stage its in our its duration
        logDebug(s"No stage associated with ${execInfo.exec} " +
          s"so speedup factor isn't applied anywhere.")
        Seq.empty
      } else {
        Seq((execInfo.stages.head, execInfo))
      }
    }.groupBy(_._1).map { case (k, v) =>
      (k, v.map(_._2))
    }
  }

  /**
   * Aggregate and process the application after reading the events.
   * @return Option of QualificationSummaryInfo, Some if we were able to process the application
   *         otherwise None.
   */
  def aggregateStats():
      Option[(QualificationSummaryInfo, Seq[PlanInfo], Seq[StageQualSummaryInfo])] = {
    appInfo.map { info =>
      val appDuration = calculateAppDuration(info.startTime).getOrElse(0L)
      val sqlDataframeDur = calculateSqlDataframeDuration
      // wall clock time
      val executorCpuTimePercent = calculateCpuTimePercent
      val endDurationEstimated = this.appEndTime.isEmpty && appDuration > 0
      val sqlDurProblem = getSQLDurationProblematic
      val readScoreRatio = calculateReadScoreRatio
      val sqlDataframeTaskDuration = calculateTaskDataframeDuration
      val noSQLDataframeTaskDuration =
        calculateNonSQLTaskDataframeDuration(sqlDataframeTaskDuration)
      val jobOverheadTime = calculateJobOverHeadTime(info.startTime)
      val nonSQLTaskDuration = noSQLDataframeTaskDuration + jobOverheadTime
      val readScoreHumanPercent = 100 * readScoreRatio
      val readScoreHumanPercentRounded = f"${readScoreHumanPercent}%1.2f".toDouble
      val score = calculateScore(readScoreRatio, sqlDataframeTaskDuration)
      val scoreRounded = f"${score}%1.2f".toDouble
      val failedIds = sqlIDtoJobFailures.filter { case (_, v) =>
        v.size > 0
      }.keys.mkString(",")
      val notSupportFormatAndTypesString = notSupportFormatAndTypes.map { case(format, types) =>
        val typeString = types.mkString(":").replace(",", ":")
        s"${format}[$typeString]"
      }.mkString(";")
      val writeFormat = writeFormatNotSupported(writeDataFormat)
      val (allComplexTypes, nestedComplexTypes) = reportComplexTypes
      val problems = getAllPotentialProblems(getPotentialProblemsForDf, nestedComplexTypes)

      val origPlanInfos = sqlPlans.map { case (id, plan) =>
        SQLPlanParser.parseSQLPlan(plan, id, pluginTypeChecker, this)
      }.toSeq
      // filter out any execs that should be removed
      val planInfos = origPlanInfos.map { p =>
        val execFilteredChildren = p.execInfo.map { e =>
          val filteredChildren = e.children.map { c =>
            c.filterNot(_.shouldRemove)
          }
          e.copy(children = filteredChildren)
        }
        val filteredPlanInfos = execFilteredChildren.filterNot(_.shouldRemove)
        p.copy(execInfo = filteredPlanInfos)
      }

      val perSqlSummary = planInfos.flatMap { pInfo =>
        val perSQLId = pInfo.execInfo.groupBy(_.sqlID)
        perSQLId.map { case (sqlID, execInfos) =>
          // there are issues with duration in whole stage code gen where duration of multiple
          // execs is more than entire stage time, for now ignore the exec duration and just
          // calculate based on average applied to total task time of each stage
          val allStagesToExecs = getStageToExec(execInfos)
          // if it doesn't have a stage id associated we can't calculate the time spent in that
          // SQL so we just drop it
          val stageSum = allStagesToExecs.keys.toSet.map { stageId =>
            val stageTaskTime = stageIdToTaskEndSum.get(stageId)
              .map(_.totalTaskDuration).getOrElse(0L)
            val execsForStage = allStagesToExecs.getOrElse(stageId, Seq.empty)
            val speedupFactors = execsForStage.map(_.speedupFactor)
            val averageSpeedupFactor = SQLPlanParser.averageSpeedup(speedupFactors)
            // need to remove the WholeStageCodegen wrappers since they aren't actual
            // execs that we want to get timings of
            val allFlattenedExecs = execsForStage.flatMap { e =>
              if (e.exec.contains("WholeStageCodegen")) {
                e.children.getOrElse(Seq.empty)
              } else {
                e.children.getOrElse(Seq.empty) :+ e
              }
            }
            val numUnsupported = allFlattenedExecs.filterNot(_.isSupported)
            logWarning(s"numUnsupported: ${numUnsupported.mkString(",")}")
            // if we have unsupported try to guess at how much time.  For now divide
            // time by number of execs and give each one equal weight
            val eachExecTime = stageTaskTime / allFlattenedExecs.size
            val unsupportedDur = eachExecTime * numUnsupported.size

            StageQualSummaryInfo(stageId, averageSpeedupFactor, stageTaskTime, unsupportedDur)
          }
          stageSum
        }
      }
      perSqlSummary.foreach { stageSum =>
        if (stageSum.nonEmpty) {
          logWarning(s"stages with average Speedup and stage " +
            s"Total Task Time, unsupportedDur: ${stageSum.mkString(",")}")
        }
      }

      val unsupportedSQLDuration = calculateSQLUnsupportedDuration(perSqlSummary)
      val speedupDuration = sqlDataframeTaskDuration - unsupportedSQLDuration
      val speedupFactor = calculateSpeedupFactor(perSqlSummary)
      val estimatedDuration =
        (speedupDuration / speedupFactor) + unsupportedSQLDuration + nonSQLTaskDuration
      val appTaskDuration = nonSQLTaskDuration + sqlDataframeTaskDuration
      val totalSpeedup = (appTaskDuration / estimatedDuration * 1000) / 1000
      val speedupBucket = if (totalSpeedup > 3) {
        "GREEN"
      } else if (totalSpeedup > 1.25) {
        "YELLOW"
      } else {
        "RED"
      }

      val summaryInfo = new QualificationSummaryInfo(info.appName, appId, scoreRounded, problems,
        sqlDataframeDur, sqlDataframeTaskDuration, appDuration, executorCpuTimePercent,
        endDurationEstimated, sqlDurProblem, failedIds, readScorePercent,
        readScoreHumanPercentRounded, notSupportFormatAndTypesString,
        getAllReadFileFormats, writeFormat, allComplexTypes, nestedComplexTypes, longestSQLDuration,
        calculateNonSqlDataframeWallClockDuration, nonSQLTaskDuration, estimatedDuration,
        unsupportedSQLDuration, speedupDuration, speedupFactor, totalSpeedup, speedupBucket)
      (summaryInfo, origPlanInfos, perSqlSummary.flatten)
    }
  }

  private[qualification] def processSQLPlan(sqlID: Long, planInfo: SparkPlanInfo): Unit = {
    checkMetadataForReadSchema(sqlID, planInfo)
    val planGraph = SparkPlanGraph(planInfo)
    val allnodes = planGraph.allNodes
    for (node <- allnodes) {
      checkGraphNodeForReads(sqlID, node)
      if (isDataSetOrRDDPlan(node.desc)) {
        sqlIDToDataSetOrRDDCase += sqlID
      }
      val issues = findPotentialIssues(node.desc)
      if (issues.nonEmpty) {
        val existingIssues = sqlIDtoProblematic.getOrElse(sqlID, Set.empty[String])
        sqlIDtoProblematic(sqlID) = existingIssues ++ issues
      }
      // Get the write data format
      if (node.name.contains("InsertIntoHadoopFsRelationCommand")) {
        val writeFormat = node.desc.split(",")(2)
        writeDataFormat += writeFormat
      }
    }
  }

  private def writeFormatNotSupported(writeFormat: ArrayBuffer[String]): String = {
    // Filter unsupported write data format
    val unSupportedWriteFormat = pluginTypeChecker.isWriteFormatsupported(writeFormat)

    unSupportedWriteFormat.distinct.mkString(";").toUpperCase
  }
}

class StageTaskQualificationSummary(
    val stageId: Int,
    val stageAttemptId: Int,
    var executorRunTime: Long,
    var executorCPUTime: Long,
    var totalTaskDuration: Long)

case class QualApplicationInfo(
    appName: String,
    appId: Option[String],
    startTime: Long,
    sparkUser: String,
    endTime: Option[Long], // time in ms
    duration: Option[Long],
    endDurationEstimated: Boolean)

case class QualSQLExecutionInfo(
    sqlID: Long,
    startTime: Long,
    endTime: Option[Long],
    duration: Option[Long],
    durationStr: String,
    sqlQualDuration: Option[Long],
    hasDataset: Boolean,
    problematic: String = "")

case class QualificationSummaryInfo(
    appName: String,
    appId: String,
    score: Double,
    potentialProblems: String,
    sqlDataFrameDuration: Long,
    sqlDataframeTaskDuration: Long,
    appDuration: Long,
    executorCpuTimePercent: Double,
    endDurationEstimated: Boolean,
    sqlDurationForProblematic: Long,
    failedSQLIds: String,
    readScorePercent: Int,
    readFileFormatScore: Double,
    readFileFormatAndTypesNotSupported: String,
    readFileFormats: String,
    writeDataFormat: String,
    complexTypes: String,
    nestedComplexTypes: String,
    longestSqlDuration: Long,
    nonSqlWallClockDuration: Long,
    nonSqlTaskDurationAndOverhead: Long,
    estimatedDuration: Long,
    unsupportedDuration: Long,
    speedupDuration: Long,
    speedupFactor: Double,
    totalSpeedup: Double,
    speedupBucket: String)

case class StageQualSummaryInfo(
    stageId: Int,
    averageSpeedup: Int,
    stageTaskTime: Long,
    unsupportedDur: Long)

object QualificationAppInfo extends Logging {
  def createApp(
      path: EventLogInfo,
      hadoopConf: Configuration,
      pluginTypeChecker: PluginTypeChecker,
      readScorePercent: Int): Option[QualificationAppInfo] = {
    val app = try {
        val app = new QualificationAppInfo(Some(path), Some(hadoopConf), pluginTypeChecker,
          readScorePercent)
        logInfo(s"${path.eventLog.toString} has App: ${app.appId}")
        Some(app)
      } catch {
        case gpuLog: GpuEventLogException =>
          logWarning(gpuLog.message)
          None
        case json: com.fasterxml.jackson.core.JsonParseException =>
          logWarning(s"Error parsing JSON: ${path.eventLog.toString}")
          None
        case il: IllegalArgumentException =>
          logWarning(s"Error parsing file: ${path.eventLog.toString}", il)
          None
        case e: Exception =>
          // catch all exceptions and skip that file
          logWarning(s"Got unexpected exception processing file: ${path.eventLog.toString}", e)
          None
      }
    app
  }
}
