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

/* globals $, Mustache, qualReportSummary */

const twoDecimalFormatter = new Intl.NumberFormat('en-US', {
  minimumFractionDigits: 2,
  maximumFractionDigits: 2,
});

function padZeroes(num) {
  return ("0" + num).slice(-2);
}

/* eslint-disable no-unused-vars */
function formatTimeMillis(timeMillis) {
  if (timeMillis <= 0) {
    return "-";
  } else {
    var dt = new Date(timeMillis);
    return formatDateString(dt);
  }
}

/* eslint-enable no-unused-vars */

function formatDateString(dt) {
  return dt.getFullYear() + "-" +
      padZeroes(dt.getMonth() + 1) + "-" +
      padZeroes(dt.getDate()) + " " +
      padZeroes(dt.getHours()) + ":" +
      padZeroes(dt.getMinutes()) + ":" +
      padZeroes(dt.getSeconds());
}

function formatDuration(milliseconds) {
  if (milliseconds < 100) {
    return parseInt(milliseconds).toFixed(1) + " ms";
  }
  var seconds = milliseconds * 1.0 / 1000;
  if (seconds < 1) {
    return seconds.toFixed(1) + " s";
  }
  if (seconds < 60) {
    return seconds.toFixed(0) + " s";
  }
  var minutes = seconds / 60;
  if (minutes < 10) {
    return minutes.toFixed(1) + " min";
  } else if (minutes < 60) {
    return minutes.toFixed(0) + " min";
  }
  var hours = minutes / 60;
  return hours.toFixed(1) + " h";
}

// don't filter on hidden html elements for an sType of title-numeric
function getColumnIndex(columns, columnName) {
  for (var i = 0; i < columns.length; i++) {
    if (columns[i].name == columnName)
      return i;
  }
  return -1;
}

// The maximum is inclusive and the minimum is inclusive
function getRandomIntInclusive(min, max) {
  min = Math.ceil(min);
  max = Math.floor(max);
  return Math.floor(Math.random() * (max - min + 1) + min);
}

/** calculations of CPU Processor **/

var CPUPercentThreshold = 40.0;

function totalCPUPercentageStyle(cpuPercent) {
  // Red if GC time over GCTimePercent of total time
  return (cpuPercent < CPUPercentThreshold) ?
      ("hsl(0, 100%, 50%, " + totalCPUPercentageAlpha(CPUPercentThreshold - cpuPercent) + ")") : "";
}

function totalCPUPercentageAlpha(actualCPUPercentage) {
  return actualCPUPercentage >= 0 ?
      (Math.min(actualCPUPercentage / 40.0 + 0.4, 1)) : 1;
}

function totalCPUPercentageColor(cpuPercent) {
  return (cpuPercent < CPUPercentThreshold) ? "white" : "black";
}

/** recommendation icons display */
function recommendationTableCellStyle(recommendation) {
  return "hsla("+ recommendation * 10.0 +",100%,50%)";
}

/* define recommendation grouping */
const recommendationRanges = {
  "A": {low: 7.5, high: 1000.0},
  "B": {low: 3.0, high: 7.7},
  "C": {low: 0.5, high: 3.0},
  "D": {low: -100.0, high: 0.5},
}

class GpuRecommendationCategory {
  constructor(id, relRate, printName, descr, initCollapsed = false) {
    this.id = id;
    this.displayName = printName;
    this.range = recommendationRanges[id];
    this.collapsed = initCollapsed;
    this.description = descr;
    this.rate = relRate;
  }

  // Getter
  get area() {
    return this.calcArea();
  }

  // Method
  isGroupOf(row) {
    return row.gpuRecommendation >= this.range.low
        && row.gpuRecommendation < this.range.high;
  }

  toggleCollapsed() {
    this.collapsed = !this.collapsed;
  }
}

let recommendationContainer = [
  new GpuRecommendationCategory("A", 5,
      "Strongly Recommended",
      "Spark Rapids is expected to speedup the App"),
  new GpuRecommendationCategory("B", 4,
      "Recommended",
      "Using Spark RAPIDS expected to give a moderate speedup."),
  new GpuRecommendationCategory("C", 3,
      "Discouraged",
      "[Not-Recommended]: It is not likely that GPU Acceleration will be tangible"),
  new GpuRecommendationCategory("D", 1,
      "Insufficient",
      "[Insufficient] Event-logs do not provide enough information to analyze."),
];

var recommendationsMap = recommendationContainer.reduce(function (map, obj) {
  map[obj.displayName] = obj;
  return map;
}, {});

let sparkUsers = new Map();

/* define constants for the tables configurations */
let defaultPageLength = 20;
let defaultLengthMenu = [[20, 40, 60, 100, -1], [20, 40, 60, 100, "All"]];
let appFieldAccCriterion = "sqlDataframeTaskDuration";//"sqlDFTaskDuration";

let simulateRecommendationEnabled = true;

function simulateGPURecommendations(appsArray, maxScore) {
  for (let i in appsArray) {
    appsArray[i]["gpuRecommendation"] = simulateRecommendationEnabled ?
        getRandomIntInclusive(1, 10)
        : ((appsArray[i][appFieldAccCriterion] * 10.00) / maxScore);
  }
}

// bind the raw data top the GPU recommendations
function setGPURecommendations(appsArray) {
  for (let i in appsArray) {
    let appCategory = recommendationContainer.find(grp => grp.isGroupOf(appsArray[i]))
    appsArray[i]["gpuCategory"] = appCategory.id;
  }
}

function processAppInfoRecords(appInfoRawRecords) {
  var map = new Map()
  appInfoRawRecords.forEach(object => {
    map.set(object.appId, object);
  });
  return map;
}

function setAppInfoRecord(appRecord, infoRecords) {
  //set default values
  appRecord["infoRec"] = {
    "sparkUser": "N/A",
    "startTimeFormated": "N/A"
  }
  if (infoRecords.has(appRecord.appId)) {
    appRecord["infoRec"] = infoRecords.get(appRecord.appId);
    appRecord["infoRec"]["startTimeFormatted"] =
        formatTimeMillis(appRecord["infoRec"]["startTime"])
  }
  sparkUsers.set(appRecord["infoRec"]["sparkUser"], true);
}

// which maps into wallclock time that shows how much of the SQL duration we think we can
// speed up on the GPU
function calculateAccelerationOpportunity(appRec) {
  let ratio = (appRec["speedupDuration"] * 1.0) / appRec["sqlDataframeTaskDuration"];
  return appRec["sqlDataFrameDuration"] * ratio;
}
function processRawData(rawRecords, appInfoRawRecords) {
  var processedRecords = [];
  var maxOpportunity = 0;
  var infoRecords = processAppInfoRecords(appInfoRawRecords);
  // let infoRecords = processAppInfoRecords(appInfoRawRecords) : Map
  for (var i in rawRecords) {
    var appRecord = JSON.parse(JSON.stringify(rawRecords[i]));
    appRecord["estimated"] = appRecord["appDurationEstimated"];
    appRecord["cpuPercent"] = appRecord["executorCPUPercent"];
    appRecord["durationCollection"] = {
      "appDuration": formatDuration(appRecord["appDuration"]),
      "sqlDFDuration": formatDuration(appRecord["sqlDataFrameDuration"]),
      "sqlDFTaskDuration": formatDuration(appRecord["sqlDataframeTaskDuration"]),
      "sqlDurationProblems": formatDuration(appRecord["sqlDurationForProblematic"]),
      "nonSqlTaskDurationAndOverhead": formatDuration(appRecord["nonSqlTaskDurationAndOverhead"]),
      "estimatedDuration": formatDuration(appRecord["estimatedDuration"]),
      "estimatedDurationWallClock":
        formatDuration((appRecord["appDuration"] * 1.0) / appRecord["totalSpeedup"]),
      "accelerationOpportunity": formatDuration(calculateAccelerationOpportunity(appRecord)),
      "unsupportedDuration": formatDuration(appRecord["unsupportedDuration"]),
      "speedupDuration": formatDuration(appRecord["speedupDuration"]),
    }
    setAppInfoRecord(appRecord, infoRecords);
    maxOpportunity =
        (maxOpportunity < appRecord[appFieldAccCriterion])
            ? appRecord[appFieldAccCriterion] : maxOpportunity;
    appRecord["attemptDetailsURL"] = "application.html?app_id=" + appRecord.appId;
    processedRecords.push(appRecord)
  }
  simulateGPURecommendations(processedRecords, maxOpportunity);
  setGPURecommendations(processedRecords);
  setGlobalReportSummary(processedRecords);
  return processedRecords;
}

function setGlobalReportSummary(processedApps) {
  let totalEstimatedApps = 0;
  let recommendedCnt = 0;
  let tlcCount = 0;
  let totalDurations = 0;
  let totalSqlDataframeTaskDuration = 0;
  for (let i in processedApps) {
    // check if completedTime is estimated
    if (processedApps[i]["estimated"]) {
      totalEstimatedApps += 1;
    }
    // check if the app is recommended or needs more information
    var recommendedGroup = recommendationContainer.find(grp => grp.isGroupOf(processedApps[i]));
    recommendedCnt = recommendedCnt + (recommendedGroup.id < "C" ? 1 : 0);
    if (recommendedGroup.id === "D") {
      tlcCount += 1;
    }
    totalDurations += processedApps[i].appDuration;
    totalSqlDataframeTaskDuration += processedApps[i].sqlDataframeTaskDuration;
  }
  let estimatedPercentage = 0.0;
  let gpuPercent = 0.0;
  let tlcPercent = 0.0;

  if (processedApps.length != 0) {
    // calculate percentage of estimatedEndTime;
    estimatedPercentage = (totalEstimatedApps * 100.0) / processedApps.length;
    // calculate percentage of recommended GPUs
    gpuPercent = (100.0 * recommendedCnt) / processedApps.length;
    // percent of apps missing information
    tlcPercent = (100.0 * tlcCount) / processedApps.length;
  }
  qualReportSummary.totalApps.numeric = processedApps.length;
  qualReportSummary.totalApps.totalAppsDurations = formatDuration(totalDurations);
  qualReportSummary.speedups.totalSqlDataframeTaskDuration = formatDuration(totalSqlDataframeTaskDuration);
  qualReportSummary.candidates.numeric = recommendedCnt;
  qualReportSummary.tlc.numeric = tlcCount;
  qualReportSummary.totalApps.statsPercentage =
      twoDecimalFormatter.format(estimatedPercentage)
      + qualReportSummary.totalApps.statsPercentage;
  qualReportSummary.candidates.statsPercentage =
      twoDecimalFormatter.format(gpuPercent)
      + qualReportSummary.candidates.statsPercentage;
  qualReportSummary.tlc.statsPercentage =
      twoDecimalFormatter.format(tlcPercent)
      + qualReportSummary.tlc.statsPercentage;
}
