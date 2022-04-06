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


/* An array of JobInfo */
var jobInfoReportData = [];

function setJobInfoArr(dsInfoArray) {
  console.log(dsInfoArray)
  jobInfoReportData = dsInfoArray
}

function generateJobInfoReport() {
  var datasourceReport = $("#jobinfo-report");
  var showAppIndexColumn = false;
  var hasMultipleStages = false;
  var jobArray = [];
  for (var i in jobInfoReportData) {
    var job = jobInfoReportData[i];
    if (job["stageIds"].length > 1) {
      hasMultipleStages = true
      for (var j in job["stageIds"]) {
        let jStage = JSON.parse(JSON.stringify(job));
        jStage["stageId"] = j
        jobArray.push(jStage);
      }
    } else {
      job["stageId"] = job["stageIds"][0]
      jobArray.push(job);
    }
  }
  // if (jobArray.length < 20) {
  //   $.fn.dataTable.defaults.paging = false;
  // }

  var data = {
    "uiroot": uiRoot,
    "applications": jobArray,
    "hasMultipleStages": hasMultipleStages,
    "showAppIndexColumn": showAppIndexColumn
  };
  $.get(uiRoot + "/static/rapids/jobinfo-report-template.html", function (template) {
    var sibling = datasourceReport.prev();
    datasourceReport.detach();
    var stageIdsColumnName = 'stages';
    var apps = $(Mustache.render($(template).filter("#jobinfo-report-template").html(), data));
    var conf = {
      "data": jobArray,
      "columns": [
        {name: 'appIndex', data: 'appIndex'},
        {name: 'jobID', data: 'jobID'},
        {name: 'stageID', data: 'stageId'},
        {
          name: 'sql',
          data: 'sqlID',
          defaultContent: ''
        },
        {
          name: 'started',
          data: 'startTime',
          defaultContent: ''
        },
        {
          name: 'completed',
          data: 'endTime',
          defaultContent: ''
        },
      ],
      "aoColumnDefs": [
        {
          aTargets: showAppIndexColumn ? [0, 1] : [0],
          fnCreatedCell: (nTd, _ignored_sData, _ignored_oData, _ignored_iRow, _ignored_iCol) => {
            if (hasMultipleStages) {
              $(nTd).css('background-color', '#fff');
            }
          }
        },
      ],
      "deferRender": true,
      "autoWidth": false,
      "paging": jobArray.length > 20
    };
    if (hasMultipleStages && !showAppIndexColumn) {
      conf.rowsGroup = [
        'appIndex:name',
        'jobID:name'
      ];
    }
    var defaultSortColumn = "jobID";
    if (!showAppIndexColumn) {
      conf.columns = removeColumnByName(conf.columns, "appIndex");
    }
    conf.order = [[getColumnIndex(conf.columns, defaultSortColumn), "desc"]];
    datasourceReport.append(apps);
    apps.DataTable(conf);
    sibling.after(datasourceReport);
    $('#jobinfo-report [data-toggle="tooltip"]').tooltip();
  });
}

$(document).ready(generateJobInfoReport);