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

/* global $, Mustache, formatDuration, jQuery, qualificationRecords, qualReportSummary */

function resetCollapsableGrps(groupArr, flag) {
  groupArr.forEach(grpElemnt => grpElemnt.collapsed = flag);
}

/*
 * HTML template used to render the application details in the collapsible
 * rows of the GPURecommendationTable.
 */
var recommendTblAppDetailsTemplate =
    '<table class=\"table table-striped compact dataTable style=padding-left:50px;\">' +
    '  <thead>' +
    '    <tr>' +
    '      <th scope=\"col\">#</th>' +
    '      <th scope=\"col\">Value</th>' +
    '      <th scope=\"col\">Description</th>' +
    '    </tr>' +
    '  </thead>' +
    '  <tbody>' +
    '    <tr>' +
    '      <th scope=\"row\">Total Speed-up</th>' +
    '      <td> {{totalSpeedup}} </td>' +
    '      <td> Speedup factor estimated for the app. Calculated as (app_duration / gpu_estimated_duration) % </td>' +
    '    </tr>' +
    '    <tr>' +
    '      <th scope=\"row\">App Duration</th>' +
    '      <td> {{durationCollection.appDuration}} </td>' +
    '      <td> Wall-Clock time measured since the application starts till it is completed. If an app is not completed an estimated completion time would be computed. </td>' +
    '    </tr>' +
    '    <tr>' +
    '      <th scope=\"row\">Sql Duration</th>' +
    '      <td> {{durationCollection.sqlDFDuration}} </td>' +
    '      <td> Wall-Clock time spent in tasks of SQL Dataframe operations. </td>' +
    '    </tr>' +
    '    <tr>' +
    '      <th scope=\"row\">Acceleration Opportunity</th>' +
    '      <td> {{durationCollection.accelerationOpportunity}} </td>' +
    '      <td> Wall-Clock time that shows how much of the SQL duration can be speed-up on the GPU. </td>' +
    '    </tr>' +
    '    <tr>' +
    '      <th scope=\"row\">GPU Estimated Duration</th>' +
    '      <td> {{durationCollection.estimatedDurationWallClock}} </td>' +
    '      <td> Predicted runtime of the app if it was run on GPU </td>' +
    '    </tr>' +
    '  </tbody>' +
    '</table>' +
    '<div class=\" mt-3\">' +
    '  <a href=\"{{attemptDetailsURL}}\" target=\"_blank\" class=\"btn btn-secondary btn-lg btn-block mb-1\">Go To Full Details</button>' +
    '</div>';

function formatAppGPURecommendation ( rowData) {
  var text = Mustache.render(recommendTblAppDetailsTemplate, rowData);
  return text;
}

let definedDataTables = {};
let gpuRecommendationTableID = "datatables.gpuRecommendations";

function expandAllGpuRowEntries() {
  expandAllGpuRows(definedDataTables[gpuRecommendationTableID]);
}

function collapseAllGpuRowEntries() {
  collapseAllGpuRows(definedDataTables[gpuRecommendationTableID]);
}

function expandAllGpuRows(gpuTable) {
  resetCollapsableGrps(recommendationContainer, false);

  // Enumerate all rows
  gpuTable.rows().every(function(){
    // If row has details collapsed
    if (!this.child.isShown()){
      // Open this row
      this.child(formatAppGPURecommendation(this.data())).show();
      $(this.node()).addClass('shown');
    }
  });
  gpuTable.draw(false);

}

function collapseAllGpuRows(gpuTable) {
  resetCollapsableGrps(recommendationContainer, true);
  // Enumerate all rows
  gpuTable.rows().every(function(){
    // If row has details expanded
    if(this.child.isShown()){
      // Collapse row details
      this.child.hide();
      $(this.node()).removeClass('shown');
    }
  });
  gpuTable.draw(false);
}

function initRating(container){
  $('span.rating', container).raty({
    half: true,
    starHalf: 'https://cdnjs.cloudflare.com/ajax/libs/raty/2.7.0/images/star-half.png',
    starOff: 'https://cdnjs.cloudflare.com/ajax/libs/raty/2.7.0/images/star-off.png',
    starOn: 'https://cdnjs.cloudflare.com/ajax/libs/raty/2.7.0/images/star-on.png',
    readOnly: true,
    score: function() {
      return $(this).attr('data-score');
    }
  });
}



$(document).ready(function(){
  // do required filtering here
  let attemptArray = processRawData(qualificationRecords, appInfoRecords);
  let initGpuRecommendationConf = UIConfig[gpuRecommendationTableID];
  // Start implementation of GPU Recommendations Apps

  var recommendGPUColName = "gpuRecommendationX"
  var opportunityColName = "opportunity"
  var sortColumnForGPURecommend = opportunityColName
  var gpuRecommendationConf = {
    responsive: true,
    info: true,
    paging: (attemptArray.length > defaultPageLength),
    pageLength: defaultPageLength,
    lengthMenu: defaultLengthMenu,
    stripeClasses: [],
    "data": attemptArray,
    "columns": [
      {
        "className":      'dt-control',
        "orderable":      false,
        "data":           null,
        "defaultContent": ''
      },
      {data: "appName"},
      {
        data: "appId",
        render:  (appId, type, row) => {
          if (type === 'display' || type === 'filter') {
            return `<a href="${row.attemptDetailsURL}" target="_blank">${appId}</a>`
          }
          return appId;
        }
      },
      {
        name: 'appDuration',
        data: 'appDuration',
        type: 'numeric',
        searchable: false,
        render: function (data, type, row) {
          if (type === 'display' || type === 'filter') {
            return formatDuration(data)
          }
          return data;
        },
        fnCreatedCell: (nTd, sData, oData, _ignored_iRow, _ignored_iCol) => {
          if (oData.estimated) {
            $(nTd).css('color', 'blue');
          }
        }
      },
      {
        name: opportunityColName,
        data: 'gpuRecommendation',
        render: function (data, type, row) {
          let calcValue = data * 10.0;
          if (type === 'display') {
            return '<progress title="' + calcValue + '% " value="' + calcValue + '" max="100.0"></progress>';
            //return twoDecimalFormatter.format(data * 10.0) + "%"
          }
          return data;
        },
      },
      {
        name: recommendGPUColName,
        data: 'gpuRecommendation',
        render: function (data, type, row) {
          var recommendedGroup = recommendationContainer.find(grp => grp.isGroupOf(row));
          return recommendedGroup.displayName;
        },
        fnCreatedCell: (nTd, sData, oData, _ignored_iRow, _ignored_iCol) => {
          $(nTd).css('background', recommendationTableCellStyle(sData));
        },
      }
    ],
    //dom with search panes
    //dom: 'Bfrtlip',
    //dom: '<"dtsp-dataTable"Bfrtip>',
    dom: 'Bfrtlip',
    initComplete: function(settings, json) {
      // Add custom Tool Tip to the headers of the table
      $('#gpu-recommendation-table thead th').each(function () {
        var $td = $(this);
        var toolTipVal = toolTipsValues.gpuRecommendations[$td.text().trim()];
        $td.attr('data-toggle', "tooltip");
        $td.attr('data-placement', "top");
        $td.attr('html', "true");
        $td.attr('data-html', "true");
        $td.attr('title', toolTipVal);
      });
    }
  };

  gpuRecommendationConf.order =
      [[getColumnIndex(gpuRecommendationConf.columns, sortColumnForGPURecommend), "desc"]];
  if (initGpuRecommendationConf["rowgroup.enabled"] == true) {
    gpuRecommendationConf.rowGroup = {
      startRender: function (rows, group) {
        // var collapsed = !!(collapsedGroups[group]);
        let collapsedBool = recommendationsMap[group].collapsed;
        rows.nodes().each(function (r) {
          r.style.display = '';
          if (collapsedBool) {
            r.style.display = 'none';
          }
        });
        // Iterate group rows and close open child rows.
        if (collapsedBool) {
          rows.every(function (rowIdx, tableLoop, rowLoop) {
            if (this.child.isShown()) {
              var tr = $(this.node());
              this.child.hide();

              tr.removeClass('shown');
            }
          });
        }
        var arrow = collapsedBool ?
            '<span class="collapse-table-arrow arrow-closed"></span> '
            : ' <span class="collapse-table-arrow arrow-open"></span> ';

        let toolTip = 'data-toggle=\"tooltip\" data-html=\"true\" data-placement=\"top\" '
            + 'title=\"' + recommendationsMap[group].description + '\"';
        var addToolTip = true;
        return $('<tr/>')
            .append('<td colspan=\"' + rows.columns()[0].length + '\"'
                + (addToolTip ? toolTip : '') + '>'
                + arrow + '&nbsp;'
                + group
                + ' (' + rows.count() + ')'
                + '</td>')
            .attr('data-name', group)
            .toggleClass('collapsed', collapsedBool);
      },
      dataSrc: function (row) {
        var recommendedGroup = recommendationContainer.find(grp => grp.isGroupOf(row))
        return recommendedGroup.displayName;
      }
    }
  } // rowGrouping by recommendations


  // set the dom of the tableConf
  gpuRecommendationConf.dom = initGpuRecommendationConf["Dom"].default;

  if (initGpuRecommendationConf.hasOwnProperty('searchPanes')) {
    let searchPanesConf = initGpuRecommendationConf['searchPanes']
    if (searchPanesConf["enabled"]) {
      // disable searchpanes on default columns
      gpuRecommendationConf.columnDefs = [{
        "searchPanes": {
          show: false,
        },
        "targets": ['_all']
      }];
      // add custom panes
      gpuRecommendationConf.searchPanes = searchPanesConf["dtConfigurations"];

      // add the searchpanes to the dom
      gpuRecommendationConf.dom = 'P' + gpuRecommendationConf.dom;
      // add custom panes to display recommendations
      let panesConfigurations = searchPanesConf["panes"];
      // first define values of the first recommendation Pane
      let gpuCatgeoryOptions = function() {
        let categoryOptions = [];
        for (let i in recommendationContainer) {
          if (i == recommendationContainer.length - 1) {
            // skip last index (insufficient)
            continue;
          }
          let currOption = {
            label: recommendationContainer[i].displayName,
            value: function(rowData, rowIdx) {
              return (rowData["gpuCategory"] === recommendationContainer[i].id);
            }
          }
          categoryOptions.push(currOption);
        }
        return categoryOptions;
      };
      // define the display options of the recommendation pane
      let gpuRecommendationPane = function() {
        let gpuPaneConfig = panesConfigurations["recommendation"];
        let recommendationPaneConf = {};
        recommendationPaneConf.header = gpuPaneConfig["header"];
        recommendationPaneConf.options = gpuCatgeoryOptions();
        recommendationPaneConf.dtOpts = {
          "searching": gpuPaneConfig["search"],
          "order": gpuPaneConfig["order"],
        }
        recommendationPaneConf.combiner = 'and';
        return recommendationPaneConf;
      }
      // define searchPanes for users
      let sparkUsersOptions = function() {
        let sparkUserOptions = [];
        sparkUsers.forEach((data, userName) => {
          let currOption = {
            label: userName,
            value: function(rowData, rowIdx) {
              return (rowData["infoRec"]["sparkUser"] === userName);
            },
          }
          sparkUserOptions.push(currOption);
        });
        return sparkUserOptions;
      };
      // define the display options of the user filter pane
      let userPane = function() {
        let userPaneConfig = panesConfigurations["users"];
        let recommendationPaneConf = {};
        recommendationPaneConf.header = userPaneConfig["header"];
        recommendationPaneConf.options = sparkUsersOptions();
        recommendationPaneConf.dtOpts = {
          "searching": userPaneConfig["search"],
          "order": userPaneConfig["order"],
        }
        recommendationPaneConf.combiner = 'and';
        return recommendationPaneConf;
      }
      gpuRecommendationConf.searchPanes.panes = [
        gpuRecommendationPane(), userPane()
      ];
    }
  }

  // add buttons if enabled
  if (initGpuRecommendationConf.hasOwnProperty('buttons')) {
    let buttonsConf = initGpuRecommendationConf['buttons'];
    if (buttonsConf["enabled"]) {
      // below if to avoid the buttons messed up with length page
      // dom: "<'row'<'col-sm-12 col-md-10'><'col-sm-12 col-md-2'B>>" +
      //      "<'row'<'col-sm-12 col-md-6'l><'col-sm-12 col-md-6'f>>" +
      //      "<'row'<'col-sm-12'tr>>" +
      //      "<'row'<'col-sm-12 col-md-5'i><'col-sm-12 col-md-7'p>>",
      gpuRecommendationConf["buttons"] = buttonsConf.buttons
      gpuRecommendationConf.dom = 'B' + gpuRecommendationConf.dom
    }
  }

  var gpuRecommendationTable = $('#gpu-recommendation-table').DataTable(gpuRecommendationConf);

  definedDataTables[gpuRecommendationTableID] = gpuRecommendationTable;

  //TODO: we need to expand the rowGroups on search events
  //There is a possible solution https://stackoverflow.com/questions/57692989/datatables-trigger-rowgroup-click-with-search-filter

  $('#gpu-recommendation-table tbody').on('click', 'tr.dtrg-start', function () {
    var name = $(this).data('name');
    // we may need to hide tooltip hangs
    // $('#gpu-recommendation-table [data-toggle="tooltip"]').tooltip('hide');
    recommendationsMap[name].toggleCollapsed();
    gpuRecommendationTable.draw(false);
  });


  // Add event listener for opening and closing details
  $('#gpu-recommendation-table tbody').on('click', 'td.dt-control', function () {
    var tr = $(this).closest('tr');
    var row = gpuRecommendationTable.row( tr );

    if ( row.child.isShown() ) {
      // This row is already open - close it
      row.child.hide();
      tr.removeClass('shown');
    }
    else {
      // Open this row
      row.child( formatAppGPURecommendation(row.data()) ).show();
      tr.addClass('shown');
    }
  });

  // Handle click on "Expand All" button
  $('#btn-show-all-children').on('click', function() {
    expandAllGpuRows(gpuRecommendationTable);
  });

  // Handle click on "Collapse All" button
  $('#btn-hide-all-children').on('click', function(){
    collapseAllGpuRows(gpuRecommendationTable);
  });

  // set the template of the report qualReportSummary
  var template = $("#qual-report-summary-template").html();
  var text = Mustache.render(template, qualReportSummary);
  $("#qual-report-summary").html(text);

  // set the template of the Qualification runtimeInformation
  if (false) {
    //TODO: fill the template of the execution: last executed, how long it took..etc
    var template = $("#qual-report-runtime-information-template").html();
    var text = Mustache.render(template, qualReportSummary);
    $("#qual-report-runtime-information").html(text);
  }

});
