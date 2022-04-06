/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

/* global $, Mustache, formatDuration, formatTimeMillis, jQuery, uiRoot */

function getColumnIndex(columns, columnName) {
    for(var i = 0; i < columns.length; i++) {
        if (columns[i].name == columnName)
            return i;
    }
    return -1;
}

/* An array of DataSourceInfo */
var datasourceReportData = [];

function setDataSourceInfoArr(dsInfoArray) {
    console.log(dsInfoArray)
    datasourceReportData = dsInfoArray
}

$(document).ready(function() {
    $.blockUI({ message: '<h3>Loading RAPIDS profiling summary...</h3>'});
    var datasourceReport = $("#datasource-report");
    setDataTableDefaults();
    var data = {
        "uiroot": uiRoot,
        "applications": datasourceReportData,
    };
    $.get(uiRoot + "/static/rapids/datasource-report-template.html", function(template) {
        var sibling = datasourceReport.prev();
        datasourceReport.detach();
        var apps = $(Mustache.render($(template).filter("#datasource-report-template").html(),data));
        var conf = {
            "data": datasourceReportData,
            "columns": [
                {name: 'appIndex', data: 'appIndex'},
                {name: 'sqlID', data: 'sqlID'},
                {name: 'format', data: 'format'},
                {name: 'location', data: 'location'},
                {name: 'pushedFilters', data: 'pushedFilters'},
                {name: 'schema', data: 'schema'},
            ],
            "deferRender": true,
            "autoWidth": false
        };
        var defaultSortColumn = "sqlID";
        conf.order = [[ getColumnIndex(conf.columns, defaultSortColumn), "desc" ]];
        datasourceReport.append(apps);
        apps.DataTable(conf);
        sibling.after(datasourceReport);
        $('#datasource-report [data-toggle="tooltip"]').tooltip();
        $.unblockUI();
    });
});